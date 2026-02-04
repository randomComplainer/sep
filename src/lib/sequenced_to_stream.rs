use derive_more::From;
use futures::prelude::*;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tracing::Instrument as _;

use crate::sequence::{StreamEntry, StreamEntryValue};

use crate::protocol::msg::session as msg;

#[derive(Debug, From)]
pub enum Command {
    Data(#[from] msg::Data),
    Eof(#[from] msg::Eof),
}

#[derive(Debug, From)]
pub enum Event {
    Ack(#[from] msg::Ack),
    EofAck(#[from] msg::EofAck),
}

pub struct Config {
    pub max_bytes_ahead: u32,
    pub max_packet_size: u16,
}

struct State<Stream, EvtTx> {
    config: Config,
    next_seq: u16,
    buffed_bytes: u32,
    buffed_entries: std::collections::BinaryHeap<std::cmp::Reverse<StreamEntry>>,
    stream_to_write: Stream,
    evt_tx: EvtTx,
}

macro_rules! try_emit_evt {
    ($evt_tx:expr, $evt:ident) => {
        tracing::debug!(?$evt, "event");
        if let Err(_) = $evt_tx.send($evt).await {
            tracing::warn!("event channel is broken, exiting");
            return Ok(None);
        }
    };
}

impl<Stream, EvtTx, EvtTxErr> State<Stream, EvtTx>
where
    Stream: AsyncWrite + Unpin + Send + 'static,
    EvtTxErr: std::fmt::Debug,
    EvtTx: Sink<Event, Error = EvtTxErr> + Unpin + Send + 'static,
{
    pub fn new(config: Config, stream_to_write: Stream, evt_tx: EvtTx) -> Self {
        let estimated_buffered_packets =
            (config.max_bytes_ahead as usize / config.max_packet_size as usize) + 1;

        Self {
            config,
            next_seq: 0,
            buffed_bytes: 0,
            buffed_entries: std::collections::BinaryHeap::with_capacity(estimated_buffered_packets),
            stream_to_write,
            evt_tx,
        }
    }

    pub async fn handle_command(mut self, cmd: Command) -> Result<Option<Self>, std::io::Error> {
        match cmd {
            Command::Data(data) => {
                let len = data.data.len() as u32;

                self.buffed_entries
                    .push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));

                self.buffed_bytes += len;
                tracing::trace!(bytes = self.buffed_bytes, "buffered");

                if self.buffed_bytes > self.config.max_bytes_ahead {
                    panic!("too many data buffered");
                }
            }
            Command::Eof(eof) => {
                self.buffed_entries
                    .push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
            }
        };

        let mut bytes_written = 0u32;
        while self
            .buffed_entries
            .peek()
            .map(|e| e.0.0 == self.next_seq)
            .unwrap_or(false)
        {
            let stream_entry = self.buffed_entries.pop().unwrap();
            let seq = stream_entry.0.0;

            match stream_entry.0.1 {
                StreamEntryValue::Data(data) => {
                    let len = data.len();

                    self.stream_to_write
                        .write_all(data.as_ref())
                        .instrument(tracing::trace_span!("write to stream", seq, ?len))
                        .await
                        .inspect_err(|err| tracing::error!(?err, "stream write error"))?;

                    self.buffed_bytes -= len as u32;
                    bytes_written += len as u32;
                }
                StreamEntryValue::Eof => {
                    assert_eq!(0, self.buffed_bytes);
                    assert_eq!(0, self.buffed_entries.len());

                    if bytes_written > 0 {
                        let evt = msg::Ack {
                            bytes: bytes_written as u32,
                        }
                        .into();

                        try_emit_evt!(self.evt_tx, evt);
                    }

                    let evt = msg::EofAck.into();
                    try_emit_evt!(self.evt_tx, evt);

                    return Ok(None);
                }
            };
            self.next_seq += 1;
        }

        if bytes_written > 0 {
            let evt = msg::Ack {
                bytes: bytes_written as u32,
            }
            .into();
            try_emit_evt!(self.evt_tx, evt);
        }

        Ok(Some(self))
    }
}

pub async fn run(
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Unpin + Send + 'static,
    stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    config: Config,
) -> Result<(), std::io::Error> {
    let mut state = State::new(config, stream_to_write, evt_tx);

    while let Some(cmd) = cmd_rx
        .next()
        .instrument(tracing::trace_span!("recv cmd"))
        .await
    {
        tracing::debug!(cmd = ?cmd, "cmd");
        state = match state.handle_command(cmd).await? {
            Some(new_state) => new_state,
            None => break,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // use std::assert_matches::assert_matches;
    //
    // use bytes::BytesMut;
    // use tokio::io::AsyncReadExt;
    //
    // use super::*;
    //
    // fn create_task(
    //     max_packet_ahead: u16,
    //     stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    // ) -> (
    //     impl futures::Sink<Command, Error = impl std::fmt::Debug> + Unpin,
    //     impl futures::Stream<Item = Event> + Send + Unpin,
    //     impl std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
    // ) {
    //     let (cmd_tx, cmd_rx) = futures::channel::mpsc::channel(1);
    //     let (event_tx, event_rx) = futures::channel::mpsc::channel(1);
    //
    //     let config = Config { max_packet_ahead };
    //
    //     let task = run(cmd_rx, event_tx, stream_to_write, config);
    //     (cmd_tx, event_rx, task)
    // }
    //
    // #[test_log::test]
    // fn happy_path() {
    //     let (mut cmd_tx, mut event_rx, task) =
    //         create_task(1, tokio_test::io::Builder::new().write(&[1, 2, 3]).build());
    //
    //     let mut task = tokio_test::task::spawn(task);
    //
    //     for i in 0..=2 {
    //         tokio_test::assert_pending!(task.poll());
    //
    //         tokio_test::assert_ready!(
    //             tokio_test::task::spawn(cmd_tx.send(Command::Data(msg::Data {
    //                 seq: i.try_into().unwrap(),
    //                 data: BytesMut::from([(i + 1).try_into().unwrap()].as_ref()),
    //             })))
    //             .poll()
    //         )
    //         .unwrap();
    //
    //         tokio_test::assert_pending!(task.poll());
    //         tokio_test::assert_pending!(task.poll());
    //         tokio_test::assert_pending!(task.poll());
    //
    //         let evt =
    //             tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
    //
    //         match evt {
    //             super::Event::Ack(msg::Ack { bytes }) => {
    //                 assert_eq!(bytes, 1);
    //             }
    //         };
    //     }
    //
    //     tokio_test::assert_ready!(
    //         tokio_test::task::spawn(cmd_tx.send(Command::Eof(msg::Eof { seq: 3 }))).poll()
    //     )
    //     .unwrap();
    //
    //     tokio_test::block_on(task).unwrap();
    //
    //     let evt =
    //         tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
    //
    //     match evt {
    //         super::Event::Ack(msg::Ack { bytes }) => {
    //             assert_eq!(bytes, 1);
    //         }
    //     };
    // }
    //
    // #[tokio::test]
    // async fn quit_on_broken_cmd_stream() {
    //     let (cmd_tx, mut _event_rx, main_task) = create_task(3, tokio::io::duplex(0).0);
    //     drop(cmd_tx);
    //
    //     let result = main_task.await;
    //     assert_matches!(result, Ok(()));
    // }
    //
    // // it's slightly concerning that
    // // event channel's broken state is not checked
    // // until next packet is written to the stream
    // #[test_log::test(tokio::test)]
    // async fn quit_on_broken_evt_stream() {
    //     let (mut cmd_tx, evt_rx, main_task) =
    //         create_task(3, tokio_test::io::Builder::new().write(&[1]).build());
    //     drop(evt_rx);
    //
    //     let main_task = tokio::spawn(main_task);
    //
    //     cmd_tx
    //         .send(Command::Data(msg::Data {
    //             seq: 0.try_into().unwrap(),
    //             data: BytesMut::from([1].as_ref()),
    //         }))
    //         .await
    //         .unwrap();
    //
    //     let result = main_task.await.unwrap();
    //     assert_matches!(result, Ok(()));
    // }
    //
    // #[tokio::test]
    // async fn io_error() {
    //     let (mut cmd_tx, mut _event_rx, main_task) = create_task(
    //         1,
    //         tokio_test::io::Builder::new()
    //             .write_error(std::io::Error::new(
    //                 std::io::ErrorKind::BrokenPipe,
    //                 "broken pipe",
    //             ))
    //             .build(),
    //     );
    //
    //     cmd_tx
    //         .send(Command::Data(msg::Data {
    //             seq: 0.try_into().unwrap(),
    //             data: BytesMut::from([1].as_ref()),
    //         }))
    //         .await
    //         .unwrap();
    //
    //     let result = main_task.await;
    //
    //     assert_matches!(result, Err(std::io::Error { .. }));
    //     let e = result.unwrap_err();
    //     assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
    // }
    //
    // #[tokio::test]
    // async fn panic_on_exceed_max_packet_ahead() {
    //     let (mut cmd_tx, mut _event_rx, main_task) = create_task(3, tokio::io::duplex(0).0);
    //     let send_cmds = async move {
    //         for i in 1..=4 {
    //             cmd_tx
    //                 .send(Command::Data(msg::Data {
    //                     seq: i.try_into().unwrap(),
    //                     data: BytesMut::from([i.try_into().unwrap()].as_ref()),
    //                 }))
    //                 .await
    //                 .unwrap();
    //         }
    //     };
    //
    //     let send_cmds = tokio::spawn(send_cmds);
    //     let main_task = tokio::spawn(main_task);
    //
    //     let (send_cmds_result, main_task_result) = tokio::join!(send_cmds, main_task);
    //
    //     send_cmds_result.unwrap();
    //
    //     assert!(main_task_result.is_err());
    //     assert!(main_task_result.unwrap_err().is_panic());
    // }
    //
    // #[test]
    // fn dont_block_on_stream() {
    //     let (stream_write, mut stream_read) = tokio::io::duplex(1);
    //
    //     let (mut cmd_tx, mut event_rx, main_task) = create_task(10, stream_write);
    //
    //     let mut main_task = tokio_test::task::spawn(main_task);
    //
    //     for i in 0..10 {
    //         let mut cmd_sending = tokio_test::task::spawn(cmd_tx.send(Command::Data(msg::Data {
    //             seq: i.try_into().unwrap(),
    //             data: BytesMut::from([i.try_into().unwrap()].as_ref()),
    //         })));
    //         tokio_test::assert_ready!(cmd_sending.poll()).unwrap();
    //         tokio_test::assert_pending!(main_task.poll());
    //     }
    //
    //     let mut buf = BytesMut::with_capacity(9);
    //
    //     for _i in 0..10 {
    //         // forward event
    //         tokio_test::assert_pending!(main_task.poll());
    //         let e =
    //             tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
    //
    //         match e {
    //             super::Event::Ack(msg::Ack { bytes }) => {
    //                 assert_eq!(bytes, 1);
    //             }
    //         };
    //
    //         let n = tokio_test::assert_ready!(
    //             tokio_test::task::spawn(stream_read.read_buf(&mut buf)).poll()
    //         )
    //         .unwrap();
    //         assert_eq!(n, 1);
    //
    //         tokio_test::assert_pending!(main_task.poll());
    //     }
    //
    //     assert_eq!(buf.as_ref(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].as_ref());
    // }
}

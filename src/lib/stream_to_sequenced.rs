use bytes::{BufMut, BytesMut};
use derive_more::From;
use futures::prelude::*;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    sync::watch,
};
use tracing::Instrument as _;

use crate::ok_or;
use crate::protocol::msg::session as msg;

#[derive(Debug, From)]
pub enum Command {
    Ack(#[from] msg::Ack),
    EofAck(#[from] msg::EofAck),
}

#[derive(Debug, From)]
pub enum Event {
    Data(#[from] msg::Data),
    Eof(#[from] msg::Eof),
}

pub struct Config {
    pub max_bytes_ahead: u64,
}

#[derive(Clone, Copy)]
struct ExternalState {
    acked: u64,
    eof_acked: bool,
}

impl ExternalState {
    pub fn has_capacity_for(&self, total_read: u64, config: &Config) -> bool {
        total_read - self.acked <= config.max_bytes_ahead
    }

    pub fn all_acked(&self, total_sent: u64) -> bool {
        self.acked >= total_sent
    }
}

#[derive(Default)]
struct InternalState {
    total_read: u64,
    next_seq: u16,
}

impl InternalState {
    pub fn assign_seq(&mut self, packet_size: u64) -> u16 {
        self.total_read += packet_size;
        let result = self.next_seq;
        self.next_seq += 1;
        result
    }
}

// read that never exceeds BytesMut's capacity
async fn read_buf<ReadStream: AsyncRead + Unpin>(
    src: &mut ReadStream,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let spare = buf.spare_capacity_mut();

    assert!(!spare.is_empty());

    let mut slice =
        unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len()) };

    let n = src.read_buf(&mut slice).await?;

    unsafe {
        buf.advance_mut(n);
    }

    Ok(n)
}

async fn stream_reading_loop(
    mut stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    buf_pool: crate::buffer_pool::BufferPool,
    mut evt_tx: impl Sink<Event> + Unpin,
    mut internal_state: InternalState,
    mut external_state: watch::Receiver<ExternalState>,
    config: Config,
) -> Result<(), std::io::Error> {
    let mut readyness_checker = [];
    loop {
        stream_to_read
            .read(&mut readyness_checker)
            .await
            .inspect_err(|err| tracing::error!(?err, "stream read error"))?;

        let mut buf = ok_or!(buf_pool.request_one().await, return Ok(()));

        let n = read_buf(&mut stream_to_read, buf.as_mut())
            .await
            .inspect_err(|err| tracing::error!(?err, "stream read error"))?;

        tracing::trace!(bytes = n, "read");

        let total_sent = internal_state.total_read;
        let seq = internal_state.assign_seq(n as u64);

        if n == 0 {
            break;
        }

        let acked = {
            let lock = external_state.borrow();
            lock.acked
        };
        let unacked = total_sent - acked;
        let cur_avail_win = config.max_bytes_ahead - unacked;

        let lock = ok_or!(
            external_state
                .wait_for(|state| state.has_capacity_for(internal_state.total_read, &config))
                .instrument(tracing::trace_span!(
                    "wait for available window",
                    seq,
                    data_len = n,
                    unacked,
                    cur_avail_win,
                ))
                .await,
            return Ok(())
        );
        drop(lock);

        let evt = msg::Data {
            seq,
            data: buf.into(),
        }
        .into();
        ok_or!(evt_tx.send(evt).await, return Ok(()));
    }

    let evt = msg::Eof {
        seq: internal_state.next_seq,
    }
    .into();
    ok_or!(evt_tx.send(evt).await, return Ok(()));

    let lock = ok_or!(
        external_state
            .wait_for(|state| state.all_acked(internal_state.total_read))
            .instrument(tracing::trace_span!("wait for all remaining ack"))
            .await,
        return Ok(())
    );
    drop(lock);

    assert_eq!(internal_state.total_read, external_state.borrow().acked);

    ok_or!(
        external_state
            .wait_for(|s| s.eof_acked)
            .instrument(tracing::trace_span!("wait for eof acked"))
            .await,
        return Ok(())
    );

    return Ok(());
}

// Err(std::io::Error) when stream_to_read io error
// Ok(()) otherwise, including when cmd/evt channels are broken
pub async fn run(
    mut cmd_rx: impl Stream<Item = Command> + Unpin,
    mut evt_tx: impl Sink<Event> + Unpin,
    buf_pool: crate::buffer_pool::BufferPool,
    stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    first_pack: Option<BytesMut>,
    config: Config,
) -> Result<(), std::io::Error> {
    let internal_state = match first_pack {
        Some(first_pack) if first_pack.len() > 0 => {
            let len = first_pack.len() as u64;
            let evt = msg::Data {
                seq: 0,
                data: first_pack.into(),
            }
            .into();
            ok_or!(evt_tx.send(evt).await, return Ok(()));
            InternalState {
                next_seq: 1,
                total_read: len,
            }
        }
        _ => Default::default(),
    };

    let (external_state_tx, external_state_rx) = watch::channel(ExternalState {
        acked: 0,
        eof_acked: false,
    });

    let cmd_receiving_task = {
        async move {
            while let Some(cmd) = cmd_rx.next().await {
                tracing::debug!(cmd = ?cmd, "command");
                match cmd {
                    Command::Ack(ack) => {
                        external_state_tx.send_modify(|old| old.acked += ack.bytes as u64);
                    }
                    Command::EofAck(_) => {
                        external_state_tx.send_modify(|old| old.eof_acked = true);
                    }
                }
            }

            tracing::debug!("end of commands");
            return Ok::<_, std::io::Error>(());
        }
    };

    let stream_reading_task = stream_reading_loop(
        stream_to_read,
        buf_pool,
        evt_tx,
        internal_state,
        external_state_rx,
        config,
    );

    // cmd_receiving_task doesn't end by itself,
    // only when cmd_rx is broken
    // so we need to select instead of try_join
    tokio::select! {
        r = cmd_receiving_task => r,
        r = stream_reading_task => r
    }
}

#[cfg(test)]
mod tests {
    // use std::assert_matches::assert_matches;
    //
    // use super::*;
    //
    // use tokio::io::AsyncWriteExt as _;
    // use tokio_test::io::Builder;
    //
    // fn create_task(
    //     stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    // ) -> (
    //     impl futures::Sink<Command, Error = impl std::fmt::Debug> + Unpin,
    //     impl futures::Stream<Item = Event> + Send + Unpin,
    //     impl std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
    // ) {
    //     let (cmd_tx, cmd_rx) = futures::channel::mpsc::channel(1);
    //     let (event_tx, event_rx) = futures::channel::mpsc::channel(1);
    //
    //     let config = Config {
    //         max_packet_ahead: 2,
    //         max_packet_size: 1,
    //     };
    //
    //     let task = run(cmd_rx, event_tx, stream_to_read, None, config);
    //     (cmd_tx, event_rx, task)
    // }
    //
    // #[tokio::test]
    // #[test_log::test]
    // async fn happy_path() {
    //     let (mut cmd_tx, mut evt_rx, task) = create_task(Builder::new().read(&[1, 2]).build());
    //
    //     let task = tokio::spawn(task.instrument(tracing::info_span!("test target")));
    //
    //     let evt = evt_rx
    //         .next()
    //         .instrument(tracing::info_span!("receive evt 1"))
    //         .await
    //         .unwrap();
    //
    //     match evt {
    //         super::Event::Data(msg::Data { seq, data }) => {
    //             assert_eq!(seq, 0);
    //             assert_eq!(data.as_ref(), &[1]);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    //
    //     cmd_tx
    //         .send(Command::Ack(msg::Ack { bytes: 1 }))
    //         .instrument(tracing::info_span!("send cmd 1"))
    //         .await
    //         .unwrap();
    //
    //     let evt = evt_rx
    //         .next()
    //         .instrument(tracing::info_span!("receive evt 2"))
    //         .await
    //         .unwrap();
    //     match evt {
    //         super::Event::Data(msg::Data { seq, data }) => {
    //             assert_eq!(seq, 1);
    //             assert_eq!(data.as_ref(), &[2]);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    //
    //     cmd_tx
    //         .send(Command::Ack(msg::Ack { bytes: 1 }))
    //         .instrument(tracing::info_span!("send cmd 2"))
    //         .await
    //         .unwrap();
    //
    //     let evt = evt_rx
    //         .next()
    //         .instrument(tracing::info_span!("receive evt 3"))
    //         .await
    //         .unwrap();
    //     match evt {
    //         super::Event::Eof(msg::Eof { seq }) => {
    //             assert_eq!(seq, 2);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    //
    //     cmd_tx
    //         .send(Command::Ack(msg::Ack { bytes: 1 }))
    //         .instrument(tracing::info_span!("send cmd 3"))
    //         .await
    //         .unwrap();
    //
    //     task.await.unwrap().unwrap();
    // }
    //
    // #[test]
    // #[test_log::test]
    // fn respects_max_package_ahead() {
    //     let (mut cmd_tx, mut event_rx, task) =
    //         create_task(tokio_test::io::Builder::new().read(&[1, 2, 3]).build());
    //     let mut main_task = tokio_test::task::spawn(task);
    //
    //     tokio_test::assert_pending!(main_task.poll());
    //
    //     let evt =
    //         tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
    //     match evt {
    //         super::Event::Data(msg::Data { seq, data }) => {
    //             assert_eq!(seq, 0);
    //             assert_eq!(data.as_ref(), &[1]);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    //     tokio_test::assert_pending!(main_task.poll());
    //
    //     let evt =
    //         tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
    //     match evt {
    //         super::Event::Data(msg::Data { seq, data }) => {
    //             assert_eq!(seq, 1);
    //             assert_eq!(data.as_ref(), &[2]);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    //     tokio_test::assert_pending!(main_task.poll());
    //
    //     // no more packages due to max packet ahead reached
    //     tokio_test::assert_pending!(tokio_test::task::spawn(event_rx.next()).poll());
    //
    //     tokio_test::assert_ready!(
    //         tokio_test::task::spawn(
    //             cmd_tx
    //                 .send(Command::Ack(msg::Ack { bytes: 1 }))
    //                 .instrument(tracing::info_span!("send cmd 3"))
    //         )
    //         .poll()
    //     )
    //     .unwrap();
    //     tokio_test::assert_pending!(main_task.poll());
    //     tokio_test::assert_pending!(main_task.poll());
    //
    //     let evt =
    //         tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
    //     match evt {
    //         super::Event::Data(msg::Data { seq, data }) => {
    //             assert_eq!(seq, 2);
    //             assert_eq!(data.as_ref(), &[3]);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    // }
    //
    // #[tokio::test]
    // async fn quit_on_broken_cmd_stream() {
    //     let (cmd_tx, _event_rx, task) = create_task(tokio::io::duplex(1024).0);
    //
    //     drop(cmd_tx);
    //     let resut = task.await;
    //     assert_matches!(resut, Ok(()));
    // }
    //
    // #[tokio::test]
    // async fn quit_on_broken_evt_stream() {
    //     let (_cmd_tx, event_rx, task) = create_task(tokio::io::duplex(1024).0);
    //
    //     drop(event_rx);
    //     let result = task.await;
    //     assert_matches!(result, Ok(()));
    // }
    //
    // #[tokio::test]
    // async fn err_on_broken_stream() {
    //     let (_cmd_tx, _event_rx, task) = create_task(
    //         tokio_test::io::Builder::new()
    //             .read_error(std::io::Error::new(
    //                 std::io::ErrorKind::BrokenPipe,
    //                 "broken pipe",
    //             ))
    //             .build(),
    //     );
    //
    //     let result = task.await;
    //     assert_matches!(result, Err(_));
    //     let e = result.unwrap_err();
    //     assert_matches!(e.kind(), std::io::ErrorKind::BrokenPipe);
    // }
    //
    // #[test]
    // fn dont_block_on_stream() {
    //     let (stream_to_read, mut stream_to_write) = tokio::io::duplex(1024);
    //     tokio_test::block_on(stream_to_write.write_all(&[1, 2])).unwrap();
    //     let (mut cmd_tx, mut event_rx, task) = create_task(stream_to_read);
    //
    //     let mut main_task = tokio_test::task::spawn(task);
    //
    //     tokio_test::assert_pending!(main_task.poll());
    //
    //     match tokio_test::block_on(event_rx.next()).unwrap() {
    //         super::Event::Data(msg::Data { seq, data }) => {
    //             assert_eq!(seq, 0);
    //             assert_eq!(data.as_ref(), &[1]);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    //
    //     match tokio_test::block_on(event_rx.next()).unwrap() {
    //         super::Event::Data(msg::Data { seq, data }) => {
    //             assert_eq!(seq, 1);
    //             assert_eq!(data.as_ref(), &[2]);
    //         }
    //         _ => panic!("unexpected event"),
    //     };
    //
    //     // stream blocked
    //     tokio_test::assert_pending!(tokio_test::task::spawn(event_rx.next()).poll());
    //
    //     // main task blocked
    //     tokio_test::assert_pending!(main_task.poll());
    //
    //     // command sender does not get blocked
    //     let _ = tokio_test::assert_ready!(
    //         tokio_test::task::spawn(cmd_tx.send(Command::Ack(msg::Ack { bytes: 1 }))).poll()
    //     );
    //
    //     // main task & stream still blocked
    //     tokio_test::assert_pending!(main_task.poll());
    //     tokio_test::assert_pending!(tokio_test::task::spawn(event_rx.next()).poll());
    //
    //     // command sender does not get blocked
    //     let _ = tokio_test::assert_ready!(
    //         tokio_test::task::spawn(cmd_tx.send(Command::Ack(msg::Ack { bytes: 1 }))).poll()
    //     );
    //
    //     drop(stream_to_write);
    // }
}

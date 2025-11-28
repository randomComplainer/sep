use std::sync::Arc;
use std::sync::atomic::AtomicU16;

use derive_more::From;
use futures::prelude::*;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tracing::*;

use crate::session::sequence::{StreamEntry, StreamEntryValue};

use super::msg;

#[derive(Debug, From)]
pub enum Command {
    Data(#[from] msg::Data),
    Eof(#[from] msg::Eof),
}

#[derive(Debug, From)]
pub enum Event {
    Ack(#[from] msg::Ack),
}

// return Ok(()) on broken channel
macro_rules! try_emit_evt {
    ($evt_tx:ident, $evt:ident) => {
        let sp = info_span!("emit event:", evt = ?$evt);

        if let Err(_) = $evt_tx
            .send($evt)
            .instrument(sp)
            .await
        {
            error!("event channel is broken, exiting");
            return Ok(());
        }
    }
}

pub struct Config {
    pub max_packet_ahead: u16,
}

async fn streaming_loop(
    mut stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    mut data_rx: impl Stream<Item = StreamEntry> + Unpin,
    mut evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Unpin,
    buffed_count: Arc<AtomicU16>,
) -> Result<(), std::io::Error> {
    while let Some(StreamEntry(seq, entry)) = data_rx
        .next()
        .instrument(debug_span!("receive local stream entry"))
        .await
    {
        match entry {
            StreamEntryValue::Data(data) => {
                debug!(seq, data = data.len(), "stream entry: data");
                stream_to_write
                    .write_all(data.as_ref())
                    .instrument(info_span!("write to stream", seq, len = data.len()))
                    .await?;

                buffed_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

                let evt = msg::Ack { seq }.into();

                try_emit_evt!(evt_tx, evt);
            }
            StreamEntryValue::Eof => {
                debug!(seq, "stream entry: eof");

                buffed_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(0, buffed_count.load(std::sync::atomic::Ordering::SeqCst));

                let evt = msg::Ack { seq }.into();
                try_emit_evt!(evt_tx, evt);
                info!(seq = seq, "eof reached, streaming exits");
                return Ok(());
            }
        }
    }

    error!("stream entry channel is broken, exiting");

    Ok(())
}

pub async fn run(
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Unpin + Send + 'static,
    stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    config: Config,
) -> Result<(), std::io::Error> {
    // stream interactions are on separate Future to avoid blocking command sender

    let mut next_seq = 0u16;
    let buffed_count = Arc::new(AtomicU16::new(0));
    let (mut local_stream_data_tx, local_stream_data_rx) =
        futures::channel::mpsc::channel(config.max_packet_ahead as usize + 1);

    let mut heap = std::collections::BinaryHeap::<std::cmp::Reverse<StreamEntry>>::with_capacity(
        (super::MAX_DATA_AHEAD + 1) as usize,
    );

    let cmd_receiving_task = {
        let buffed_count = buffed_count.clone();
        async move {
            while let Some(cmd) = cmd_rx
                .next()
                .instrument(debug_span!("receive command"))
                .await
            {
                debug!(cmd = ?cmd, "command received");

                buffed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let loaded_buffed_count = buffed_count.load(std::sync::atomic::Ordering::SeqCst);
                if loaded_buffed_count > config.max_packet_ahead {
                    panic!("too many data cached");
                }

                match cmd {
                    Command::Data(data) => {
                        heap.push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));
                    }
                    Command::Eof(eof) => {
                        heap.push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
                    }
                };

                debug!("buffed count: {}", loaded_buffed_count);

                while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                    let stream_entry = heap.pop().unwrap();
                    let span = debug_span!("send stream entry", seq = stream_entry.0.0);
                    if let Err(_) = local_stream_data_tx
                        .send(stream_entry.0)
                        .instrument(span)
                        .await
                    {
                        error!("local stream channel is broken, exiting");
                        return Ok(());
                    }

                    next_seq += 1;
                }
            }

            error!("command channel is broken, exiting");
            return Ok::<_, std::io::Error>(());
        }
    }
    .instrument(info_span!("command receiving task"));

    let streaming_task =
        streaming_loop(stream_to_write, local_stream_data_rx, evt_tx, buffed_count)
            .instrument(info_span!("streaming loop"));

    tokio::select! {
        r = streaming_task => r,
        r = cmd_receiving_task => r
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use bytes::BytesMut;
    use tokio::io::AsyncReadExt;

    use super::*;

    fn create_task(
        max_packet_ahead: u16,
        stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    ) -> (
        impl futures::Sink<Command, Error = impl std::fmt::Debug> + Unpin,
        impl futures::Stream<Item = Event> + Send + Unpin,
        impl std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
    ) {
        let (cmd_tx, cmd_rx) = futures::channel::mpsc::channel(1);
        let (event_tx, event_rx) = futures::channel::mpsc::channel(1);

        let config = Config { max_packet_ahead };

        let task = run(cmd_rx, event_tx, stream_to_write, config);
        (cmd_tx, event_rx, task)
    }

    #[test_log::test]
    fn happy_path() {
        let (mut cmd_tx, mut event_rx, task) =
            create_task(1, tokio_test::io::Builder::new().write(&[1, 2, 3]).build());

        let mut task = tokio_test::task::spawn(task);

        for i in 0..=2 {
            tokio_test::assert_pending!(task.poll());

            tokio_test::assert_ready!(
                tokio_test::task::spawn(cmd_tx.send(Command::Data(msg::Data {
                    seq: i.try_into().unwrap(),
                    data: BytesMut::from([(i + 1).try_into().unwrap()].as_ref()),
                })))
                .poll()
            )
            .unwrap();

            tokio_test::assert_pending!(task.poll());
            tokio_test::assert_pending!(task.poll());
            tokio_test::assert_pending!(task.poll());

            let evt =
                tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();

            match evt {
                super::Event::Ack(msg::Ack { seq }) => {
                    assert_eq!(seq, i);
                }
            };
        }

        tokio_test::assert_ready!(
            tokio_test::task::spawn(cmd_tx.send(Command::Eof(msg::Eof { seq: 3 }))).poll()
        )
        .unwrap();

        tokio_test::block_on(task).unwrap();

        let evt =
            tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();

        match evt {
            super::Event::Ack(msg::Ack { seq }) => {
                assert_eq!(seq, 3);
            }
        };
    }

    #[tokio::test]
    async fn quit_on_broken_cmd_stream() {
        let (cmd_tx, mut _event_rx, main_task) = create_task(3, tokio::io::duplex(0).0);
        drop(cmd_tx);

        let result = main_task.await;
        assert_matches!(result, Ok(()));
    }

    // it's slightly concerning that
    // event channel's broken state is not checked
    // until next packet is written to the stream
    #[test_log::test(tokio::test)]
    async fn quit_on_broken_evt_stream() {
        let (mut cmd_tx, evt_rx, main_task) =
            create_task(3, tokio_test::io::Builder::new().write(&[1]).build());
        drop(evt_rx);

        let main_task = tokio::spawn(main_task);

        cmd_tx
            .send(Command::Data(msg::Data {
                seq: 0.try_into().unwrap(),
                data: BytesMut::from([1].as_ref()),
            }))
            .await
            .unwrap();

        let result = main_task.await.unwrap();
        assert_matches!(result, Ok(()));
    }

    #[tokio::test]
    async fn io_error() {
        let (mut cmd_tx, mut _event_rx, main_task) = create_task(
            1,
            tokio_test::io::Builder::new()
                .write_error(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "broken pipe",
                ))
                .build(),
        );

        cmd_tx
            .send(Command::Data(msg::Data {
                seq: 0.try_into().unwrap(),
                data: BytesMut::from([1].as_ref()),
            }))
            .await
            .unwrap();

        let result = main_task.await;

        assert_matches!(result, Err(std::io::Error { .. }));
        let e = result.unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn panic_on_exceed_max_packet_ahead() {
        let (mut cmd_tx, mut _event_rx, main_task) = create_task(3, tokio::io::duplex(0).0);
        let send_cmds = async move {
            for i in 1..=4 {
                cmd_tx
                    .send(Command::Data(msg::Data {
                        seq: i.try_into().unwrap(),
                        data: BytesMut::from([i.try_into().unwrap()].as_ref()),
                    }))
                    .await
                    .unwrap();
            }
        };

        let send_cmds = tokio::spawn(send_cmds);
        let main_task = tokio::spawn(main_task);

        let (send_cmds_result, main_task_result) = tokio::join!(send_cmds, main_task);

        send_cmds_result.unwrap();

        assert!(main_task_result.is_err());
        assert!(main_task_result.unwrap_err().is_panic());
    }

    #[test]
    fn dont_block_on_stream() {
        let (stream_write, mut stream_read) = tokio::io::duplex(1);

        let (mut cmd_tx, mut event_rx, main_task) = create_task(10, stream_write);

        let mut main_task = tokio_test::task::spawn(main_task);

        for i in 0..10 {
            let mut cmd_sending = tokio_test::task::spawn(cmd_tx.send(Command::Data(msg::Data {
                seq: i.try_into().unwrap(),
                data: BytesMut::from([i.try_into().unwrap()].as_ref()),
            })));
            tokio_test::assert_ready!(cmd_sending.poll()).unwrap();
            tokio_test::assert_pending!(main_task.poll());
        }

        let mut buf = BytesMut::with_capacity(9);

        for i in 0..10 {
            // forward event
            tokio_test::assert_pending!(main_task.poll());
            let e =
                tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();

            match e {
                super::Event::Ack(msg::Ack { seq }) => {
                    assert_eq!(seq, i);
                }
            };

            let n = tokio_test::assert_ready!(
                tokio_test::task::spawn(stream_read.read_buf(&mut buf)).poll()
            )
            .unwrap();
            assert_eq!(n, 1);

            tokio_test::assert_pending!(main_task.poll());
        }

        assert_eq!(buf.as_ref(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].as_ref());
    }
}

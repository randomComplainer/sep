use bytes::BytesMut;
use derive_more::From;
use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tracing::*;

use super::SequencingError;
use super::msg;

#[derive(Debug, From)]
pub enum Command {
    Ack(#[from] msg::Ack),
}

#[derive(Debug, From)]
pub enum Event {
    Data(#[from] msg::Data),
    Eof(#[from] msg::Eof),
}

pub struct Config {
    pub max_packet_ahead: u16,
    pub max_packet_size: usize,
}

// simple wrapper around u16 sequence number
// the number save is actual max acked seq + 1
// 0 is reserved for no ack yet
struct MaxAcked(u16);
impl MaxAcked {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn update(&mut self, new_ack: u16) -> bool {
        let n = new_ack + 1;
        if n > self.0 {
            self.0 = n;
            true
        } else {
            false
        }
    }

    // unacked count if next_seq is sent
    pub fn unacked_count_if(&self, next_seq: u16) -> u16 {
        next_seq - self.0
    }

    // pass other value in to avoid underflow when checking for equality with 0
    pub fn eq(&self, seq: u16) -> bool {
        self.0 == seq + 1
    }
}

async fn try_emit_evt(
    evt_tx: &mut (impl Sink<Event, Error = impl std::fmt::Debug> + Unpin),
    evt: Event,
) -> Result<(), SequencingError> {
    let sp = info_span!("emit event:", evt = ?evt);
    evt_tx
        .send(evt)
        .instrument(sp)
        .await
        .inspect_err(|_| error!("sequenced channel is broken"))
        .map_err(|_| SequencingError::SequencedBroken)
}

async fn stream_reading_loop(
    mut seq: u16,
    mut stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    mut evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Unpin,
    mut max_acked_rx: tokio::sync::watch::Receiver<MaxAcked>,
    config: Config,
) -> Result<(), SequencingError> {
    loop {
        let lock = match max_acked_rx
            .wait_for(|max_acked| max_acked.unacked_count_if(seq) < config.max_packet_ahead)
            .await
        {
            Ok(lock) => lock,
            Err(_) => {
                error!("max_acked_rx is broken");
                return Err(SequencingError::SequencedBroken);
            }
        };

        drop(lock);

        // TODO: reuse buf
        let mut buf = bytes::BytesMut::with_capacity(config.max_packet_size);
        let n = stream_to_read.read_buf(&mut buf).await.inspect_err(|err| {
            error!("stream read error: {:?}", err);
        })?;

        if n != 0 {
            let evt = msg::Data { seq, data: buf }.into();
            try_emit_evt(&mut evt_tx, evt).await?;
        } else {
            let evt = msg::Eof { seq }.into();
            try_emit_evt(&mut evt_tx, evt).await?;
            info!(seq = seq, "stream eof reached, wait for eof to be acked");

            let lock = max_acked_rx
                .wait_for(|max_acked| max_acked.eq(seq))
                .await
                .expect("max_acked_rx is broken");
            drop(lock);

            info!(seq = seq, "ack of eof reached");
            return Ok(());
        };

        seq += 1;
    }
}

pub async fn run(
    mut cmd_rx: impl Stream<Item = Command> + Unpin,
    mut event_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Unpin,
    stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    first_pack: Option<BytesMut>,
    config: Config,
) -> Result<(), SequencingError> {
    let mut seq = 0;

    if let Some(first_pack) = first_pack {
        let evt = msg::Data {
            seq: 0,
            data: first_pack,
        }
        .into();
        try_emit_evt(&mut event_tx, evt).await?;
        seq += 1;
    }

    let (max_acked_tx, max_acked_rx) = tokio::sync::watch::channel(MaxAcked::new());

    let cmd_receiving_task = async move {
        while let Some(cmd) = cmd_rx.next().await {
            debug!(cmd = ?cmd, "command received");
            match cmd {
                Command::Ack(ack) => {
                    max_acked_tx.send_if_modified(|old| old.update(ack.seq));
                }
            }
        }

        error!("unexpected end of command channel");

        return Err::<(), _>(SequencingError::SequencedBroken);
    };

    let stream_reading_task =
        stream_reading_loop(seq, stream_to_read, event_tx, max_acked_rx, config);

    // Two separate tasks are needed so that
    // waiting on stream does not block transmitter of commands.
    // Command receiving task never returns Ok,
    // so it's either
    // - one of the tasks errored
    // - stream_reading_task returned Ok
    tokio::select! (
        err = cmd_receiving_task => Err(err.unwrap_err()),
        r = stream_reading_task => r,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::AsyncWriteExt as _;

    fn create_task() -> (
        impl futures::Sink<Command, Error = impl std::fmt::Debug> + Unpin,
        impl futures::Stream<Item = Event> + Send + Unpin,
        impl tokio::io::AsyncWrite + Unpin + Send + 'static,
        impl std::future::Future<Output = Result<(), SequencingError>> + Send + 'static,
    ) {
        let (cmd_tx, cmd_rx) = futures::channel::mpsc::channel(1);
        let (event_tx, event_rx) = futures::channel::mpsc::channel(1);

        let (stream_read, stream_write) = tokio::io::duplex(1024);

        let config = Config {
            max_packet_ahead: 2,
            max_packet_size: 1,
        };

        let task = run(cmd_rx, event_tx, stream_read, None, config);
        (cmd_tx, event_rx, stream_write, task)
    }

    #[tokio::test]
    #[test_log::test]
    async fn happy_path() {
        let (mut cmd_tx, mut evt_rx, mut stream_write, task) = create_task();

        let main_task = async move {
            let task = tokio::spawn(task.instrument(tracing::info_span!("test target")));

            let evt = evt_rx
                .next()
                .instrument(tracing::info_span!("receive evt 1"))
                .await
                .unwrap();

            match evt {
                super::Event::Data(msg::Data { seq, data }) => {
                    assert_eq!(seq, 0);
                    assert_eq!(data.as_ref(), &[1]);
                }
                _ => panic!("unexpected event"),
            };

            cmd_tx
                .send(Command::Ack(msg::Ack { seq: 0 }))
                .instrument(tracing::info_span!("send cmd 1"))
                .await
                .unwrap();

            let evt = evt_rx
                .next()
                .instrument(tracing::info_span!("receive evt 2"))
                .await
                .unwrap();
            match evt {
                super::Event::Data(msg::Data { seq, data }) => {
                    assert_eq!(seq, 1);
                    assert_eq!(data.as_ref(), &[2]);
                }
                _ => panic!("unexpected event"),
            };

            cmd_tx
                .send(Command::Ack(msg::Ack { seq: 1 }))
                .instrument(tracing::info_span!("send cmd 2"))
                .await
                .unwrap();

            let evt = evt_rx
                .next()
                .instrument(tracing::info_span!("receive evt 3"))
                .await
                .unwrap();
            match evt {
                super::Event::Eof(msg::Eof { seq }) => {
                    assert_eq!(seq, 2);
                }
                _ => panic!("unexpected event"),
            };

            cmd_tx
                .send(Command::Ack(msg::Ack { seq: 2 }))
                .instrument(tracing::info_span!("send cmd 3"))
                .await
                .unwrap();

            task.await.unwrap().unwrap();
        }
        .instrument(tracing::info_span!("parent task"));

        let other_side_of_stream = async move {
            stream_write.write_all(&[1, 2]).await.unwrap();
        }
        .instrument(tracing::info_span!("other side of stream"));

        tokio::join!(other_side_of_stream, main_task);
    }

    #[test]
    #[test_log::test]
    fn respects_max_package_ahead() {
        let (mut cmd_tx, mut event_rx, mut stream_write, task) = create_task();
        let mut main_task = tokio_test::task::spawn(task);

        tokio_test::assert_ready!(
            tokio_test::task::spawn(stream_write.write_all(&[1, 2, 3, 4])).poll()
        )
        .unwrap();

        tokio_test::assert_pending!(main_task.poll());

        let evt =
            tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
        match evt {
            super::Event::Data(msg::Data { seq, data }) => {
                assert_eq!(seq, 0);
                assert_eq!(data.as_ref(), &[1]);
            }
            _ => panic!("unexpected event"),
        };
        tokio_test::assert_pending!(main_task.poll());

        let evt =
            tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
        match evt {
            super::Event::Data(msg::Data { seq, data }) => {
                assert_eq!(seq, 1);
                assert_eq!(data.as_ref(), &[2]);
            }
            _ => panic!("unexpected event"),
        };
        tokio_test::assert_pending!(main_task.poll());

        // no more packages due to max packet ahead reached
        tokio_test::assert_pending!(tokio_test::task::spawn(event_rx.next()).poll());

        tokio_test::assert_ready!(
            tokio_test::task::spawn(
                cmd_tx
                    .send(Command::Ack(msg::Ack { seq: 0 }))
                    .instrument(tracing::info_span!("send cmd 3"))
            )
            .poll()
        )
        .unwrap();
        tokio_test::assert_pending!(main_task.poll());

        let evt =
            tokio_test::assert_ready!(tokio_test::task::spawn(event_rx.next()).poll()).unwrap();
        match evt {
            super::Event::Data(msg::Data { seq, data }) => {
                assert_eq!(seq, 2);
                assert_eq!(data.as_ref(), &[3]);
            }
            _ => panic!("unexpected event"),
        };
    }

    #[test]
    fn dont_block_on_stream() {
        let (mut cmd_tx, mut event_rx, mut stream_write, task) = create_task();

        let mut main_task = tokio_test::task::spawn(task);

        let _ = tokio_test::assert_ready!(
            tokio_test::task::spawn(stream_write.write_all(&[1, 2])).poll()
        );

        tokio_test::assert_pending!(main_task.poll());

        match tokio_test::block_on(event_rx.next()).unwrap() {
            super::Event::Data(msg::Data { seq, data }) => {
                assert_eq!(seq, 0);
                assert_eq!(data.as_ref(), &[1]);
            }
            _ => panic!("unexpected event"),
        };

        match tokio_test::block_on(event_rx.next()).unwrap() {
            super::Event::Data(msg::Data { seq, data }) => {
                assert_eq!(seq, 1);
                assert_eq!(data.as_ref(), &[2]);
            }
            _ => panic!("unexpected event"),
        };

        // stream blocked
        tokio_test::assert_pending!(tokio_test::task::spawn(event_rx.next()).poll());

        // main task blocked
        tokio_test::assert_pending!(main_task.poll());

        // command sender does not get blocked
        let _ = tokio_test::assert_ready!(
            tokio_test::task::spawn(cmd_tx.send(Command::Ack(msg::Ack { seq: 0 }))).poll()
        );

        // main task & stream still blocked
        tokio_test::assert_pending!(main_task.poll());
        tokio_test::assert_pending!(tokio_test::task::spawn(event_rx.next()).poll());

        // command sender does not get blocked
        let _ = tokio_test::assert_ready!(
            tokio_test::task::spawn(cmd_tx.send(Command::Ack(msg::Ack { seq: 1 }))).poll()
        );
    }
}

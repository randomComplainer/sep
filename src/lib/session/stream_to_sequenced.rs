use bytes::BytesMut;
use derive_more::From;
use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tracing::*;

use super::msg;

#[derive(Debug, From)]
pub enum Command {
    Ack(#[from] msg::Ack),
}

#[derive(Debug, From)]
pub enum Event {
    Data(#[from] msg::Data),
    Eof(#[from] msg::Eof),
    IoError(#[from] msg::IoError),
}

pub struct Config {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
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

async fn stream_reading_loop(
    mut seq: u16,
    mut stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    mut evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Unpin,
    mut max_acked_rx: tokio::sync::watch::Receiver<MaxAcked>,
    config: Config,
) -> Result<(), std::io::Error> {
    loop {
        let lock = match max_acked_rx
            .wait_for(|max_acked| max_acked.unacked_count_if(seq) < config.max_packet_ahead)
            .await
        {
            Ok(lock) => lock,
            Err(_) => {
                error!("max_acked_rx is broken, exiting");
                return Ok(());
            }
        };

        drop(lock);

        // TODO: reuse buf
        let mut buf = bytes::BytesMut::with_capacity(config.max_packet_size as usize);
        let n = match stream_to_read.read_buf(&mut buf).await {
            Ok(n) => n,
            Err(err) => {
                error!("stream read error: {:?}", err);
                let evt = msg::IoError.into();
                try_emit_evt!(evt_tx, evt);
                return Err(err);
            }
        };

        if n != 0 {
            let evt = msg::Data { seq, data: buf }.into();
            try_emit_evt!(evt_tx, evt);
        } else {
            let evt = msg::Eof { seq }.into();
            try_emit_evt!(evt_tx, evt);
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

// Err(std::io::Error) when stream_to_read io error
// Ok(()) otherwise, including when cmd/evt channels are broken
pub async fn run(
    mut cmd_rx: impl Stream<Item = Command> + Unpin,
    mut event_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Unpin,
    stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    first_pack: Option<BytesMut>,
    config: Config,
) -> Result<(), std::io::Error> {
    let mut seq = 0;

    if let Some(first_pack) = first_pack {
        if first_pack.len() > 0 {
            let evt = msg::Data {
                seq: 0,
                data: first_pack,
            }
            .into();
            try_emit_evt!(event_tx, evt);
            seq += 1;
        }
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

        error!("command channel is broken, exiting");

        return Ok::<_, std::io::Error>(());
    }
    .instrument(info_span!("command receiving task"));

    let stream_reading_task =
        stream_reading_loop(seq, stream_to_read, event_tx, max_acked_rx, config)
            .instrument(info_span!("stream reading task"));

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
    use std::assert_matches::assert_matches;

    use super::*;

    use tokio::io::AsyncWriteExt as _;
    use tokio_test::io::Builder;

    fn create_task(
        stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    ) -> (
        impl futures::Sink<Command, Error = impl std::fmt::Debug> + Unpin,
        impl futures::Stream<Item = Event> + Send + Unpin,
        impl std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
    ) {
        let (cmd_tx, cmd_rx) = futures::channel::mpsc::channel(1);
        let (event_tx, event_rx) = futures::channel::mpsc::channel(1);

        let config = Config {
            max_packet_ahead: 2,
            max_packet_size: 1,
        };

        let task = run(cmd_rx, event_tx, stream_to_read, None, config);
        (cmd_tx, event_rx, task)
    }

    #[tokio::test]
    #[test_log::test]
    async fn happy_path() {
        let (mut cmd_tx, mut evt_rx, task) = create_task(Builder::new().read(&[1, 2]).build());

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

    #[test]
    #[test_log::test]
    fn respects_max_package_ahead() {
        let (mut cmd_tx, mut event_rx, task) =
            create_task(tokio_test::io::Builder::new().read(&[1, 2, 3]).build());
        let mut main_task = tokio_test::task::spawn(task);

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

    #[tokio::test]
    async fn quit_on_broken_cmd_stream() {
        let (cmd_tx, _event_rx, task) = create_task(tokio::io::duplex(1024).0);

        drop(cmd_tx);
        let resut = task.await;
        assert_matches!(resut, Ok(()));
    }

    #[tokio::test]
    async fn quit_on_broken_evt_stream() {
        let (_cmd_tx, event_rx, task) = create_task(tokio::io::duplex(1024).0);

        drop(event_rx);
        let result = task.await;
        assert_matches!(result, Ok(()));
    }

    #[tokio::test]
    async fn err_on_broken_stream() {
        let (_cmd_tx, _event_rx, task) = create_task(
            tokio_test::io::Builder::new()
                .read_error(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "broken pipe",
                ))
                .build(),
        );

        let result = task.await;
        assert_matches!(result, Err(_));
        let e = result.unwrap_err();
        assert_matches!(e.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn dont_block_on_stream() {
        let (stream_to_read, mut stream_to_write) = tokio::io::duplex(1024);
        tokio_test::block_on(stream_to_write.write_all(&[1, 2])).unwrap();
        let (mut cmd_tx, mut event_rx, task) = create_task(stream_to_read);

        let mut main_task = tokio_test::task::spawn(task);

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

        drop(stream_to_write);
    }
}

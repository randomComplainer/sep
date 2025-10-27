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
    pub max_package_ahead: u16,
    pub max_package_size: usize,
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
        let lock = max_acked_rx
            .wait_for(|max_acked| max_acked.unacked_count_if(seq) <= config.max_package_ahead)
            .await
            .expect("max_acked_rx is broken");
        drop(lock);

        // TODO: reuse buf
        let mut buf = bytes::BytesMut::with_capacity(config.max_package_size);
        let n = stream_to_read.read_buf(&mut buf).await?;

        if n != 0 {
            let evt = msg::Data { seq, data: buf }.into();
            try_emit_evt(&mut evt_tx, evt).await?;
        } else {
            let evt = msg::Eof { seq }.into();
            try_emit_evt(&mut evt_tx, evt).await?;
            info!(seq = seq, "stream eof reached");

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

    #[test]
    fn dont_block_on_stream() {
        let (mut cmd_tx, cmd_rx) = futures::channel::mpsc::channel(1);
        let (stream_read, mut stream_write) = tokio::io::duplex(1024);
        let config = Config {
            max_package_ahead: 2,
            max_package_size: 1,
        };
        let (event_tx, mut event_rx) = futures::channel::mpsc::channel(1);

        let mut main_task =
            tokio_test::task::spawn(run(cmd_rx, event_tx, stream_read, None, config));

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

use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use derive_more::From;
use futures::SinkExt as _;
use futures::StreamExt as _;
use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::ok_or;
use crate::protocol::msg::session as msg;
use crate::sink_ext::SinkExt;

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

mod state {
    use super::*;

    #[derive(Debug, From)]
    pub enum Cmd {
        Buf(#[from] msg::Buf),
        Eof,
        Ack(u32),
        EofAck,
    }

    #[derive(Debug, From)]
    #[cfg_attr(test, derive(Eq, PartialEq))]
    pub enum Action {
        Data(#[from] msg::Data),
        Eof(#[from] msg::Eof),
        Done,
    }

    pub struct State {
        max_data_ahead: u64,
        acked: u64,
        eof_acked: bool,
        total_sent: u64,
        next_seq: u16,
        queued: Option<msg::Buf>,
    }

    impl State {
        pub fn new(max_data_ahead: u64) -> Self {
            Self {
                max_data_ahead,
                acked: 0,
                eof_acked: false,
                total_sent: 0,
                next_seq: 0,
                queued: None,
            }
        }

        pub fn on_cmd(&mut self, cmd: Cmd) -> Vec<Action> {
            match cmd {
                Cmd::Buf(buf) => self.on_data(buf),
                Cmd::Eof => self.on_eof(),
                Cmd::Ack(x) => self.on_ack(x),
                Cmd::EofAck => self.on_eof_ack(),
            }
        }

        fn on_data(&mut self, data: msg::Buf) -> Vec<Action> {
            self.try_send_data(data)
        }

        fn on_eof(&mut self) -> Vec<Action> {
            assert!(self.queued.is_none());
            vec![msg::Eof { seq: self.next_seq }.into()]
        }

        fn on_ack(&mut self, ack_bytes: u32) -> Vec<Action> {
            self.acked += ack_bytes as u64;
            if let Some(data) = self.queued.take() {
                self.try_send_data(data)
            } else {
                self.check_done()
            }
        }

        fn on_eof_ack(&mut self) -> Vec<Action> {
            assert!(!self.eof_acked);
            self.eof_acked = true;
            self.check_done()
        }

        fn check_done(&self) -> Vec<Action> {
            if self.eof_acked && self.acked == self.total_sent {
                assert!(self.queued.is_none());
                vec![Action::Done]
            } else {
                Default::default()
            }
        }

        fn try_send_data(&mut self, data: msg::Buf) -> Vec<Action> {
            assert!(self.queued.is_none());
            let len = data.as_ref().len() as u64;
            if self.total_sent + len - self.acked <= self.max_data_ahead {
                let seq = self.next_seq;

                self.total_sent += len;
                self.next_seq += 1;

                vec![msg::Data { seq, data }.into()]
            } else {
                self.queued = Some(data);
                Default::default()
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn happy_path() {
            let mut state = State::new(1);

            let data: msg::Buf = BytesMut::from_iter([0u8].into_iter()).into();

            let actions = state.on_data(data.clone());

            assert_eq!(
                &[Action::Data(msg::Data { seq: 0, data })],
                actions.as_slice()
            );

            let data: msg::Buf = BytesMut::from_iter([1u8].into_iter()).into();
            let actions = state.on_data(data.clone());
            assert_eq!(0, actions.len());

            let actions = state.on_ack(1);
            assert_eq!(
                &[Action::Data(msg::Data { seq: 1, data })],
                actions.as_slice()
            );

            let actions = state.on_eof();
            assert_eq!(&[Action::Eof(msg::Eof { seq: 2 })], actions.as_slice());

            let actions = state.on_eof_ack();
            assert_eq!(0, actions.len());

            let actions = state.on_ack(1);
            assert_eq!(&[Action::Done], actions.as_slice());
        }
    }
}

async fn create_package_stream(
    buf_pool: crate::buffer_pool::BufferPool,
    mut stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    mut package_tx: impl Sink<Option<msg::Buf>> + Unpin,
    first_pack: Option<BytesMut>,
    next_pkg_signal: Arc<tokio::sync::Notify>,
) -> std::io::Result<()> {
    if let Some(buf) = first_pack
        && buf.len() > 0
    {
        next_pkg_signal.notified().await;
        ok_or!(package_tx.send(Some(buf.into())).await, return Ok(()));
    }

    loop {
        next_pkg_signal.notified().await;
        let mut buf = ok_or!(buf_pool.request_one().await, return Ok(()));

        let n = read_buf(&mut stream_to_read, buf.as_mut())
            .await
            .inspect_err(|err| tracing::error!(?err, "stream read error"))?;

        if n == 0 {
            ok_or!(package_tx.send(None).await, return Ok(()));
            break;
        } else {
            ok_or!(package_tx.send(Some(buf.into())).await, return Ok(()));
        }
    }

    Ok(())
}

// Err(std::io::Error) when stream_to_read io error
// Ok(()) otherwise, including when cmd/evt channels are broken
pub async fn run(
    cmd_rx: impl Stream<Item = Command> + Unpin,
    evt_tx: impl Sink<Event> + Unpin,
    buf_pool: crate::buffer_pool::BufferPool,
    stream_to_read: impl AsyncRead + Unpin + Send + 'static,
    first_pack: Option<BytesMut>,
    config: Config,
) -> Result<(), std::io::Error> {
    let (pkg_tx, pkg_rx) = futures::channel::mpsc::unbounded();
    let next_pkg_signal = Arc::new(tokio::sync::Notify::new());

    let mut evt_tx = evt_tx.inspect(|evt| tracing::debug!(evt=?evt, "event"));

    let io_task = create_package_stream(
        buf_pool,
        stream_to_read,
        pkg_tx,
        first_pack,
        Arc::clone(&next_pkg_signal),
    );

    let mut state_cmd_stream = futures::stream::select(
        pkg_rx.then(|pkg| {
            std::future::ready(match pkg {
                Some(buf) => state::Cmd::Buf(buf),
                None => state::Cmd::Eof,
            })
        }),
        cmd_rx.then(|cmd| {
            std::future::ready(match cmd {
                Command::Ack(ack) => state::Cmd::Ack(ack.bytes),
                Command::EofAck(_) => state::Cmd::EofAck,
            })
        }),
    );

    let state_task = async move {
        let mut state = state::State::new(config.max_bytes_ahead);
        next_pkg_signal.notify_one();

        while let Some(cmd) = state_cmd_stream.next().await {
            tracing::debug!(cmd = ?cmd, "command");

            let actions = state.on_cmd(cmd);
            for action in actions {
                match action {
                    state::Action::Data(data) => {
                        next_pkg_signal.notify_one();
                        ok_or!(evt_tx.send(data.into()).await, return);
                    }
                    state::Action::Eof(eof) => ok_or!(evt_tx.send(eof.into()).await, return),
                    state::Action::Done => {
                        tracing::debug!("done");
                        return;
                    }
                };
            }
        }
    };

    match future::select(Box::pin(io_task), Box::pin(state_task)).await {
        future::Either::Left((io_result, state_task)) => {
            io_result?;
            state_task.await;
            Ok(())
        }
        future::Either::Right(_) => Ok(()),
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

use std::fmt::Debug;

use futures::prelude::*;
use tokio::sync::oneshot;
use tracing::Instrument as _;

use crate::prelude::*;
use crate::protocol::MessageReader;
use crate::protocol::MessageWriter;
use crate::protocol::msg::conn::ConnMsg;

#[derive(Clone)]
pub struct Config {
    io_write_timeout: std::time::Duration,
    ping_interval: std::time::Duration,
    aliveness_timeout: std::time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            io_write_timeout: std::time::Duration::from_secs(20),
            ping_interval: std::time::Duration::from_secs(5),
            aliveness_timeout: std::time::Duration::from_secs(25),
        }
    }
}

async fn write_loop<MessageToWrite>(
    config: Config,
    mut stream_write: impl MessageWriter<Message = ConnMsg<MessageToWrite>> + Send,
    mut msg_sender_tx: impl Sink<oneshot::Sender<MessageToWrite>> + Unpin,
    mut close_rx: gentle_close::Receiver,
) -> std::io::Result<()>
where
    MessageToWrite: Send + Debug + Unpin + 'static,
{
    let mut ping_timer = Box::pin(tokio::time::sleep(config.ping_interval));
    let mut ping_counter = 0;
    let (send_one_tx, send_one_rx) = oneshot::channel();
    let mut send_one_rx = Box::pin(send_one_rx);

    macro_rules! write_msg {
        ($msg:expr) => {
            let msg:ConnMsg<MessageToWrite> = $msg;
            let span = tracing::trace_span!("write msg", msg=?msg);
            tokio::time::timeout(config.io_write_timeout, stream_write.send_msg(msg)).map(
                |timeout_result| match timeout_result {
                    Ok(x) => x,
                    Err(_) => {
                        tracing::error!("timeout writing msg");
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "timeout writing msg",
                    )) },
                },
            )
                .instrument(span)
                .await?;
            ping_timer  = Box::pin(tokio::time::sleep(config.ping_interval));

        };
    }

    macro_rules! shutdown {
        () => {
            drop(send_one_rx);
            write_msg!(ConnMsg::EndOfStream);
            stream_write.shutdown().await?;
            drop(ping_timer);
            return Ok(());
        };
    }

    if let Err(_) = msg_sender_tx.send(send_one_tx).await {
        tracing::error!("all writehandle dropped, exiting");
        shutdown!();
    }

    loop {
        tokio::select! {
            write_one = send_one_rx.as_mut() => {
                let msg = match write_one  {
                    Ok(x) => x,
                    Err(_) => {
                        let (send_one_tx, new_send_one_rx) = tokio::sync::oneshot::channel();
                        send_one_rx = Box::pin(new_send_one_rx);
                        if let Err(_) = msg_sender_tx.send(send_one_tx).await {
                            tracing::error!("all writehandle dropped");
                            shutdown!();
                        }
                        continue;
                    }
                };

                write_msg!(msg.into());

                let (new_wirte_one_tx, new_write_one_rx) = tokio::sync::oneshot::channel();

                if let Err(_) = msg_sender_tx.send(new_wirte_one_tx).await {
                    tracing::error!("sender_tx is broken, exiting");
                    shutdown!();
                }

                send_one_rx = Box::pin(new_write_one_rx);
            },
            _ = ping_timer.as_mut() => {
                tracing::trace!(count = ping_counter, "ping");
                write_msg!(ConnMsg::Ping);
                ping_counter += 1;
            },
            close_signal = close_rx.receive() => {
                match close_signal {
                    Ok(_) => {
                        shutdown!();
                    },
                    Err(_) => {
                        tracing::error!("gentle close sender dropped, exiting");
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "gentle close sender dropped"
                        ));
                    },
                };
            }
        }
    }
}

async fn read_loop<MessageToRead>(
    config: Config,
    mut stream_read: impl MessageReader<Message = ConnMsg<MessageToRead>> + Send,
    mut msg_tx: impl Sink<MessageToRead, Error = impl std::fmt::Debug + Send> + Unpin + Send,
    close_tx: gentle_close::Sender,
) -> std::io::Result<()>
where
    MessageToRead: Send + Debug + Unpin + 'static,
{
    let mut alive_timer = tokio::time::sleep(config.aliveness_timeout);
    let mut ping_counter = 0;

    loop {
        tokio::select! {
            _ = alive_timer => {
                tracing::error!(count = ping_counter, "aliveness timeout, exiting");
                return Err::<(), _>(std::io::Error::new(std::io::ErrorKind::Other, "aliveness timeout"));
            },
            msg = stream_read.recv_msg() => {
                let msg = match msg {
                    Ok(Some(msg)) => msg,
                    Ok(None) => {
                        tracing::error!("unexpected end of stream");
                        return Err::<(), _>(std::io::Error::new(std::io::ErrorKind::Other, "unexpected end of stream"));
                    },
                    Err(err) => match err {
                        DecodeError::Io(err) => return Err(err),
                        DecodeError::InvalidStream(err) => panic!("invalid stream: {:?}", err),
                    }
                };

                tracing::debug!(?msg, "msg from stream");

                match msg {
                    ConnMsg::Protocol(msg) => {
                        if let Err(_) = msg_tx.send(msg).await {
                            tracing::warn!("msg_to_recv_tx is broken, exiting");
                            close_tx.gentle_close();
                            return Ok(());
                        }
                    },
                    ConnMsg::Ping => {
                        tracing::trace!(count=ping_counter, "ping");
                        ping_counter += 1;
                    },
                    ConnMsg::EndOfStream => {
                        close_tx.gentle_close();
                        return Ok(());
                    }
                };

                alive_timer = tokio::time::sleep(config.aliveness_timeout);
            }
        }
    }
}

pub fn run<MessageToSend, MessageToRecv>(
    config: Config,
    stream_read: impl MessageReader<Message = ConnMsg<MessageToRecv>> + Send,
    stream_write: impl MessageWriter<Message = ConnMsg<MessageToSend>> + Send,
    msg_to_recv_tx: impl Sink<MessageToRecv, Error = impl std::fmt::Debug + Send>
    + Unpin
    + Send
    + 'static,
    msg_to_write_sender_tx: impl Sink<
        oneshot::Sender<MessageToSend>,
        Error = impl std::fmt::Debug + Send,
    > + Unpin
    + Send
    + 'static,
) -> (
    impl Future<Output = Result<(), std::io::Error>> + Send,
    gentle_close::Sender,
)
where
    MessageToSend: Send + Debug + Unpin + 'static,
    MessageToRecv: Send + Debug + Unpin + 'static,
{
    let (close_tx, close_rx) = gentle_close::channel();

    let write_loop_task = write_loop(
        config.clone(),
        stream_write,
        msg_to_write_sender_tx,
        close_rx,
    )
    .instrument(tracing::trace_span!("write loop"));

    let read_loop_task = read_loop(config, stream_read, msg_to_recv_tx, close_tx.clone())
        .instrument(tracing::trace_span!("read loop"));

    (
        async move { tokio::try_join! { write_loop_task, read_loop_task }.map(|_| ()) },
        close_tx,
    )
}

mod gentle_close {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, Ordering};

    use tokio::sync::SetOnce;

    pub fn channel() -> (Sender, Receiver) {
        let close_set_once = Arc::new(SetOnce::new());
        (
            Sender {
                close_tx: Arc::clone(&close_set_once),
                sender_count: Arc::new(AtomicU8::new(1)),
            },
            Receiver {
                close_rx: close_set_once,
            },
        )
    }

    pub struct Receiver {
        // Ok: gentle close
        // Err: all senders dropped
        close_rx: Arc<SetOnce<Result<(), ()>>>,
    }

    impl Receiver {
        pub async fn receive(&mut self) -> Result<(), ()> {
            self.close_rx.wait().await.clone()
        }
    }

    pub struct Sender {
        close_tx: Arc<SetOnce<Result<(), ()>>>,
        sender_count: Arc<AtomicU8>,
    }

    impl Sender {
        pub fn gentle_close(self) {
            let _ = self.close_tx.set(Ok(()));
        }
    }

    impl Clone for Sender {
        fn clone(&self) -> Self {
            self.sender_count.fetch_add(1, Ordering::Relaxed);
            Self {
                close_tx: Arc::clone(&self.close_tx),
                sender_count: Arc::clone(&self.sender_count),
            }
        }
    }

    impl Drop for Sender {
        fn drop(&mut self) {
            if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 0 {
                let _ = self.close_tx.set(Err(()));
            }
        }
    }
}

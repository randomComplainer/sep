use std::fmt::Debug;

use futures::prelude::*;
use tracing::Instrument as _;

use crate::prelude::*;
use crate::protocol::MessageReader;
use crate::protocol::MessageWriter;
use crate::protocol::msg::conn::ConnMsg;

pub struct Config {
    io_write_timeout: std::time::Duration,
    ping_interval: std::time::Duration,
    aliveness_timeout: std::time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            io_write_timeout: std::time::Duration::from_secs(5),
            ping_interval: std::time::Duration::from_secs(2),
            aliveness_timeout: std::time::Duration::from_secs(15),
        }
    }
}

pub fn run<MessageToSend, MessageToRecv>(
    config: Config,
    mut stream_read: impl MessageReader<Message = ConnMsg<MessageToRecv>> + Send,
    mut stream_write: impl MessageWriter<Message = ConnMsg<MessageToSend>> + Send,
    mut msg_to_recv_tx: impl Sink<MessageToRecv, Error = impl std::fmt::Debug + Send>
    + Unpin
    + Send
    + 'static,
    sender_tx: impl crate::async_channel_ext::Sender<crate::oneshot_with_ack::RawSender<MessageToSend>>,
) -> (
    impl Future<Output = Result<(), std::io::Error>> + Send,
    tokio::sync::mpsc::Sender<()>,
)
where
    MessageToSend: Send + Debug + Unpin + 'static,
    MessageToRecv: Send + Debug + Unpin + 'static,
{
    let (send_end_of_stream_tx, mut send_end_of_stream_rx) = tokio::sync::mpsc::channel::<()>(2);
    let local_send_end_of_stream_tx = send_end_of_stream_tx.clone();

    let fut = async move {
        let write_loop = async move {
            let mut ping_timer = tokio::time::sleep(config.ping_interval);
            let mut ping_counter = 0;
            let (send_one_tx, send_one_rx) = crate::oneshot_with_ack::channel();
            let mut write_one_rx = Box::pin(send_one_rx);

            if let Err(_) = sender_tx.send(send_one_tx).await {
                return Ok::<_, std::io::Error>(());
            }

            macro_rules! write_msg {
                ($msg:expr) => {
                    let msg:ConnMsg<MessageToSend> = $msg;
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
                    ping_timer  = tokio::time::sleep(config.ping_interval);

                };
            }

            loop {
                tokio::select! {
                    write_one = write_one_rx.as_mut() => {
                        let (msg, acker) = match write_one  {
                            Ok(x) => x,
                            Err(_) => {
                                return Ok::<_, std::io::Error>(());
                            }
                        };

                        write_msg!(msg.into());

                        acker.ack();

                        let (new_wirte_one_tx, new_write_one_rx) = crate::oneshot_with_ack::channel();

                        if let Err(_) = sender_tx.send(new_wirte_one_tx).await {
                            tracing::warn!("sender_tx is broken, exiting");
                            return Ok::<_, std::io::Error>(());
                        }

                        write_one_rx = Box::pin(new_write_one_rx);

                    },
                    _ = ping_timer => {
                        tracing::trace!(count = ping_counter, "ping");
                        write_msg!(ConnMsg::Ping);
                        ping_counter += 1;
                    },
                    Some(_) = send_end_of_stream_rx.recv() => {
                        write_msg!(ConnMsg::EndOfStream);
                        drop(ping_timer);
                        return Ok(());
                    }
                }
            }
        }
        .instrument(tracing::trace_span!("write loop"));

        let read_loop = async move {
            let mut alive_timer = tokio::time::sleep(config.aliveness_timeout);
            let mut ping_counter = 0;

            loop {
                tokio::select! {
                    _ = alive_timer => {
                        tracing::error!(count = ping_counter, "aliveness timeout, exiting");
                        return Err::<(), _>(std::io::Error::new(std::io::ErrorKind::Other, "aliveness timeout"));
                    },
                    msg = stream_read.recv_msg().instrument(tracing::trace_span!("read msg")) => {
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
                                if let Err(_) = msg_to_recv_tx.send(msg).await {
                                    tracing::warn!("msg_to_recv_tx is broken, exiting");
                                    let _ = local_send_end_of_stream_tx.send(()).await;
                                    return Ok(());
                                }
                            },
                            ConnMsg::Ping => {
                                tracing::trace!(count=ping_counter, "ping");
                                ping_counter += 1;
                            },
                            ConnMsg::EndOfStream => {
                                let _ = local_send_end_of_stream_tx.send(()).await;
                                return Ok(());
                            }
                        };

                        alive_timer = tokio::time::sleep(config.aliveness_timeout);
                    }
                }
            }
            }.instrument(tracing::trace_span!("read loop"))
        ;

        tokio::try_join!(write_loop, read_loop).map(|_| ())
    };

    (fut, send_end_of_stream_tx)
}

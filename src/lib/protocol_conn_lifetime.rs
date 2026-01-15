use std::fmt::Debug;

use futures::prelude::*;
use tokio::sync::oneshot;
use tracing::Instrument as _;

use crate::prelude::*;
use crate::protocol::MessageReader;
use crate::protocol::MessageWriter;
use crate::protocol::msg::conn::ConnMsg;

pub struct Config {
    io_write_timeout: std::time::Duration,
    ping_interval: std::time::Duration,
    ping_receive_timeout: std::time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            io_write_timeout: std::time::Duration::from_secs(4),
            ping_interval: std::time::Duration::from_secs(5),
            ping_receive_timeout: std::time::Duration::from_secs(15),
        }
    }
}

pub type SendEntry<MessageToSend> = (MessageToSend, oneshot::Sender<()>);

pub fn run<MessageToSend, MessageToRecv>(
    config: Config,
    mut stream_read: impl MessageReader<Message = ConnMsg<MessageToRecv>> + Send,
    mut stream_write: impl MessageWriter<Message = ConnMsg<MessageToSend>> + Send,
    mut msg_to_recv_tx: impl Sink<MessageToRecv, Error = impl std::fmt::Debug> + Unpin + Send + 'static,
    mut sender_tx: impl Sink<oneshot::Sender<SendEntry<MessageToSend>>, Error = impl std::fmt::Debug>
    + Unpin
    + Send
    + 'static,
) -> (
    impl Future<Output = Result<(), std::io::Error>> + Send,
    tokio::sync::mpsc::Sender<()>,
)
where
    MessageToSend: Send + Debug,
    MessageToRecv: Send + Debug,
{
    let (send_end_of_stream_tx, mut send_end_of_stream_rx) = tokio::sync::mpsc::channel::<()>(2);
    let local_send_end_of_stream_tx = send_end_of_stream_tx.clone();

    let fut = async move {
        // dont cancel me,
        // or you will risk missing message if it's in the middle of being sent
        let send_loop = async move {
            let mut next_ping = tokio::time::Instant::now();
            let mut ping_counter = 0;
            let (send_one_tx, send_one_rx) = oneshot::channel();
            let mut send_one_rx = Box::pin(send_one_rx);

            if let Err(_) = sender_tx.send(send_one_tx).await {
                return Ok::<_, std::io::Error>(());
            }

            macro_rules! send_msg {
            ($msg:expr) => {{
                let msg:ConnMsg<MessageToSend> = $msg;
                let span = tracing::trace_span!("send msg", msg=?msg);
                tokio::time::timeout(config.io_write_timeout, stream_write.send_msg(msg)).map(
                    |timeout_result| match timeout_result {
                        Ok(x) => x,
                        Err(_) => Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "timeout sending msg",
                        )),
                    },
                )
                   .instrument(span)
            }};
        }

            loop {
                // Check ping timer first
                // This is to avoid connection too busy
                // sending messages that ping can't be sent
                let now = tokio::time::Instant::now();
                if now >= next_ping {
                    tracing::trace!(count = ping_counter, "ping");
                    send_msg!(ConnMsg::Ping).await?;
                    ping_counter += 1;
                    next_ping = now + config.ping_interval;
                    continue;
                }

                let ping_timer = tokio::time::sleep_until(next_ping);

                tokio::select! {
                    send_one = send_one_rx.as_mut() => {
                        let (msg, ack_tx) = match send_one  {
                            Ok(send_one) => send_one,
                            Err(_) => {
                                return Ok::<_, std::io::Error>(());
                            }
                        };

                        send_msg!(msg.into()).await?;

                        if let Err(_) = ack_tx.send(()) {
                            return Ok::<_, std::io::Error>(());
                        }

                        let (new_send_one_tx, new_send_one_rx) = oneshot::channel();

                        if let Err(_) = sender_tx.send(new_send_one_tx).await {
                            return Ok::<_, std::io::Error>(());
                        }

                        send_one_rx = Box::pin(new_send_one_rx);
                    },
                    _ = ping_timer => {
                        tracing::trace!(count = ping_counter, "ping");
                        send_msg!(ConnMsg::Ping).await?;
                        ping_counter += 1;
                        next_ping = now + config.ping_interval;
                    },
                    Some(_) = send_end_of_stream_rx.recv() => {
                        tracing::debug!("send end of stream");
                        send_msg!(ConnMsg::EndOfStream).await?;
                        return Ok(());
                    }
                }
            }
        }
        .instrument(tracing::trace_span!("send loop"));

        let recv_loop = async move {
            let mut ping_timer = Box::pin(tokio::time::sleep(config.ping_receive_timeout));
            let mut ping_counter = 0;

            loop {
                tokio::select! {
                    _ = ping_timer.as_mut() => {
                        tracing::warn!(count = ping_counter, "ping timeout, exiting");
                        return Err::<(), _>(std::io::Error::new(std::io::ErrorKind::Other, "ping timeout"));
                    },
                    msg = stream_read.recv_msg().instrument(tracing::trace_span!("receive msg from stream")) => {
                        let msg = match msg {
                            Ok(Some(msg)) => msg,
                            Ok(None) => {
                                tracing::debug!("end of stream, exiting");
                                return Ok(());
                            },
                            Err(err) => match err {
                                DecodeError::Io(err) => return Err(err),
                                DecodeError::InvalidStream(err) => panic!("invalid stream: {:?}", err),
                            }
                        };

                        tracing::debug!(?msg, "message from stream");

                        match msg {
                            ConnMsg::Protocol(msg) => {
                                let span = tracing::trace_span!("forward msg to msg_to_recv_tx", ?msg);
                                if let Err(_) = msg_to_recv_tx.send(msg).instrument(span).await {
                                    tracing::debug!("msg_to_recv_tx is broken, exiting");
                                    return Ok(());
                                }
                            },
                            ConnMsg::Ping => {
                                tracing::trace!(count=ping_counter, "ping");
                                ping_counter += 1;
                                ping_timer = Box::pin(tokio::time::sleep(config.ping_receive_timeout));
                            },
                            ConnMsg::EndOfStream => {
                                tracing::debug!("recv end of stream");
                                let _ = local_send_end_of_stream_tx.send(());
                                return Ok(());
                            }
                        }
                    }
                }
            }
            }.instrument(tracing::trace_span!("recv loop"))
        ;

        tokio::try_join!(send_loop, recv_loop).map(|_| ())
    };

    (fut, send_end_of_stream_tx)
}

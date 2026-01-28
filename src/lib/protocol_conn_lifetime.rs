use std::fmt::Debug;
use std::pin::Pin;

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
            io_write_timeout: std::time::Duration::from_secs(20),
            ping_interval: std::time::Duration::from_secs(2),
            aliveness_timeout: std::time::Duration::from_secs(20),
        }
    }
}

pub struct WriteHandle<Msg> {
    sender_rx: async_channel::Receiver<tokio::sync::oneshot::Sender<Msg>>,
    error_rx: tokio::sync::SetOnce<()>,
}

impl<Msg> Clone for WriteHandle<Msg> {
    fn clone(&self) -> Self {
        Self {
            sender_rx: self.sender_rx.clone(),
            error_rx: self.error_rx.clone(),
        }
    }
}

pub enum NextSender<Msg> {
    Found(ConnId, tokio::sync::oneshot::Sender<Msg>),
    Closed(ConnId),
    IoError(ConnId),
}

impl<Msg> WriteHandle<Msg> {
    pub async fn get_sender(&self) -> Result<Option<tokio::sync::oneshot::Sender<Msg>>, ()> {
        tokio::select! {
            sender = self.sender_rx.recv() => {
                match sender {
                    Ok(sender) => Ok(Some(sender)),
                    Err(_) => {
                        Ok(None)
                    }
                }
            },
            _ = self.error_rx.wait() => {
                Err(())
            }
        }
    }

    pub fn next_sender<'a>(
        write_handles: &'a [(ConnId, WriteHandle<Msg>)],
    ) -> Pin<Box<dyn Future<Output = NextSender<Msg>> + Send + 'a>>
    where
        Msg: Send + Debug + Unpin + 'static,
    {
        use NextSender::*;

        if write_handles.is_empty() {
            return Box::pin(std::future::pending());
        }
        Box::pin(
            futures::future::select_all(write_handles.iter().map(|(conn_id, write_handle)| {
                Box::pin(write_handle.get_sender().map(|write_handle_result| {
                    match write_handle_result {
                        Ok(Some(sender)) => Found(*conn_id, sender),
                        Ok(None) => Closed(*conn_id),
                        Err(_) => IoError(*conn_id),
                    }
                }))
            }))
            .map(|x| x.0),
        )
    }

    // Err = Conn IO Error
    pub async fn loop_through(
        mut msg_queue: impl Stream<Item = Msg> + Unpin,
        mut conn_writer_rx: impl Stream<Item = (ConnId, WriteHandle<Msg>)> + Unpin,
    ) -> Result<(), ConnId>
    where
        Msg: Send + Debug + Unpin + 'static,
    {
        use NextSender::*;

        let mut handles: Vec<(ConnId, WriteHandle<Msg>)> = Vec::new();

        let mut head_msg = None::<Msg>;

        loop {
            let msg = match head_msg.take() {
                Some(msg) => msg,
                None => match msg_queue.next().await {
                    Some(msg) => msg,
                    None => {
                        tracing::debug!("end of messages");
                        return Ok(());
                    }
                },
            };

            tracing::trace!(?msg, handles_count = handles.len(), "dispatch msg");

            tokio::select! {
                next_sender_result = Self::next_sender(&handles)  => {
                    let (conn_id, sender ) = match next_sender_result {
                        Found(conn_id, sender) => (conn_id, sender),
                        Closed(conn_id) => {
                            let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                            handles.swap_remove(idx);
                            continue;
                        },
                        IoError(conn_id) => return Err(conn_id),
                    };

                    tracing::trace!(?conn_id, ?msg, "send msg to conn");
                    match sender.send(msg) {
                        Ok(_) => continue,
                        Err(msg) => {
                            // Closed
                            tracing::trace!(?conn_id, "conn closed, retry sending msg to another conn");
                            head_msg = Some(msg);
                            let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                            handles.swap_remove(idx);
                            continue;
                        }
                    }
                },

                conn = conn_writer_rx.next() => {
                    let (conn_id, write_handle) = match conn {
                        Some(conn) => conn,
                        None => {
                            tracing::warn!("end of conn_writer_rx, exiting");
                            return Ok(());
                        }
                    };

                    head_msg = Some(msg);
                    handles.push((conn_id, write_handle));
                    tracing::trace!(?conn_id, "aquired conn");
                    continue;
                },
            }
        }
    }
}

#[derive(Clone)]
pub struct GentleCloseHandle {
    tx: tokio::sync::SetOnce<()>,
}

impl GentleCloseHandle {
    pub fn new() -> Self {
        Self {
            tx: tokio::sync::SetOnce::new(),
        }
    }

    pub async fn graceful_close(&self) {
        let _ = self.tx.set(());
    }

    pub fn reciver(&self) -> tokio::sync::SetOnce<()> {
        self.tx.clone()
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
) -> (
    impl Future<Output = Result<(), std::io::Error>> + Send,
    WriteHandle<MessageToSend>,
    GentleCloseHandle,
)
where
    MessageToSend: Send + Debug + Unpin + 'static,
    MessageToRecv: Send + Debug + Unpin + 'static,
{
    let gentle_close_handle = GentleCloseHandle::new();
    let (sender_tx, sender_rx) =
        async_channel::unbounded::<tokio::sync::oneshot::Sender<MessageToSend>>();
    let io_error_notifier = tokio::sync::SetOnce::new();

    let gentle_close_rx = gentle_close_handle.reciver();
    let local_gentle_close_handle = gentle_close_handle.clone();

    let write_loop = async move {
        let mut ping_timer = Box::pin(tokio::time::sleep(config.ping_interval));
        let mut ping_counter = 0;
        let (send_one_tx, send_one_rx) = tokio::sync::oneshot::channel();
        let mut send_one_rx = Box::pin(send_one_rx);

        if let Err(_) = sender_tx.send(send_one_tx).await {
            tracing::warn!("all writehandle dropped");
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
                    ping_timer  = Box::pin(tokio::time::sleep(config.ping_interval));

                };
            }

        loop {
            tokio::select! {
                write_one = send_one_rx.as_mut() => {
                    let msg = match write_one  {
                        Ok(x) => x,
                        Err(_) => {
                            let (send_one_tx, new_send_one_rx) = tokio::sync::oneshot::channel();
                            send_one_rx = Box::pin(new_send_one_rx);
                            if let Err(_) = sender_tx.send(send_one_tx).await {
                                tracing::warn!("all writehandle dropped");
                                return Ok::<_, std::io::Error>(());
                            }
                            continue;
                        }
                    };

                    write_msg!(msg.into());

                    let (new_wirte_one_tx, new_write_one_rx) = tokio::sync::oneshot::channel();

                    if let Err(_) = sender_tx.send(new_wirte_one_tx).await {
                        tracing::warn!("sender_tx is broken, exiting");
                        return Ok::<_, std::io::Error>(());
                    }

                    send_one_rx = Box::pin(new_write_one_rx);

                },
                _ = ping_timer.as_mut() => {
                    tracing::trace!(count = ping_counter, "ping");
                    write_msg!(ConnMsg::Ping);
                    ping_counter += 1;
                },
                _ = gentle_close_rx.wait() => {
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
                                let _ = local_gentle_close_handle.graceful_close();
                                return Ok(());
                            }
                        },
                        ConnMsg::Ping => {
                            tracing::trace!(count=ping_counter, "ping");
                            ping_counter += 1;
                        },
                        ConnMsg::EndOfStream => {
                            let _ = local_gentle_close_handle.graceful_close();
                            return Ok(());
                        }
                    };

                    alive_timer = tokio::time::sleep(config.aliveness_timeout);
                }
            }
        }
    }.instrument(tracing::trace_span!("read loop"));

    let io_error_sender = io_error_notifier.clone();
    let fut = async move {
        tokio::try_join!(write_loop, read_loop)
            .inspect_err(|_| {
                let _ = io_error_sender.set(());
            })
            .map(|_| ())
    };

    (
        fut,
        WriteHandle {
            sender_rx,
            error_rx: io_error_notifier,
        },
        gentle_close_handle,
    )
}

use futures::prelude::*;
use tracing::*;

use crate::handover;
use crate::prelude::*;

pub trait ServerConnector
where
    Self: Clone + Sync + Send + Unpin + 'static,
{
    type GreetedWrite: protocol::client_agent::GreetedWrite;
    type GreetedRead: protocol::client_agent::GreetedRead;

    type Fut: std::future::Future<
            Output = Result<(Box<str>, Self::GreetedRead, Self::GreetedWrite), std::io::Error>,
        > + Send
        + Unpin
        + 'static;

    fn connect(&self) -> Self::Fut;
}

impl<TFn, TGreetedRead, TGreetedWrite, TFuture> ServerConnector for TFn
where
    TFn: (Fn() -> TFuture) + Clone + Sync + Send + Unpin + 'static,
    TGreetedRead: protocol::client_agent::GreetedRead,
    TGreetedWrite: protocol::client_agent::GreetedWrite,
    TFuture: std::future::Future<
            Output = Result<(Box<str>, TGreetedRead, TGreetedWrite), std::io::Error>,
        > + Send
        + Unpin
        + 'static,
{
    type GreetedWrite = TGreetedWrite;
    type GreetedRead = TGreetedRead;
    type Fut = TFuture;
    fn connect(&self) -> Self::Fut {
        self()
    }
}

pub fn run(
    mut server_read: impl protocol::client_agent::GreetedRead,
    mut server_write: impl protocol::client_agent::GreetedWrite,
    mut client_msg_rx: handover::Receiver<protocol::msg::ClientMsg>,
    mut server_msg_tx: impl Sink<(u16, session::msg::ServerMsg), Error = impl std::fmt::Debug>
    + Unpin
    + Send
    + 'static,
) -> impl Future<Output = Result<(), std::io::Error>> + Send {
    async move {
        let (end_of_server_stream_tx, end_of_server_stream_rx) =
            tokio::sync::oneshot::channel::<()>();

        let send_loop = async move {
            let mut end_of_server_stream_rx = Box::pin(end_of_server_stream_rx);
            let mut ping_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(30)));
            let mut ping_counter = 0;

            loop {
                tokio::select! {
                    client_msg = client_msg_rx.recv()
                        .instrument(debug_span!("receive client msg to send"))
                        => {
                        let client_msg = match client_msg {
                            Some(client_msg) => client_msg,
                            None => {
                                return Ok::<_, std::io::Error>(());
                            }
                        };

                        let span = debug_span!("send client msg to server", ?client_msg);
                        server_write.send_msg(client_msg.into()).instrument(span).await?;
                    },
                    _ = ping_timer.as_mut() => {
                        debug!(count = ping_counter, "ping");

                        server_write.send_msg(protocol::msg::conn::ClientMsg::Ping)
                            .instrument(debug_span!("send ping", count = ping_counter))
                            .await?;

                        ping_counter += 1;
                        ping_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(30)));
                    },
                    _ = &mut end_of_server_stream_rx => {
                        debug!("end of server messages notified");
                        return Ok::<_, std::io::Error>(());
                    }
                }
            }
        }
        .instrument(debug_span!("send loop"));

        let (pong_tx, mut pong_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let pong_waiting_loop = async move {
            let mut pong_counter = 0;
            let mut pong_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(60)));
            loop {
                tokio::select! {
                    _ = pong_timer.as_mut() => {
                        warn!("pong timeout, exiting");
                        return Err::<(), _>(std::io::Error::new(std::io::ErrorKind::Other, "pong timeout"));
                    },
                    pong = pong_rx.recv() => {
                        match pong {
                            Some(_) => {
                                debug!(count=pong_counter, "pong");
                                pong_counter += 1;
                                pong_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(60)));
                            }
                            None => {
                                debug!("pong rx is broken, exiting");
                                return Ok::<_, std::io::Error>(());
                            }
                        }
                    }
                }
            }
        }.instrument(info_span!("pong waiting loop"));

        let recv_loop = async move {
            while let Some(server_msg) = match server_read
                .recv_msg()
                .instrument(debug_span!("receive server msg"))
                .await
            {
                Ok(server_msg) => server_msg,
                Err(e) => match e {
                    DecodeError::Io(err) => return Err(err),
                    DecodeError::InvalidStream(err) => panic!("invalid stream: {:?}", err),
                },
            } {
                debug!("message from server: {:?}", &server_msg);

                use protocol::msg::conn::ServerMsg::*;

                match server_msg {
                    Protocol(server_msg) => match server_msg {
                        protocol::msg::ServerMsg::SessionMsg(proxyee_id, server_msg) => {
                            let span = debug_span!(
                                "forward server msg to session",
                                session_id = proxyee_id,
                                ?server_msg
                            );

                            if let Err(_) = server_msg_tx
                                .send((proxyee_id, server_msg))
                                .instrument(span)
                                .await
                            {
                                warn!("failed to send server msg to session, exiting");
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "failed to forward server msg",
                                ));
                            }
                        }
                    },
                    EndOfStream => {
                        debug!("end of server messages");
                        let _ = end_of_server_stream_tx.send(());
                        return Ok::<_, std::io::Error>(());
                    }
                    Pong => {
                        if let Err(_) = pong_tx.send(()) {
                            warn!("pong tx is broken, exiting");
                            return Ok(());
                        }
                    }
                };
            }

            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected end of server read stream",
            ))
        }
        .instrument(debug_span!("recv loop"));

        tokio::try_join! {
             send_loop,
             recv_loop,
             pong_waiting_loop,
        }
        .map(|_| ())
    }
}

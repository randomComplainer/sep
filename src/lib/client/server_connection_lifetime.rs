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
            Output = Result<(protocol::ConnId, Self::GreetedRead, Self::GreetedWrite), std::io::Error>,
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
            Output = Result<(protocol::ConnId, TGreetedRead, TGreetedWrite), std::io::Error>,
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
        let end_of_stream_notifier= tokio::sync::SetOnce::<()>::new();

        // dont cancel me, 
        // or you will risk missing message if it's in the middle of being sent
        let send_loop = {
            let end_of_stream_notifier = end_of_stream_notifier.clone();
            async move {
                let mut ping_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(0)));
                let mut ping_counter = 0;

                macro_rules! send_msg {
                    ($msg:expr) => {
                        tokio::time::timeout(
                            std::time::Duration::from_secs(4),
                            server_write.send_msg($msg)
                        )
                            .map(|timeout_result|  
                                match timeout_result {
                                    Ok(x) => x,
                                    Err(_) => Err(std::io::Error::new(
                                            std::io::ErrorKind::TimedOut,
                                            "timeout sending msg to server",
                                    )
                                    )
                                }
                            )
                    }
                }

                loop {
                    // TODO: extract ping loop out of this loop
                    // to make sure client message get recived asap
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
                                send_msg!(client_msg.into()).instrument(span).await?;
                            },
                            _ = ping_timer.as_mut() => {
                                debug!(count = ping_counter, "ping");

                                send_msg!(protocol::msg::conn::ClientMsg::Ping)
                                    .instrument(debug_span!("send ping", count = ping_counter))
                                    .await?;

                                ping_counter += 1;
                                ping_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(5)));
                            },
                            _ = end_of_stream_notifier.wait() => {
                                debug!("end of server messages notified");
                                return Ok::<_, std::io::Error>(());
                            }
                    }
                }
        }}
        .instrument(debug_span!("send loop"));

        let recv_loop = async move {
            let mut pong_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(10)));
            let mut pong_counter = 0;

            loop {
                tokio::select! {
                    _ = pong_timer.as_mut() => {
                        warn!("pong timeout, exiting");
                        return Err::<(), _>(std::io::Error::new(std::io::ErrorKind::Other, "pong timeout"));
                    },
                    msg = server_read.recv_msg() => {
                        let server_msg = match msg {
                            Err(err) => match err {
                                DecodeError::Io(err) => return Err(err),
                                DecodeError::InvalidStream(err) => panic!("invalid stream: {:?}", err),
                            },
                            Ok(Some(msg)) => msg,
                            Ok(None) => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "unexpected end of server read stream",
                                ));
                            }
                        };

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
                                let _ = end_of_stream_notifier.set(());
                                return Ok::<_, std::io::Error>(());
                            }
                            Pong => {
                                debug!(count=pong_counter, "pong");
                                pong_counter += 1;
                                pong_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(10)));
                            }
                        };
                    }
                }   
            }
        }
        .instrument(debug_span!("recv loop"));

        match futures::future::select(Box::pin(send_loop), Box::pin(recv_loop) ).await {
            future::Either::Left((send_result, _) ) => send_result,

            // do not cancel send_loop
            future::Either::Right((recv_result, send_loop) ) => {
                let send_result = send_loop.await;
                match recv_result {
                    Ok(_) => send_result,
                    Err(recv_err) => Err(recv_err),
                }
            },
        }
    }
}

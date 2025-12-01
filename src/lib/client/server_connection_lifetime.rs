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
            Output = Result<(Self::GreetedRead, Self::GreetedWrite), std::io::Error>,
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
    TFuture: std::future::Future<Output = Result<(TGreetedRead, TGreetedWrite), std::io::Error>>
        + Send
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
        let send_loop = async move {
            while let Some(client_msg) = client_msg_rx
                .recv()
                .instrument(debug_span!("receive client msg to send"))
                .await
            {
                let span = debug_span!("send client msg to server", ?client_msg);
                server_write.send_msg(client_msg).instrument(span).await?;
            }

            Ok::<_, std::io::Error>(())
        }
        .instrument(debug_span!("send loop"));

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

                match server_msg {
                    protocol::msg::ServerMsg::SessionMsg(proxyee_id, server_msg) => {
                        let span = debug_span!(
                            "forward server msg to session",
                            session_id = proxyee_id,
                            ?server_msg
                        );
                        server_msg_tx
                            .send((proxyee_id, server_msg))
                            .instrument(span)
                            .await
                            .unwrap();
                    }
                    protocol::msg::ServerMsg::EndOfStream => {
                        debug!("end of server messages");
                        return Ok::<_, std::io::Error>(());
                    }
                };
            }
            panic!("server_read stream is broken");
        }
        .instrument(debug_span!("recv loop"));

        tokio::select! {
            x = send_loop => x,
            x = recv_loop => x,
        }
    }
}

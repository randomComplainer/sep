use chacha20::cipher::StreamCipher;
use futures::prelude::*;
use tracing::*;

use crate::handover;
use crate::prelude::*;

pub trait ServerConnector
where
    Self: Clone + Sync + Send + Unpin + 'static,
{
    type ServerStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static;
    type Cipher: StreamCipher + Unpin + Send + 'static;
    type Fut: std::future::Future<
            Output = Result<
                (
                    protocol::client_agent::GreetedWrite<Self::ServerStream, Self::Cipher>,
                    protocol::client_agent::GreetedRead<Self::ServerStream, Self::Cipher>,
                ),
                std::io::Error,
            >,
        > + Send
        + Unpin
        + 'static;

    fn connect(&self) -> Self::Fut;
}

impl<TFn, TServerStream, TCipher, TFuture> ServerConnector for TFn
where
    TFn: (Fn() -> TFuture) + Clone + Sync + Send + Unpin + 'static,
    TServerStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    TCipher: StreamCipher + Unpin + Send + 'static,
    TFuture: std::future::Future<
            Output = Result<
                (
                    protocol::client_agent::GreetedWrite<TServerStream, TCipher>,
                    protocol::client_agent::GreetedRead<TServerStream, TCipher>,
                ),
                std::io::Error,
            >,
        > + Send
        + Unpin
        + 'static,
{
    type ServerStream = TServerStream;
    type Cipher = TCipher;
    type Fut = TFuture;
    fn connect(&self) -> Self::Fut {
        self()
    }
}

pub fn run<TConnectServer>(
    connect_to_server: TConnectServer,
    mut client_msg_rx: handover::Receiver<protocol::msg::ClientMsg>,
    mut server_msg_tx: futures::channel::mpsc::Sender<protocol::msg::ServerMsg>,
) -> impl Future<Output = Result<(), std::io::Error>> + Send
where
    TConnectServer: ServerConnector,
{
    async move {
        let (mut server_write, mut server_read) = connect_to_server.connect().await?;
        let (close_notify_tx, mut close_notify_rx) = futures::channel::oneshot::channel::<()>();

        let reciving_msg_from_server = async move {
            while let Some(msg) = server_read.recv_msg().await.unwrap() {
                debug!("message from server: {:?}", &msg);
                server_msg_tx.send(msg).await.unwrap();
            }

            debug!("end of server messages");

            // server_read closed => stop sennding msg to server
            // it's fine if write stream is already closed
            let _ = close_notify_tx.send(());

            Ok::<_, std::io::Error>(())
        };

        let sending_msg_to_server = async move {
            loop {
                match futures::future::select(close_notify_rx, client_msg_rx.recv().boxed()).await {
                    futures::future::Either::Left(x) => {
                        drop(x);
                        // TODO: error handling
                        server_write.close().await.unwrap();

                        return Ok::<_, std::io::Error>(());
                    }
                    futures::future::Either::Right((client_msg_opt, not_yet_closed)) => {
                        if let Some(client_msg) = client_msg_opt {
                            close_notify_rx = not_yet_closed;
                            debug!("message to server: {:?}", &client_msg);
                            // TODO: error handling
                            server_write.send_msg(client_msg).await.unwrap();
                        } else {
                            return Ok::<_, std::io::Error>(Default::default());
                        }
                    }
                }
            }
        };

        tokio::try_join! {
            sending_msg_to_server,
            reciving_msg_from_server,
        }
        .map(|_| ())
    }
}

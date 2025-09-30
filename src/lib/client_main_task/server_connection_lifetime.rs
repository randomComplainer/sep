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
    mut server_msg_tx: futures::channel::mpsc::Sender<(u16, session::msg::ServerMsg)>,
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
                match msg {
                    protocol::msg::ServerMsg::SessionMsg(proxyee_id, server_msg) => {
                        server_msg_tx.send((proxyee_id, server_msg)).await.unwrap();
                    }
                    protocol::msg::ServerMsg::EndOfStream => {
                        debug!("end of server messages");
                        let _ = close_notify_tx.send(());
                        return Ok::<_, std::io::Error>(());
                    }
                };
            }

            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected end of server messages"));
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

#[cfg(test)]
mod tests {
    use rand::RngCore as _;
    use std::sync::{Arc, Mutex};

    use super::*;

    // verify that client connection lifetime task can end gracefully
    // when server closes connection
    // might not need to test this in the future
    #[tokio::test]
    async fn server_close_connection() {
        let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);
        let key: Arc<protocol::Key> = protocol::key_from_string("key").into();

        let mut client_stream = Arc::new(Mutex::new(Some(client_stream)));

        let mut client_id: Box<[u8; 16]> = [0u8; 16].into();
        rand::rng().fill_bytes(client_id.as_mut());
        let client_id: Arc<[u8; 16]> = client_id.into();

        let timestamp = protocol::get_timestamp();

        let (client_msg_tx, mut client_msg_rx) = handover::channel();
        let (server_msg_tx, mut server_msg_rx) = futures::channel::mpsc::channel(4);

        let server_agent = protocol::server_agent::Init::new(key.clone(), server_stream);

        let server_conn = move || {
            let key = key.clone();
            let client_stream = client_stream.clone();
            let client_id = client_id.clone();

            Box::pin(async move {
                let client_stream = client_stream.lock().unwrap().take().unwrap();

                let client_agent = protocol::client_agent::Init::new(
                    client_id,
                    1234,
                    key.clone(),
                    protocol::rand_nonce(),
                    client_stream,
                );

                client_agent.send_greeting(timestamp).await
            })
        };

        let client_task = run(server_conn, client_msg_rx, server_msg_tx);
        let client_task_handle = tokio::spawn(client_task);

        let (_client_id, _client_read, mut client_write) =
            server_agent.recv_greeting(timestamp).await.unwrap();

        client_write
            .send_msg(protocol::msg::ServerMsg::EndOfStream)
            .await
            .unwrap();

        assert_eq!(client_task_handle.await.unwrap().unwrap(), ());

        drop(client_write);
    }
}

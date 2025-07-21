use std::sync::Arc;

use chacha20::cipher::StreamCipher;
use dashmap::DashMap;
use futures::prelude::*;

use crate::prelude::*;

pub async fn run<ProxyeeStream, ServerStream, Cipher, ConnectServerFut>(
    mut new_proxee_rx: impl Stream<Item = (u16, socks5::agent::Init<ProxyeeStream>)> + Unpin,
    connect_to_server: impl Fn() -> ConnectServerFut + Unpin,
    max_server_conn: usize,
) -> Result<(), std::io::Error>
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    ServerStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    Cipher: StreamCipher + Unpin + Send + 'static,
    ConnectServerFut: std::future::Future<
            Output = Result<
                (
                    protocol::client_agent::GreetedWrite<ServerStream, Cipher>,
                    protocol::client_agent::GreetedRead<ServerStream, Cipher>,
                ),
                std::io::Error,
            >,
        > + Unpin,
{
    let (client_msg_tx, mut client_msg_rx) =
        futures::channel::mpsc::channel::<(u16, session::msg::ClientMsg)>(8);

    let (server_write_tx, mut server_write_rx) = futures::channel::mpsc::channel::<
        protocol::client_agent::GreetedWrite<ServerStream, Cipher>,
    >(16);

    let session_server_msg_senders = Arc::new(DashMap::<
        u16,
        futures::channel::mpsc::Sender<session::msg::ServerMsg>,
    >::new());

    let accepting_proxyee = {
        let client_msg_tx = client_msg_tx.clone();
        let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
        async move {
            while let Some((session_id, proxyee)) = new_proxee_rx.next().await {
                let (session_server_msg_tx, session_server_msg_rx) =
                    futures::channel::mpsc::channel(4);
                let session_client_msg_tx = client_msg_tx
                    .clone()
                    .with(move |msg| std::future::ready(Ok((session_id, msg))));
                session_server_msg_senders.insert(session_id, session_server_msg_tx);

                let session_task =
                    session::client::run(proxyee, session_server_msg_rx, session_client_msg_tx);

                tokio::spawn({
                    let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
                    async move {
                        match session_task.await {
                            Ok(_) => {}
                            Err(err) => {
                                dbg!(format!("session task failed: {:?}", err));
                            }
                        }

                        session_server_msg_senders.remove(&session_id);
                    }
                });
            }

            Ok::<_, std::io::Error>(())
        }
    };

    let sending_msg_to_server = {
        let mut server_write_tx = server_write_tx.clone();
        let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
        async move {
            let mut server_conn_count = 0;

            while let Some((session_id, client_msg)) = client_msg_rx.next().await {
                let mut server_write = {
                    match server_write_rx.try_next() {
                        Ok(Some(server_write)) => server_write,
                        _ => {
                            if server_conn_count < max_server_conn {
                                let session_server_msg_senders =
                                    Arc::clone(&session_server_msg_senders);
                                let (server_write, mut server_read) = connect_to_server().await?;

                                let reciving_msg_from_server = {
                                    async move {
                                        // TODO: proper error handling
                                        while let Some(agent_msg) =
                                            server_read.recv_msg().await.unwrap()
                                        {
                                            dbg!(format!("message from server: {:?}", &agent_msg));
                                            let (session_id, session_msg): (
                                                u16,
                                                session::msg::ServerMsg,
                                            ) = match agent_msg {
                                                protocol::msg::ServerMsg::SessionMsg(
                                                    session_id,
                                                    session_msg,
                                                ) => (session_id, session_msg),
                                            };

                                            if let Some(mut session_server_msg_sender) =
                                                session_server_msg_senders.get_mut(&session_id)
                                            {
                                                let _ = session_server_msg_sender
                                                    .send(session_msg)
                                                    .await;
                                            }
                                        }

                                        Ok::<_, std::io::Error>(())
                                    }
                                };

                                tokio::spawn(async move {
                                    match reciving_msg_from_server.await {
                                        Ok(_) => {}
                                        Err(err) => {
                                            dbg!(err);
                                        }
                                    }
                                });

                                server_write_tx.send(server_write).await.unwrap();
                                server_conn_count += 1;
                            }
                            server_write_rx.next().await.unwrap()
                        }
                    }
                };

                dbg!(format!("message to server: {:?}", &client_msg));

                tokio::spawn({
                    let mut server_write_tx = server_write_tx.clone();

                    async move {
                        server_write
                            .send_msg(client_msg.with_session_id(session_id))
                            .await
                            .unwrap();
                        // TODO: do I care about error?
                        let _ = server_write_tx.send(server_write).await;
                    }
                });
            }

            Ok::<_, std::io::Error>(())
        }
    };

    tokio::try_join! {
        accepting_proxyee,
        sending_msg_to_server,
    }
    .map(|_| ())
}

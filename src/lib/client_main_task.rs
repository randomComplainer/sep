use std::sync::{Arc, atomic::AtomicUsize};

use chacha20::cipher::StreamCipher;
use dashmap::DashMap;
use futures::prelude::*;
use thiserror::Error;

use crate::handover;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("session protocol error: session id {0}: {1}")]
    SessionProtocol(u16, String),
    #[error("lost connection to server")]
    LostConnection,
}

// TODO: error handling
// for io/messging errors, need to close entire connection group, since stream is dirty
// returns remaining client messages when server connection is closed
async fn server_connection_lifetime_task<ServerStream, Cipher, ConnectServerFut>(
    connect_to_server: impl Fn() -> ConnectServerFut + Unpin,
    mut client_msg_rx: handover::Receiver<protocol::msg::ClientMsg>,
    mut server_msg_tx: futures::channel::mpsc::Sender<protocol::msg::ServerMsg>,
) -> Result<(), std::io::Error>
where
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
    let (mut server_write, mut server_read) = connect_to_server().await?;
    let (close_notify_tx, mut close_notify_rx) = futures::channel::oneshot::channel::<()>();

    let reciving_msg_from_server = async move {
        while let Some(msg) = server_read.recv_msg().await.unwrap() {
            server_msg_tx.send(msg).await.unwrap();
        }

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

//TODO: Error types
// Server Io Error for retry
// Protpcol Error for exit
pub async fn run<ProxyeeStream, ServerStream, Cipher, ConnectServerFut>(
    mut new_proxee_rx: impl Stream<Item = (u16, socks5::agent::Init<ProxyeeStream>)> + Unpin + Send,
    connect_to_server: impl (Fn() -> ConnectServerFut) + Clone + Send + Sync + Unpin + 'static,
    max_server_conn: usize,
) -> Result<(), ClientError>
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
        > + Send
        + Unpin,
{
    let (client_msg_tx, mut client_msg_rx) =
        futures::channel::mpsc::channel::<protocol::msg::ClientMsg>(max_server_conn);

    let (server_write_tx, mut server_write_rx) = futures::channel::mpsc::channel::<
        handover::Sender<protocol::msg::ClientMsg>,
    >(max_server_conn);

    let (server_msg_tx, mut server_msg_rx) =
        futures::channel::mpsc::channel::<protocol::msg::ServerMsg>(max_server_conn);

    let session_server_msg_senders = Arc::new(DashMap::<
        u16,
        futures::channel::mpsc::Sender<session::msg::ServerMsg>,
    >::new());

    let server_conn_count = Arc::new(AtomicUsize::new(0));

    let (err_tx, mut err_rx) = futures::channel::mpsc::channel::<ClientError>(1);

    let accepting_proxyee = {
        let client_msg_tx = client_msg_tx.clone();
        let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
        let err_tx = err_tx.clone();
        async move {
            while let Some((session_id, proxyee)) = new_proxee_rx.next().await {
                let (session_server_msg_tx, session_server_msg_rx) =
                    futures::channel::mpsc::channel(4);
                let session_client_msg_tx = client_msg_tx.clone().with(move |msg| {
                    std::future::ready(Ok(protocol::msg::ClientMsg::SessionMsg(session_id, msg)))
                });
                session_server_msg_senders.insert(session_id, session_server_msg_tx);

                let session_task =
                    session::client::run(proxyee, session_server_msg_rx, session_client_msg_tx);

                tokio::spawn({
                    let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
                    let mut err_tx = err_tx.clone();
                    async move {
                        match session_task.await {
                            Ok(_) => {}
                            Err(err) => {
                                dbg!(format!("session task failed: {:?}", err));
                                if let session::client::ClientSessionError::Protocol(err) = err {
                                    let _ = err_tx
                                        .send(ClientError::SessionProtocol(session_id, err))
                                        .await;
                                }
                            }
                        }

                        session_server_msg_senders.remove(&session_id);
                    }
                });
            }

            Ok::<_, ClientError>(())
        }
    };

    let register_new_server_conn = {
        let server_msg_tx = server_msg_tx.clone();
        let server_conn_count = Arc::clone(&server_conn_count);

        async move || {
            let server_msg_tx = server_msg_tx.clone();
            let server_conn_count = Arc::clone(&server_conn_count);
            server_conn_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

            let (conn_client_msg_tx, conn_client_msg_rx) = handover::channel();
            let connect_to_server = connect_to_server.clone();

            tokio::spawn(async move {
                // TODO: error handling
                match server_connection_lifetime_task(
                    connect_to_server,
                    conn_client_msg_rx,
                    server_msg_tx,
                )
                .await
                {
                    Ok(_) => {
                        dbg!(format!("server connection lifetime task ended"));
                    }
                    Err(err) => {
                        dbg!(format!("server connection lifetime task failed: {:?}", err));
                    }
                }

                server_conn_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            });

            Ok::<_, ClientError>(conn_client_msg_tx)
        }
    };

    let sending_msg_to_server = {
        let mut server_write_tx = server_write_tx.clone();
        async move {
            while let Some(client_msg) = client_msg_rx.next().await {
                let mut server_write = {
                    match server_write_rx.try_next() {
                        Ok(Some(server_write)) => server_write,
                        _ => {
                            if server_conn_count.load(std::sync::atomic::Ordering::Acquire)
                                < max_server_conn
                            {
                                // TODO: error handling
                                // maybe don't await?
                                let conn_client_msg_tx = register_new_server_conn().await?;
                                server_write_tx.send(conn_client_msg_tx).await.unwrap();
                            }
                            server_write_rx.next().await.unwrap()
                        }
                    }
                };

                dbg!(format!("message to server: {:?}", &client_msg));

                tokio::spawn({
                    let mut server_write_tx = server_write_tx.clone();
                    let mut client_msg_tx = client_msg_tx.clone();

                    async move {
                        // if connection server write is closed,
                        // don't queue it back
                        match server_write.send(client_msg).await {
                            Ok(_) => {
                                let _ = server_write_tx.send(server_write).await;
                            }
                            Err(server_msg) => {
                                // TODO: Err?
                                client_msg_tx.send(server_msg).await.unwrap();
                            }
                        };
                    }
                });
            }

            Ok::<_, ClientError>(())
        }
    };

    let receiving_msg_from_server = {
        let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
        async move {
            while let Some(msg) = server_msg_rx.next().await {
                dbg!(format!("message from server: {:?}", &msg));
                match msg {
                    protocol::msg::ServerMsg::SessionMsg(session_id, server_msg) => {
                        if let Some(mut session_server_msg_sender) =
                            session_server_msg_senders.get_mut(&session_id)
                        {
                            // sessiion could be ended, dont care about error
                            let _ = session_server_msg_sender.send(server_msg).await;
                        }
                    }
                };
            }

            Ok::<_, ClientError>(())
        }
    };

    let checking_err = async move { err_rx.next().await.unwrap() };

    let looping_task = async move {
        tokio::try_join! {
            accepting_proxyee,
            sending_msg_to_server,
            receiving_msg_from_server,
        }
        .map(|_| ())
    };

    match futures::future::select(checking_err.boxed(), looping_task.boxed()).await {
        future::Either::Left((err, _)) => Err(err),
        future::Either::Right((r, _)) => r,
    }
}

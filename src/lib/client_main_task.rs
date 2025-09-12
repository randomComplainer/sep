use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use futures::prelude::*;
use thiserror::Error;
use tracing::*;

use crate::handover;
use crate::prelude::*;

mod server_connection_lifetime;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("session protocol error: session id {0}: {1}")]
    SessionProtocol(u16, String),
    #[error("lost connection to server")]
    LostServerConnection,
}

#[derive(Debug)]
struct State {
    session_count: usize,
    server_conn_count: usize,
}

// Exits only on server connection io error
// protocol error panics
pub async fn run<ProxyeeStream, ServerConnector>(
    mut new_proxee_rx: impl Stream<Item = (u16, socks5::agent::Init<ProxyeeStream>)>
    + Unpin
    + Send
    + 'static,
    connect_to_server: ServerConnector,
    max_server_conn: usize,
) -> std::io::Error
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    ServerConnector: server_connection_lifetime::ServerConnector,
{
    let (client_msg_tx, mut client_msg_rx) =
        futures::channel::mpsc::channel::<protocol::msg::ClientMsg>(max_server_conn);

    let (mut scope_handle, scope_task) = task_scope::new_scope::<std::io::Error>();

    let (server_msg_tx, mut server_msg_rx) =
        futures::channel::mpsc::channel::<(u16, session::msg::ServerMsg)>(max_server_conn);

    let (server_write_tx, mut server_write_rx) = futures::channel::mpsc::channel::<
        handover::Sender<protocol::msg::ClientMsg>,
    >(max_server_conn);

    let session_server_msg_senders = Arc::new(DashMap::<
        u16,
        futures::channel::mpsc::Sender<session::msg::ServerMsg>,
    >::new());

    let (state_tx, state_rx) = tokio::sync::watch::channel(State {
        session_count: 0,
        server_conn_count: 0,
    });

    let register_new_server_conn = {
        let server_msg_tx = server_msg_tx.clone();
        let scope_handle = scope_handle.clone();
        let state_tx = state_tx.clone();

        async move || {
            let server_msg_tx = server_msg_tx.clone();
            let mut scope_handle = scope_handle.clone();

            let (conn_client_msg_tx, conn_client_msg_rx) = handover::channel();

            scope_handle
                .run_async(
                    async move {
                        state_tx.send_modify(|state| {
                            state.server_conn_count += 1;
                        });

                        // TODO: error handling
                        let result = server_connection_lifetime::run(
                            connect_to_server,
                            conn_client_msg_rx,
                            server_msg_tx,
                        )
                        .await;

                        state_tx.send_modify(|state| {
                            state.server_conn_count -= 1;
                        });

                        match result {
                            Ok(_) => {
                                debug!("server connection lifetime task ended");
                                Ok(())
                            }
                            Err(err) => {
                                debug!("server connection lifetime task failed: {:?}", err);
                                Err(err)
                            }
                        }
                    }
                    // TODO: record connection identifier in the span
                    .instrument(info_span!("server connection")),
                )
                .await
                .unwrap();

            Ok::<_, std::io::Error>(conn_client_msg_tx)
        }
    };

    // make sure there are at least one server conn
    // if there are any session sill running
    scope_handle
        .run_async({
            let mut state_rx = state_rx.clone();
            let register_new_server_conn = register_new_server_conn.clone();
            let mut server_write_tx = server_write_tx.clone();
            async move {
                loop {
                    let lock = state_rx
                        .wait_for(|state| state.server_conn_count == 0 && state.session_count > 0)
                        .await;
                    drop(lock);
                    debug!("register new server conn for running session");

                    let server_write = (register_new_server_conn.clone())().await?;
                    server_write_tx
                        .send(server_write)
                        .await
                        .expect("server_write_tx is broken");
                }
            }
        })
        .await
        .unwrap();

    // accepting proxyee
    scope_handle
        .run_async({
            let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
            let mut scope_handle = scope_handle.clone();
            let state_tx = state_tx.clone();
            let client_msg_tx = client_msg_tx.clone();

            async move {
                while let Some((session_id, proxyee)) = new_proxee_rx.next().await {
                    let socks5_span = info_span!(
                        "session",
                        socks5_port = session_id,
                        start_time = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                    );

                    let (session_server_msg_tx, session_server_msg_rx) =
                        futures::channel::mpsc::channel(4);

                    let session_client_msg_tx = client_msg_tx.clone().with_sync(move |msg| {
                        protocol::msg::ClientMsg::SessionMsg(session_id, msg)
                    });

                    session_server_msg_senders.insert(session_id, session_server_msg_tx);

                    scope_handle
                        .run_async({
                            let state_tx = state_tx.clone();
                            let session_server_msg_senders =
                                Arc::clone(&session_server_msg_senders);

                            async move {
                                state_tx.send_modify(|state| {
                                    state.session_count += 1;
                                });

                                let session_result = session::client::run(
                                    proxyee,
                                    session_server_msg_rx,
                                    session_client_msg_tx,
                                )
                                .await;

                                session_server_msg_senders.remove(&session_id);

                                state_tx.send_modify(|state| {
                                    state.session_count -= 1;
                                });

                                match session_result {
                                    Ok(_) => Ok(()),
                                    Err(err) => {
                                        error!("session task failed: {:?}", err);
                                        Ok(())
                                    }
                                }
                            }
                            .instrument(socks5_span)
                        })
                        .await
                        .unwrap();
                }
                Ok(())
            }
        })
        .await
        .unwrap();

    // sending msg to server
    scope_handle
        .run_async({
            let server_write_tx = server_write_tx.clone();
            let mut scope_handle = scope_handle.clone();
            let state_rx = state_rx.clone();

            async move {
                while let Some(client_msg) = client_msg_rx.next().await {
                    debug!("find server conn write to send msg");

                    let mut server_write = {
                        match server_write_rx.try_next() {
                            Ok(Some(server_write)) => {
                                debug!("idle server conn write is available");
                                server_write
                            }
                            Ok(None) => {
                                panic!("server_write_rx is broken");
                            }
                            Err(_) => {
                                debug!("all server write is busy");
                                if state_rx.borrow().server_conn_count < max_server_conn {
                                    debug!("register new server conn");
                                    (register_new_server_conn.clone())().await?
                                } else {
                                    debug!("max server conn reached, wait for an idle server conn");
                                    server_write_rx
                                        .next()
                                        .await
                                        .expect("server_write_rx is broken")
                                }
                            }
                        }
                    };
                    debug!("server conn write is ready to send msg");

                    scope_handle
                        .run_async({
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

                                Ok(())
                            }
                        })
                        .await
                        .unwrap();
                }

                Ok(())
            }
        })
        .await
        .unwrap();

    // receiving msg from server
    scope_handle
        .run_async({
            let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
            async move {
                while let Some((session_id, msg)) = server_msg_rx.next().await {
                    if let Some(mut session_server_msg_sender) =
                        session_server_msg_senders.get_mut(&session_id)
                    {
                        // sessiion could be ended, dont care about error
                        let _ = session_server_msg_sender.send(msg).await;
                    };
                }

                Ok(())
            }
        })
        .await
        .unwrap();

    // log state
    scope_handle
        .run_async({
            let mut state_rx = state_rx.clone();
            async move {
                loop {
                    state_rx.changed().await.unwrap();
                    debug!("state changed: {:?}", *state_rx.borrow());
                }
            }
        })
        .await
        .unwrap();

    scope_task.await
}

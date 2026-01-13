use std::borrow::Borrow;

use futures::prelude::*;
use tracing::*;

use crate::handover;
use crate::message_dispatch;
use crate::prelude::*;

#[derive(Debug)]
pub enum Command {
    SendClientMsg((u16, session::msg::ClientMsg)),
    SessionStarted(u16),
    SessionEnded(u16),
}

pub enum Event {
    ServerMsg((u16, session::msg::ServerMsg)),
}

pub struct Config {
    pub max_server_conn: u16,
}

#[derive(Debug)]
struct State {
    pub session_count: usize,
    pub server_conn_count: usize,
}

pub async fn run<ServerConnector>(
    connect_to_server: ServerConnector,
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Clone + Unpin + Send + 'static,
    config: Config,
) -> std::io::Error
where
    ServerConnector: super::server_connection_lifetime::ServerConnector + Send,
{
    let (connection_scope_handle, connection_scope_task) =
        task_scope::new_scope::<std::io::Error>();

    let (state_tx, state_rx) = tokio::sync::watch::channel(State {
        session_count: 0,
        server_conn_count: 0,
    });

    let (mut client_msg_dispatch_cmd_tx, client_msg_dispatch_task) = message_dispatch::run();

    // increase conn count sycnhronously
    // actual connecting is done asynchronously
    // so it will not block you too long on await
    let create_connection = {
        let connect_to_server = connect_to_server.clone();
        let mut client_msg_dispatch_cmd_tx = client_msg_dispatch_cmd_tx.clone();
        let mut connecting_scope_handle = connection_scope_handle.clone();
        let evt_tx = evt_tx.clone();
        let state_tx = state_tx.clone();
        async move || {
            state_tx.send_modify(|old| {
                old.server_conn_count += 1;
            });
            connecting_scope_handle
                .run_async(async move {
                    let (conn_client_msg_tx, conn_client_msg_rx) = handover::channel();

                    debug!("connecting to server");
                    let (conn_id, server_read, server_write) = connect_to_server.connect().await?;
                    let lifetime_span = info_span!("server connection lifetime", ?conn_id);
                    debug!("connected to server");

                    client_msg_dispatch_cmd_tx
                        .send(message_dispatch::Command::Sender(
                            conn_id,
                            conn_client_msg_tx,
                        ))
                        .await
                        .expect("conn_client_msg_tx is broken");

                    let conn_result = super::server_connection_lifetime::run(
                        server_read,
                        server_write,
                        conn_client_msg_rx,
                        evt_tx
                            .clone()
                            .with_sync(move |client_msg| Event::ServerMsg(client_msg)),
                    )
                    .instrument(lifetime_span)
                    .await;

                    state_tx.send_modify(|old| {
                        old.server_conn_count -= 1;
                    });

                    if let Err(err) = conn_result {
                        warn!(?err, "server connection lifetime task failed");
                    }

                    Ok::<_, std::io::Error>(())
                })
                .await;
        }
    };

    let keep_alive_loop = {
        let create_connection = create_connection.clone();
        let mut state_rx = state_rx.clone();
        let max_server_conn = config.max_server_conn;
        async move {
            while let Ok(x) = state_rx
                .wait_for(|state| {
                    let expected = std::cmp::min(max_server_conn as usize, state.session_count * 2);
                    state.server_conn_count < expected
                })
                .await
            {
                debug!(state = ?&*x, "creating more server connections");
                drop(x);
                create_connection.clone()().await;
            }
        }
    };

    let cmd_loop = async move {
        while let Some(cmd) = cmd_rx.next().instrument(debug_span!("receive cmd")).await {
            debug!(?cmd, "new cmd");
            match cmd {
                Command::SendClientMsg(client_msg) => {
                    if let Err(_) = client_msg_dispatch_cmd_tx
                        .send(message_dispatch::Command::Msg(
                            protocol::msg::ClientMsg::SessionMsg(client_msg.0, client_msg.1),
                        ))
                        .instrument(debug_span!("forward client msg for dispatch"))
                        .await
                    {
                        warn!("client_msg_dispatch_cmd_tx is broken, exiting");
                        return;
                    }
                }
                Command::SessionStarted(_) => {
                    state_tx.send_modify(|state| {
                        state.session_count += 1;
                    });
                }
                Command::SessionEnded(_) => {
                    state_tx.send_modify(|state| {
                        state.session_count -= 1;
                    });
                }
            }
        }

        debug!("end of cmd stream, exiting");
    };

    tokio::try_join! {
        connection_scope_task
            .map(|e| Err::<(), _>(e))
            .instrument(info_span!("connection scope task")),
        client_msg_dispatch_task
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("client msg dispatch task")),
        keep_alive_loop
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("keep_alive_loop")),
        cmd_loop
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("cmd_loop")),
    }
    .unwrap_err()
}

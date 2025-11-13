use std::collections::VecDeque;
use std::sync::Arc;

use futures::prelude::*;
use tokio::sync::Mutex;
use tracing::*;

use crate::prelude::*;
use crate::handover;

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

struct State {
    pub session_count: usize,
    pub server_conn_count: usize,
}

pub async fn run<ServerConnector>(
    connect_to_server: ServerConnector,
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    evt_tx: impl Sink<Event, Error= impl std::fmt::Debug> + Clone + Unpin + Send + 'static,
    config: Config,
) -> std::io::Error  where
ServerConnector: super::server_connection_lifetime::ServerConnector + Send,{
    let (server_write_queue_tx, mut server_write_queue_rx) = futures::channel::mpsc::channel::<
        handover::Sender<protocol::msg::ClientMsg>>(config.max_server_conn as usize);

    let (connection_scope_handle, connection_scope_task) = task_scope::new_scope::<std::io::Error>();
    let (state_tx, state_rx) = tokio::sync::watch::channel(State{
        session_count: 0,
        server_conn_count: 0,
    });

    let (mut client_msg_queue_tx, mut client_msg_queue_rx) = 
        futures::channel::mpsc::channel::<protocol::msg::ClientMsg>(config.max_server_conn as usize);

    // increase conn count sycnhronously
    let create_connection = { 
        let connect_to_server = connect_to_server.clone();
        let mut connecting_scope_handle = connection_scope_handle.clone();
        let evt_tx = evt_tx.clone();
        let mut server_write_queue_tx = server_write_queue_tx.clone();
        let state_tx = state_tx.clone();
        async move || {
            state_tx.send_modify(|old| {
                old.server_conn_count += 1;
            });
            connecting_scope_handle.run_async(async move {
                let (conn_client_msg_tx, conn_client_msg_rx) = handover::channel();

                debug!("connecting to server");
                let (server_write, server_read) = connect_to_server.connect().await?;
                debug!("connected to server");

                server_write_queue_tx.send(conn_client_msg_tx).await.expect("server_write_queue_tx is broken");

                super::server_connection_lifetime::run(
                    server_write,
                    server_read,
                    conn_client_msg_rx,
                    evt_tx.clone().with_sync(move |client_msg| Event::ServerMsg(client_msg))
                )
                    .inspect(|_| state_tx.send_modify(|old| {
                        old.server_conn_count -= 1;
                    }))
                    .await?;

                Ok::<_, std::io::Error>(())
            }).await;
        } 
    };

    let (mut client_msg_sending_worker_scope_handle, client_msg_sending_worker_scope_task) = 
            task_scope::new_scope::<()>();
    let client_msg_dispatch_loop = {
        let create_connection = create_connection.clone();
        let mut state_rx = state_rx.clone();
        let server_write_queue_tx = server_write_queue_tx.clone();
        async move {
            let client_msg_retry_queue = VecDeque::<protocol::msg::ClientMsg>::with_capacity(
                config.max_server_conn as usize,
            );
            let client_msg_retry_queue = Arc::new(Mutex::new(client_msg_retry_queue));

            while let Some(msg) = {
                match client_msg_retry_queue.lock().await.pop_front() {
                    Some(x) => Some(x),
                    None => client_msg_queue_rx.next().await,
                }
            } {
                debug!("aquiring server write");
                let mut server_write = {
                    if let Some(server_write) = server_write_queue_rx.next().poll_once().await {
                        debug!("found idle server write");
                        server_write.unwrap()
                    } else {
                        debug!("no idle server write");
                        tokio::select! {
                            server_write = server_write_queue_rx.next() => {
                                debug!("found idle server write");
                                server_write.unwrap()
                            },
                            lock = state_rx.wait_for(|state| state.server_conn_count < config.max_server_conn as usize).boxed() => {
                                drop(lock);
                                debug!("creating new servere connection");
                                create_connection.clone()().await;
                                server_write_queue_rx
                                    .next().await.unwrap()
                            },
                        }
                    }
                };
                debug!("aquired server write");

                client_msg_sending_worker_scope_handle.run_async( { 
                    let mut server_write_queue_tx = server_write_queue_tx.clone();
                    let client_msg_retry_queue = client_msg_retry_queue.clone();
                    async move{
                        match server_write.send(msg).await {
                            Ok(_) => {
                                server_write_queue_tx.send(server_write)
                                    .instrument(debug_span!("sucessfully send msg to server, queue server write back"))
                                    .await
                                    .unwrap();
                                Ok(())
                                },
                            Err(msg) => {
                                debug!("server_write is closed, retry sending client message");
                                client_msg_retry_queue.lock().await.push_back(msg);
                                Ok(())
                            },
                        }
                    } })
                .await; 
            }

            debug!("end of client_msg stream, exiting");
        }
    };

    let keep_alive_loop = {
        let create_connection = create_connection.clone();
        let mut state_rx = state_rx.clone();
        async move {
            while let Ok(x) = state_rx.wait_for(|state| state.session_count > 0 && state.server_conn_count == 0).await 
            { 
                drop(x);
                debug!("no server conn present, create new one for running session");
                create_connection.clone()().await; 
            }
        }
    };

    let cmd_loop = async move {
        while let Some(cmd) = cmd_rx.next().await {
            match cmd {
                Command::SendClientMsg(client_msg) => {
                    client_msg_queue_tx
                        .send(protocol::msg::ClientMsg::SessionMsg(client_msg.0, client_msg.1))
                        .await.unwrap();
                    },
                Command::SessionStarted(_) => {
                    state_tx.send_modify(|state| {
                        state.session_count += 1;
                    });
                },
                Command::SessionEnded(_) => {
                    state_tx.send_modify(|state| {
                        state.session_count -= 1;
                    });
                },
            }
        }

        debug!("end of cmd stream, exiting");
    };

    tokio::try_join!{
        connection_scope_task
            .map(|e| Err::<(), _>(e))
            .instrument(info_span!("connection scope task")),
        client_msg_sending_worker_scope_task
            .instrument(info_span!("client msg sending worker scope task"))
            .map(|x| Ok::<_, std::io::Error>(x)),
        client_msg_dispatch_loop
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("client msg dispatch loop")),
        keep_alive_loop
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("keep_alive_loop")),
        cmd_loop
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("cmd_loop")),
    }
    .unwrap_err()
}


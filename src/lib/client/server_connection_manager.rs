use futures::prelude::*;
use tracing::*;

use crate::handover;
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
    // There will be active connections
    // and any connection that is closed by server but not yet removed from the queue
    // (they will be removed when we try to send a message to them)
    // so keep the queue unbounded for now
    let (server_write_queue_tx, mut server_write_queue_rx) = futures::channel::mpsc::unbounded::<(
        Box<str>,
        handover::Sender<protocol::msg::ClientMsg>,
    )>();

    let (connection_scope_handle, connection_scope_task) =
        task_scope::new_scope::<std::io::Error>();

    let (state_tx, state_rx) = tokio::sync::watch::channel(State {
        session_count: 0,
        server_conn_count: 0,
    });

    let (mut client_msg_queue_tx, mut client_msg_queue_rx) =
        futures::channel::mpsc::unbounded::<protocol::msg::ClientMsg>();

    // increase conn count sycnhronously
    // actual connecting is done asynchronously
    // so it will not block you too long on wait
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
            connecting_scope_handle
                .run_async(async move {
                    let (conn_client_msg_tx, conn_client_msg_rx) = handover::channel();

                    debug!("connecting to server");
                    let (conn_id, server_read, server_write) = connect_to_server.connect().await?;
                    let lifetime_span = info_span!(
                        "server connection lifetime",
                        conn_id = ?conn_id
                    );
                    debug!("connected to server");

                    server_write_queue_tx
                        .send((conn_id, conn_client_msg_tx))
                        .await
                        .expect("server_write_queue_tx is broken");

                    super::server_connection_lifetime::run(
                        server_read,
                        server_write,
                        conn_client_msg_rx,
                        evt_tx
                            .clone()
                            .with_sync(move |client_msg| Event::ServerMsg(client_msg)),
                    )
                    .inspect(|_| {
                        state_tx.send_modify(|old| {
                            old.server_conn_count -= 1;
                        })
                    })
                    .instrument_with_result(lifetime_span)
                    .await?;

                    Ok::<_, std::io::Error>(())
                })
                .await;
        }
    };

    let client_msg_dispatch_loop = {
        let create_connection = create_connection.clone();
        let state_rx = state_rx.clone();
        let mut server_write_queue_tx = server_write_queue_tx.clone();
        async move {
            while let Some(msg) = client_msg_queue_rx
                .next()
                .instrument(debug_span!("receive next client msg to forward"))
                .await
            {
                debug!(?msg, "new client msg to forward");

                let mut msg = msg;

                loop {
                    let aquire_server_write = {
                        let server_write_queue_rx = &mut server_write_queue_rx;
                        let create_connection = create_connection.clone();
                        let mut state_rx = state_rx.clone();
                        async move {
                            tokio::select! {
                                server_write = server_write_queue_rx.next() => {
                                    debug!("found idle server write");

                                    server_write
                                },
                                lock = state_rx.wait_for(|state| state.server_conn_count < config.max_server_conn as usize) => {
                                    drop(lock);
                                    debug!("creating new servere connection");
                                    create_connection().await;
                                    server_write_queue_rx.next().await
                                },
                            }
                        }
                        .instrument(debug_span!("aquire server write"))
                    };

                    let mut server_write = match aquire_server_write.await {
                        Some(x) => x,
                        None => {
                            warn!("server_write_queue_rx is broken, exiting");
                            return;
                        }
                    };

                    match server_write.1
                        .send(msg)
                        .instrument(debug_span!("forward client msg to connection", conn_id = ?server_write.0))
                        .await
                    {
                        Ok(_) => {
                            // it's just a channel, it's fine to wait it
                            if let Err(_) = server_write_queue_tx
                                .send(server_write)
                                .instrument(debug_span!(
                                    "sucessfully send msg to server, queue server write back"
                                ))
                                .await
                            {
                                warn!("server_write_queue_tx is broken, exiting");
                                return;
                            }
                            break;
                        }
                        Err(msg_opt) => {
                            debug!("server_write is closed");
                            msg = msg_opt.expect("client msg consumed unexpectedly");
                            debug!("retry sending client message");
                            continue;
                        }
                    }
                }
            }

            debug!("end of client_msg stream, exiting");
        }
    };

    let keep_alive_loop = {
        let create_connection = create_connection.clone();
        let mut state_rx = state_rx.clone();
        async move {
            while let Ok(x) = state_rx
                .wait_for(|state| state.session_count > 0 && state.server_conn_count == 0)
                .await
            {
                drop(x);
                debug!("no server conn present, create new one for running session");
                create_connection.clone()().await;
            }
        }
    };

    let cmd_loop = async move {
        while let Some(cmd) = cmd_rx.next().instrument(debug_span!("receive cmd")).await {
            debug!(?cmd, "new cmd");
            match cmd {
                Command::SendClientMsg(client_msg) => {
                    client_msg_queue_tx
                        .send(protocol::msg::ClientMsg::SessionMsg(
                            client_msg.0,
                            client_msg.1,
                        ))
                        .instrument(debug_span!("forward client msg to queue"))
                        .await
                        .unwrap();
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

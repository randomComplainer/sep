use std::ops::Add as _;

use futures::channel::mpsc;
use futures::prelude::*;
use rand::Rng as _;
use tracing::*;

use crate::handover;
use crate::message_dispatch;
use crate::prelude::*;
use protocol::SessionId;

#[derive(Debug)]
pub enum Command {
    ServerMsg(protocol::msg::ServerMsg),
    NewSession(SessionId),
    EndSession(SessionId),
}

#[derive(Debug)]
pub enum Event {
    ClientMsg(protocol::msg::ClientMsg),
    Started,
    Ended,
}

pub async fn run<GreetedRead, GreetedWrite>(
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Clone + Unpin + Send + 'static,
    mut new_conn_rx: handover::Receiver<(protocol::ConnId, GreetedRead, GreetedWrite)>,
) -> Result<(), std::io::Error>
where
    GreetedRead:
        protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>,
    GreetedWrite:
        protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>,
{
    let (mut conn_scope_handle, conn_scope_task) = task_scope::new_scope::<std::io::Error>();

    let (mut server_msg_dispatch_cmd_tx, server_msg_dispatch_cmd_rx) = mpsc::unbounded();
    let server_msg_dispatch_task = message_dispatch::run::<
        protocol::ConnId,
        protocol::msg::ServerMsg,
    >(server_msg_dispatch_cmd_rx);

    let accepting_new_conn = {
        let evt_tx = evt_tx.clone();
        let server_msg_dispatch_cmd_tx = server_msg_dispatch_cmd_tx.clone();
        async move {
            while let Some((conn_id, client_read, client_write)) = new_conn_rx.recv().await {
                debug!("new client connection incoming");

                let conn_lifetime_span = info_span!("conn lifetime", ?conn_id);

                let mut evt_tx = evt_tx.clone();
                conn_scope_handle
                    .run_async(
                        {
                            let server_msg_dispatch_cmd_tx = server_msg_dispatch_cmd_tx.clone();
                            async move {
                                if let Err(_) = evt_tx.send(Event::Started).await {
                                    warn!("evt_tx is broken, exiting");
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        "evt_tx is broken",
                                    ));
                                }

                                let (conn_task, conn_end_of_stream_tx) =
                                    crate::protocol_conn_lifetime::run(
                                        Default::default(),
                                        client_read,
                                        client_write,
                                        evt_tx.clone().with_sync(
                                            |msg: protocol::msg::ClientMsg| Event::ClientMsg(msg),
                                        ),
                                        server_msg_dispatch_cmd_tx.clone().with_sync(
                                            move |send_one| {
                                                message_dispatch::Command::Sender(conn_id, send_one)
                                            },
                                        ),
                                    );

                                tokio::spawn(async move {
                                    let duration = std::time::Duration::from_secs(10).add(
                                        std::time::Duration::from_mins(
                                            rand::rng().random_range(..=10u64),
                                        ),
                                    );

                                    // let duration = std::time::Duration::from_secs(10);

                                    tokio::time::sleep(duration).await;
                                    let _ = conn_end_of_stream_tx.send(()).await;
                                });

                                let conn_result = conn_task.await;

                                if let Err(_) = evt_tx.send(Event::Ended).await {
                                    warn!("evt_tx is broken, exiting");
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        "evt_tx is broken",
                                    ));
                                }

                                if let Err(err) = conn_result {
                                    error!(?err, "client connection lifetime task failed");
                                    return Err(err);
                                }

                                Ok::<_, std::io::Error>(())
                            }
                        }
                        .instrument(conn_lifetime_span),
                    )
                    .await;
            }
        }
    };

    // aka. forwarding server msg to client connections
    let receiving_cmd = {
        async move {
            while let Some(cmd) = cmd_rx.next().await {
                match cmd {
                    Command::ServerMsg(server_msg) => {
                        if let Err(_) = server_msg_dispatch_cmd_tx
                            .send(message_dispatch::Command::Msg(
                                match &server_msg {
                                    protocol::msg::ServerMsg::SessionMsg(session_id, _) => {
                                        *session_id
                                    }
                                },
                                server_msg,
                            ))
                            .await
                        {
                            warn!("server_msg_dispatch_cmd_tx is broken, exiting");
                            return;
                        }
                    }
                    Command::NewSession(session_id) => {
                        if let Err(_) = server_msg_dispatch_cmd_tx
                            .send(message_dispatch::Command::NewSession(session_id))
                            .await
                        {
                            warn!("server_msg_dispatch_cmd_tx is broken, exiting");
                            return;
                        }
                    }
                    Command::EndSession(session_id) => {
                        if let Err(_) = server_msg_dispatch_cmd_tx
                            .send(message_dispatch::Command::EndSession(session_id))
                            .await
                        {
                            warn!("server_msg_dispatch_cmd_tx is broken, exiting");
                            return;
                        }
                    }
                }
            }
        }
    }
    .instrument(info_span!("receiving cmd"));

    tokio::select! {
        _ = accepting_new_conn => Ok(()),
        _ = receiving_cmd => Ok(()),
        _ = server_msg_dispatch_task.instrument(info_span!("server msg dispatch task")) => Ok(()),
        e = conn_scope_task.instrument(info_span!("conn scope task")) => Err(e),
    }
}

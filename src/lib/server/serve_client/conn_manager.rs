use futures::channel::mpsc;
use futures::prelude::*;
use tracing::*;

use super::client_conn_lifetime;
use crate::handover;
use crate::prelude::*;

#[derive(Debug)]
pub enum Command {
    ServerMsg(protocol::msg::ServerMsg),
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
    mut new_conn_rx: handover::Receiver<(Box<str>, GreetedRead, GreetedWrite)>,
) -> Result<(), std::io::Error>
where
    GreetedRead: protocol::server_agent::GreetedRead,
    GreetedWrite: protocol::server_agent::GreetedWrite,
{
    let (mut conn_scope_handle, conn_scope_task) = task_scope::new_scope::<std::io::Error>();

    let (mut client_write_queue_tx, mut client_write_queue_rx) =
        mpsc::unbounded::<(Box<str>, handover::Sender<protocol::msg::ServerMsg>)>();

    let accepting_new_conn = {
        let evt_tx = evt_tx.clone();
        let mut client_write_queue_tx = client_write_queue_tx.clone();
        async move {
            while let Some((conn_id, client_read, client_write)) = new_conn_rx.recv().await {
                debug!("new client connection incoming");
                let (conn_server_msg_tx, conn_server_msg_rx) =
                    handover::channel::<protocol::msg::ServerMsg>();

                let conn_lifetime_span = info_span!("conn lifetime", conn_id = conn_id.as_ref());

                if let Err(_) = client_write_queue_tx
                    .send((conn_id, conn_server_msg_tx))
                    .await
                {
                    warn!("client write queue is broken, exiting");
                    return;
                }

                let mut evt_tx = evt_tx.clone();
                conn_scope_handle
                    .run_async(
                        async move {
                            if let Err(_) = evt_tx.send(Event::Started).await {
                                warn!("evt_tx is broken, exiting");
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "evt_tx is broken",
                                ));
                            }

                            let r =
                                client_conn_lifetime::run(
                                    client_read,
                                    client_write,
                                    evt_tx.clone().with_sync(
                                        move |msg: protocol::msg::ClientMsg| Event::ClientMsg(msg),
                                    ),
                                    conn_server_msg_rx,
                                )
                                .instrument(info_span!("client connection lifetime task"))
                                .await;

                            if let Err(_) = evt_tx.send(Event::Ended).await {
                                warn!("evt_tx is broken, exiting");
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "evt_tx is broken",
                                ));
                            }

                            if let Err(err) = r {
                                error!(?err, "client connection lifetime task failed");
                                return Err(err);
                            }

                            Ok::<_, std::io::Error>(())
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
                        let mut server_msg = server_msg;
                        loop {
                            let (conn_id, mut client_write) =
                                match client_write_queue_rx.next().await {
                                    Some(x) => x,
                                    None => {
                                        warn!("client write queue is broken, exiting");
                                        return;
                                    }
                                };

                            if let Err(msg_not_sent_opt) = client_write
                                .send(server_msg)
                                .instrument(info_span!(
                                    "forward server msg to connection",
                                    conn_id = conn_id.as_ref()
                                ))
                                .await
                            {
                                warn!("client write is broken, dropping client write");
                                match msg_not_sent_opt {
                                    Some(msg_not_sent) => {
                                        server_msg = msg_not_sent;
                                        continue;
                                    }
                                    None => {
                                        error!(conn_id = conn_id.as_ref(), "server message lost");
                                        panic!("server message lost");
                                    }
                                }
                            } else {
                                // queue the sender back
                                if let Err(_) =
                                    client_write_queue_tx.send((conn_id, client_write)).await
                                {
                                    warn!("client write queue is broken, exiting");
                                    return;
                                }
                                break;
                            }
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
        e = conn_scope_task.instrument(info_span!("conn scope task")) => Err(e),
    }
}

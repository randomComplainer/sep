use std::collections::HashMap;

use futures::channel::mpsc;
use futures::prelude::*;
use tracing::*;

use super::serve_client;
use crate::handover;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
}

impl Into<serve_client::Config<crate::connect_target::ConnectTargetImpl>> for Config {
    fn into(self) -> serve_client::Config<crate::connect_target::ConnectTargetImpl> {
        serve_client::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
            connect_target: crate::connect_target::ConnectTargetImpl(
                crate::connect_target::cache::Cache::new(
                    crate::connect_target::cache::EvictQueue::new(
                        std::time::Duration::from_secs(60),
                        5,
                        tokio::time::Instant::now(),
                        64,
                    ),
                    Box::new(|domain, port| {
                        Box::pin(async move {
                            tokio::net::lookup_host((domain, port))
                                .await
                                .map(|addrs| addrs.collect::<Vec<_>>())
                                .map_err(|err| {
                                    tracing::warn!(?err, "failed to resolve domain");
                                    ()
                                })
                        })
                    }),
                ),
            ),
        }
    }
}

pub async fn run<GreetedRead, GreetedWrite>(
    mut new_conn_rx: impl Stream<Item = (Box<ClientId>, Box<str>, GreetedRead, GreetedWrite)> + Unpin,
    config: Config,
) -> Result<(), std::io::Error>
where
    GreetedRead: protocol::server_agent::GreetedRead,
    GreetedWrite: protocol::server_agent::GreetedWrite,
{
    let mut conn_senders = HashMap::<
        Box<protocol::ClientId>,
        handover::Sender<(Box<str>, GreetedRead, GreetedWrite)>,
    >::new();

    let (worker_ending_tx, mut worker_ending_rx) = mpsc::unbounded::<(
        Box<protocol::ClientId>,
        handover::ChannelRef<(Box<str>, GreetedRead, GreetedWrite)>,
    )>();

    loop {
        match futures::future::select(new_conn_rx.next(), worker_ending_rx.next()).await {
            future::Either::Left((conn, _)) => {
                // it should never end
                let (client_id, conn_id, client_read, client_write) = conn.unwrap();
                let conn = (conn_id, client_read, client_write);

                if let Err(conn) = {
                    // try to send to existing worker first
                    match conn_senders.get_mut(&client_id) {
                        Some(existing_sender) => match existing_sender.send(conn).await {
                            Ok(_) => Ok(()),
                            Err(conn_opt) => {
                                conn_senders.remove(&client_id);
                                match conn_opt {
                                    Some(conn) => Err(conn),
                                    None => Ok(()),
                                }
                            }
                        },
                        None => Err::<(), (Box<str>, GreetedRead, GreetedWrite)>(conn),
                    }
                } {
                    // create new worker

                    let (mut worker_conn_tx, worker_conn_rx) =
                        handover::channel::<(Box<str>, GreetedRead, GreetedWrite)>();

                    let channel_ref = worker_conn_tx.create_channel_ref();
                    let worker = serve_client::run(worker_conn_rx, config.clone().into());

                    tokio::spawn({
                        let client_id = client_id.clone();
                        let mut worker_ending_tx = worker_ending_tx.clone();
                        async move {
                            // don't care about result, it's a io error
                            let _ = worker.await;

                            let _ = worker_ending_tx.send((client_id, channel_ref)).await;
                        }
                        // TODO: record worker identifier in the span
                        .instrument(info_span!("worker task"))
                    });

                    if let Err(_) = worker_conn_tx.send(conn).await {
                        // TODO: temporary, queue it back or something
                        ()
                    } else {
                        conn_senders.insert(client_id, worker_conn_tx);
                    }
                };
            }
            future::Either::Right((ended, _)) => {
                let (client_id, _receiver) = ended.unwrap();

                // TODO: check if the sender&receiver are bind to the same channel
                if let Some(_sender) = conn_senders.get(&client_id) {
                    conn_senders.remove(&client_id);
                }
            }
        };
    }
}

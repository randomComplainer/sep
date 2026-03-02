use std::collections::HashMap;

use futures::channel::mpsc;
use futures::prelude::*;
use tracing::*;

use super::serve_client;
use crate::prelude::*;
use crate::protocol::ConnId;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_size: u16,
    pub max_bytes_ahead_per_conn: u32,
    pub max_conn_per_session: u8,
}

// impl Into<serve_client::Config<crate::connect_target::ConnectTargetDirect>> for Config {
// fn into(self) -> serve_client::Config<crate::connect_target::ConnectTargetDirect> {
impl Into<serve_client::Config<crate::connect_target::ConnectTargetWithDnsCache>> for Config {
    fn into(self) -> serve_client::Config<crate::connect_target::ConnectTargetWithDnsCache> {
        serve_client::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead_per_conn: self.max_bytes_ahead_per_conn,
            max_conn_per_session: self.max_conn_per_session,
            // connect_target: crate::connect_target::ConnectTargetDirect,
            connect_target: crate::connect_target::ConnectTargetWithDnsCache(
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
    mut new_conn_rx: impl Stream<Item = (Box<ClientId>, ConnId, GreetedRead, GreetedWrite)> + Unpin,
    config: Config,
) -> Result<(), std::io::Error>
where
    GreetedRead:
        protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>,
    GreetedWrite:
        protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>,
{
    let mut client_entries = HashMap::<
        Box<ClientId>,
        tokio::sync::mpsc::UnboundedSender<(ConnId, GreetedRead, GreetedWrite)>,
    >::new();

    let (client_end_tx, mut client_end_rx) = mpsc::unbounded::<(
        Box<ClientId>,
        tokio::sync::mpsc::UnboundedSender<(ConnId, GreetedRead, GreetedWrite)>,
    )>();

    let mut client_senders = HashMap::<
        Box<ClientId>,
        tokio::sync::mpsc::UnboundedSender<(ConnId, GreetedRead, GreetedWrite)>,
    >::new();

    loop {
        tokio::select! {
            new_conn_opt = new_conn_rx.next() => {
                let (client_id, conn_id, client_read, client_write) = match new_conn_opt {
                    Some(x) => x,
                    None => {
                        tracing::warn!("nex_conn_rx is broken, existing");
                        return Ok(());
                    },
                };

                if let Err(conn) = {
                    let conn = (conn_id, client_read, client_write);
                    match client_entries.get_mut(&client_id) {
                        None => Err(conn),
                        Some(sender) => {
                            sender.send(conn).map_err(|err| err.0)
                        },
                    }
                } {
                    // TODO: include ClientId in log
                    let span = info_span!("client serving");

                    let (client_sender, client_reciver) = tokio::sync::mpsc::unbounded_channel();
                    client_sender.send(conn).unwrap();
                    let client_inner_fut = span.in_scope(||
                        serve_client::run(client_reciver, config.clone().into())
                    );

                    let client_worker_fut = {
                        let client_id = client_id.clone();
                        let mut client_end_tx = client_end_tx.clone();
                        let client_sender = client_sender.clone();
                        async move {
                            // don't care about result, it's a io error
                            let _ = client_inner_fut.await;

                            let _ = client_end_tx.send((client_id,  client_sender)).await;
                        }
                        .instrument(span)
                    };

                    client_entries.insert(client_id, client_sender);
                    tokio::spawn(client_worker_fut);
                }
            },

            client = client_end_rx.next() => {
                use std::collections::hash_map::Entry::*;

                let (client_id, client_sender) = client.unwrap();
                if let Occupied(occupied) =  client_senders.entry(client_id) {
                    if client_sender.same_channel(occupied.get()) {
                        occupied.remove_entry();
                    }
                };
            },
        };
    }
}

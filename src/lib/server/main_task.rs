use std::collections::HashMap;

use chacha20::cipher::StreamCipher;
use futures::channel::mpsc;
use futures::prelude::*;
use tracing::*;

use super::connection_group;
use crate::handover;
use crate::prelude::*;

type Greeted<Stream, Cipher> = (
    protocol::server_agent::GreetedRead<Stream, Cipher>,
    protocol::server_agent::GreetedWrite<Stream, Cipher>,
);

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<connection_group::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> connection_group::Config<TConnectTarget> {
        connection_group::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
            connect_target: self.connect_target,
        }
    }
}

pub async fn run<ClientStream, Cipher, TConnectTarget>(
    mut new_conn_rx: impl Stream<
        Item = (
            Box<[u8; 16]>,
            protocol::server_agent::GreetedRead<ClientStream, Cipher>,
            protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
        ),
    > + Unpin,
    config: Config<TConnectTarget>,
) -> Result<(), std::io::Error>
where
    ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Sync + Send + 'static,
    Cipher: StreamCipher + Unpin + Sync + Send + 'static,
    TConnectTarget: ConnectTarget,
{
    let mut conn_senders =
        HashMap::<Box<protocol::ClientId>, handover::Sender<Greeted<ClientStream, Cipher>>>::new();

    let (worker_ending_tx, mut worker_ending_rx) = mpsc::channel::<(
        Box<protocol::ClientId>,
        handover::ChannelRef<Greeted<ClientStream, Cipher>>,
    )>(2);

    loop {
        match futures::future::select(new_conn_rx.next(), worker_ending_rx.next()).await {
            future::Either::Left((conn, _)) => {
                // it should never end
                let (client_id, client_read, client_write) = conn.unwrap();
                let conn = (client_read, client_write);

                if let Err(conn) = {
                    // try to send to existing worker first
                    match conn_senders.get_mut(&client_id) {
                        Some(existing_sender) => {
                            existing_sender.send(conn).await.inspect_err(|_| {
                                conn_senders.remove(&client_id);
                            })
                        }
                        None => Err::<(), Greeted<ClientStream, Cipher>>(conn),
                    }
                } {
                    // create new worker

                    let (mut worker_conn_tx, worker_conn_rx) =
                        handover::channel::<Greeted<ClientStream, Cipher>>();

                    let worker = connection_group::run(worker_conn_rx, config.clone().into());

                    tokio::spawn({
                        let client_id = client_id.clone();
                        let mut worker_ending_tx = worker_ending_tx.clone();
                        async move {
                            let channel_ref = match worker.await {
                                Ok(channel_ref) => channel_ref,
                                Err((channel_ref, err)) => {
                                    error!("worker task failed: {:?}", err);
                                    channel_ref
                                }
                            };

                            let _ = worker_ending_tx.send((client_id, channel_ref)).await;
                        }
                        // TODO: record worker identifier in the span
                        .instrument(info_span!("worker task"))
                    });

                    let _ = worker_conn_tx.send(conn).await;
                    conn_senders.insert(client_id, worker_conn_tx);
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

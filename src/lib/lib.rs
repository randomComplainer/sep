#![feature(duration_constructors_lite)]
#![feature(ip_from)]
#![feature(trait_alias)]
#![feature(assert_matches)]

pub mod handover;
pub mod sink_ext;

#[macro_use]
pub mod decode;
pub mod encrypt;

pub mod protocol;
pub mod session;
pub mod socks5;

pub mod client_conn_group;
pub mod client_main_task;

pub mod prelude {
    pub use crate::decode::*;
    pub use crate::encrypt::{EncryptedRead, EncryptedWrite};
    pub use crate::handover::ChannelExt as _;
    pub use crate::sink_ext::SinkExt as _;
    pub use crate::{decode, protocol, session, socks5};
}

pub mod server_main_task {
    use std::collections::HashMap;

    use chacha20::cipher::StreamCipher;
    use futures::channel::mpsc;
    use futures::prelude::*;

    use crate::handover;
    use crate::prelude::*;

    type Greeted<Stream, Cipher> = (
        protocol::server_agent::GreetedRead<Stream, Cipher>,
        protocol::server_agent::GreetedWrite<Stream, Cipher>,
    );

    pub async fn run<ClientStream, Cipher>(
        mut new_conn_rx: impl Stream<
            Item = (
                Box<[u8; 16]>,
                protocol::server_agent::GreetedRead<ClientStream, Cipher>,
                protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
            ),
        > + Unpin,
    ) -> Result<(), std::io::Error>
    where
        ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        Cipher: StreamCipher + Unpin + Send + 'static,
    {
        let mut conn_senders = HashMap::<
            Box<protocol::ClientId>,
            handover::Sender<Greeted<ClientStream, Cipher>>,
        >::new();

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

                        let worker = crate::client_conn_group::run(worker_conn_rx);

                        tokio::spawn({
                            let client_id = client_id.clone();
                            let mut worker_ending_tx = worker_ending_tx.clone();
                            async move {
                                let channel_ref = match worker.await {
                                    Ok(channel_ref) => channel_ref,
                                    Err((channel_ref, err)) => {
                                        dbg!(format!("worker task failed: {:?}", err));
                                        channel_ref
                                    }
                                };

                                dbg!("worker task ended");

                                let _ = worker_ending_tx.send((client_id, channel_ref)).await;
                            }
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
}

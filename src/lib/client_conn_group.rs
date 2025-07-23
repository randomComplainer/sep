use std::sync::Arc;

use chacha20::cipher::StreamCipher;
use dashmap::DashMap;
use futures::prelude::*;
use rand::prelude::*;

use crate::prelude::*;

async fn client_connection_lifetime_task<ClientStream, Cipher>(
    mut client_read: protocol::server_agent::GreetedRead<ClientStream, Cipher>,
    mut client_write: protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
    mut client_msg_tx: futures::channel::mpsc::Sender<protocol::msg::ClientMsg>,
    mut server_msg_rx: futures::channel::mpsc::Receiver<protocol::msg::ServerMsg>,
) -> Result<Vec<protocol::msg::ServerMsg>, std::io::Error>
where
    ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    Cipher: StreamCipher + Unpin + Send + 'static,
{
    //TODO: error handling

    let mut time_limit_task = Box::pin(tokio::time::sleep(std::time::Duration::from_mins(
         rand::rng().random_range(..=5u64) + 10,
    )));

    // let mut time_limit_task = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(10)));

    let reciving_msg_from_client = async move {
        while let Some(msg) = client_read.recv_msg().await.unwrap() {
            client_msg_tx.send(msg).await.unwrap();
        }

        Ok::<_, std::io::Error>(())
    };

    let sending_msg_to_client = async move {
        loop {
            match futures::future::select(time_limit_task, server_msg_rx.next()).await {
                futures::future::Either::Left(_) => {
                    dbg!("hit time limit");
                    server_msg_rx.close();

                    let _ = client_write.close().await;

                    return Ok::<_, std::io::Error>(server_msg_rx.collect::<Vec<_>>().await);
                }
                futures::future::Either::Right((server_msg_opt, not_yet)) => {
                    if let Some(server_msg) = server_msg_opt {
                        time_limit_task = not_yet;
                        client_write.send_msg(server_msg).await.unwrap();
                    } else {
                        return Ok::<_, std::io::Error>(Default::default());
                    }
                }
            }
        }
    };

    tokio::try_join! {
        sending_msg_to_client,
        reciving_msg_from_client,
    }
    .map(|(msgs, _)| msgs)
}

pub async fn run<ClientStream, Cipher>(
    mut new_conn_rx: impl Stream<
        Item = (
            u16,
            protocol::server_agent::GreetedRead<ClientStream, Cipher>,
            protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
        ),
    > + Unpin,
) -> Result<(), std::io::Error>
where
    ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    Cipher: StreamCipher + Unpin + Send + 'static,
{
    let session_client_msg_senders = Arc::new(DashMap::<
        u16,
        futures::channel::mpsc::Sender<session::msg::ClientMsg>,
    >::new());

    let (server_msg_tx, mut server_msg_rx) =
        futures::channel::mpsc::channel::<protocol::msg::ServerMsg>(4);

    let (client_write_tx, mut client_write_rx) = futures::channel::mpsc::channel::<
        futures::channel::mpsc::Sender<protocol::msg::ServerMsg>,
        // protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
    >(16);

    let (client_msg_tx, mut client_msg_rx) =
        futures::channel::mpsc::channel::<protocol::msg::ClientMsg>(4);

    let receiving_msg_from_client = {
        let server_msg_tx = server_msg_tx.clone();
        let session_client_msg_senders = Arc::clone(&session_client_msg_senders);
        async move {
            while let Some(client_msg) = client_msg_rx.next().await {
                match client_msg {
                    protocol::msg::ClientMsg::SessionMsg(
                        session_id,
                        session::msg::ClientMsg::Request(_),
                    ) => {
                        // TODO: cache size
                        let (client_msg_tx, client_msg_rx) = futures::channel::mpsc::channel(4);
                        let session_task = session::server::run(
                            client_msg_rx,
                            server_msg_tx.clone().with(move |msg| {
                                std::future::ready(Ok(protocol::msg::ServerMsg::SessionMsg(
                                    session_id, msg,
                                )))
                            }),
                        );

                        // It's fine if session fails,
                        // log and continue
                        tokio::spawn({
                            let session_client_msg_senders =
                                Arc::clone(&session_client_msg_senders);
                            async move {
                                match session_task.await {
                                    Ok(_) => (),
                                    Err(err) => {
                                        dbg!(format!("session task failed: {:?}", err));
                                    }
                                };

                                session_client_msg_senders.remove(&session_id);
                            }
                        });

                        if session_client_msg_senders.contains_key(&session_id) {
                            panic!("session already exists");
                        }

                        session_client_msg_senders.insert(session_id, client_msg_tx);
                    }
                    _ => {}
                };

                match client_msg {
                    protocol::msg::ClientMsg::SessionMsg(session_id, session_msg) => {
                        // it's fine if send fails
                        // session could be ended due to target closed or something
                        if let Some(mut client_msg_tx) =
                            session_client_msg_senders.get_mut(&session_id)
                        {
                            let _ = client_msg_tx.send(session_msg).await;
                        }
                    }
                };
            }

            Ok::<_, std::io::Error>(())
        }
    };

    let accepting_client_conns = {
        let server_msg_tx = server_msg_tx.clone();
        let client_msg_tx = client_msg_tx.clone();
        let client_write_tx = client_write_tx.clone();
        async move {
            while let Some((_, client_read, client_write)) = new_conn_rx.next().await {
                let (conn_server_msg_tx, conn_server_msg_rx) = futures::channel::mpsc::channel(1);

                tokio::spawn({
                    let mut server_msg_tx = server_msg_tx.clone();
                    let client_msg_tx = client_msg_tx.clone();
                    async move {
                        // TODO: error handling
                        if let Ok(server_msgs) = client_connection_lifetime_task(
                            client_read,
                            client_write,
                            client_msg_tx.clone(),
                            conn_server_msg_rx,
                        )
                        .await
                        {
                            dbg!(format!("client connection lifetime task ended"));
                            for server_msg in server_msgs {
                                // TODO: error handling
                                let _ = server_msg_tx.send(server_msg).await;
                            }
                        }
                    }
                });

                let _ = client_write_tx.clone().send(conn_server_msg_tx).await;
            }

            Ok::<_, std::io::Error>(())
        }
    };

    let sending_msg_to_client = async move {
        while let Some(server_msg) = server_msg_rx.next().await {
            let mut client_write = client_write_rx.next().await.unwrap();
            tokio::spawn({
                let mut client_write_tx = client_write_tx.clone();
                let mut server_msg_tx = server_msg_tx.clone();
                async move {
                    dbg!(format!("message to client: {:?}", &server_msg));

                    match client_write.try_send(server_msg) {
                        Ok(_) => {
                            // TODO: do I care about error?
                            let _ = client_write_tx.send(client_write).await.unwrap();
                        }
                        Err(err) => {
                            dbg!(format!(
                                "failed to send message through server write: {:?}",
                                err
                            ));
                            server_msg_tx.send(err.into_inner()).await.unwrap();
                        }
                    }
                }
            });
        }
        Ok::<_, std::io::Error>(())
    };

    tokio::try_join! {
        accepting_client_conns,
        sending_msg_to_client,
        receiving_msg_from_client,
    }
    .map(|_| ())
}

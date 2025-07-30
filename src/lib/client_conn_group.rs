use std::sync::Arc;

use chacha20::cipher::StreamCipher;
use dashmap::DashMap;
use futures::prelude::*;
use rand::prelude::*;
use thiserror::Error;
use tokio::sync::watch;

use crate::handover;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ConnectionGroupError {
    #[error("session protocol error: session id {0}: {1}")]
    SessionProtocol(u16, String),
    #[error("lost connection to client")]
    LostConnection,
}

async fn client_connection_lifetime_task<ClientStream, Cipher>(
    mut client_read: protocol::server_agent::GreetedRead<ClientStream, Cipher>,
    mut client_write: protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
    mut client_msg_tx: futures::channel::mpsc::Sender<protocol::msg::ClientMsg>,
    mut server_msg_rx: handover::Receiver<protocol::msg::ServerMsg>,
) -> Result<(), std::io::Error>
where
    ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    Cipher: StreamCipher + Unpin + Send + 'static,
{
    //TODO: error handling

    let mut time_limit_task = Box::pin(tokio::time::sleep(std::time::Duration::from_mins(
        rand::rng().random_range(..=10u64) + 5,
    )));

    // let mut time_limit_task = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(10)));

    let reciving_msg_from_client = async move {
        while let Some(msg) = client_read.recv_msg().await.unwrap() {
            dbg!(format!("message from client: {:?}", &msg));
            client_msg_tx.send(msg).await.unwrap();
        }

        Ok::<_, std::io::Error>(())
    };

    let sending_msg_to_client = async move {
        loop {
            match futures::future::select(time_limit_task, server_msg_rx.recv().boxed()).await {
                futures::future::Either::Left(x) => {
                    dbg!("hit connection lifetime limit");
                    drop(x);
                    let _ = client_write.close().await;

                    return Ok::<_, std::io::Error>(());
                }
                futures::future::Either::Right((server_msg_opt, not_yet)) => {
                    if let Some(server_msg) = server_msg_opt {
                        time_limit_task = not_yet;
                        dbg!(format!("message to client: {:?}", &server_msg));
                        client_write.send_msg(server_msg).await.unwrap();
                    } else {
                        return Ok::<_, std::io::Error>(());
                    }
                }
            }
        }
    };

    tokio::try_join! {
        sending_msg_to_client,
        reciving_msg_from_client,
    }
    .map(|_| ())
}

type Greeted<ClientStream, Cipher> = (
    protocol::server_agent::GreetedRead<ClientStream, Cipher>,
    protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
);
type GreetedReciver<ClientStream, Cipher> = handover::Receiver<Greeted<ClientStream, Cipher>>;
type GreetedChannelRef<ClientStream, Cipher> = handover::ChannelRef<Greeted<ClientStream, Cipher>>;

pub async fn run<ClientStream, Cipher>(
    mut new_conn_rx: GreetedReciver<ClientStream, Cipher>,
) -> Result<
    GreetedChannelRef<ClientStream, Cipher>,
    (
        GreetedChannelRef<ClientStream, Cipher>,
        ConnectionGroupError,
    ),
>
where
    ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    Cipher: StreamCipher + Unpin + Send + 'static,
{
    let channel_ref = new_conn_rx.create_channel_ref();

    let session_client_msg_senders = Arc::new(DashMap::<
        u16,
        futures::channel::mpsc::Sender<session::msg::ClientMsg>,
    >::new());

    let (server_msg_tx, mut server_msg_rx) =
        futures::channel::mpsc::channel::<protocol::msg::ServerMsg>(4);

    let (client_write_tx, mut client_write_rx) =
        futures::channel::mpsc::channel::<handover::Sender<protocol::msg::ServerMsg>>(16);

    let (client_msg_tx, mut client_msg_rx) =
        futures::channel::mpsc::channel::<protocol::msg::ClientMsg>(4);

    let (err_tx, mut err_rx) = futures::channel::mpsc::channel::<ConnectionGroupError>(1);

    struct State {
        conn_num: usize,
        session_num: usize,
    }
    let (state_tx, mut state_rx) = watch::channel(State {
        conn_num: 0,
        session_num: 0,
    });

    let receiving_msg_from_client = {
        let server_msg_tx = server_msg_tx.clone();
        let session_client_msg_senders = Arc::clone(&session_client_msg_senders);
        let state_tx = state_tx.clone();
        let err_tx = err_tx.clone();
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
                            server_msg_tx.clone().with_sync(move |msg| {
                                protocol::msg::ServerMsg::SessionMsg(session_id, msg)
                            }),
                        );

                        if session_client_msg_senders.contains_key(&session_id) {
                            return Err(ConnectionGroupError::SessionProtocol(
                                session_id,
                                "duplicated session id".to_string(),
                            ));
                        }

                        // It's fine if session fails,
                        // log and continue
                        tokio::spawn({
                            let session_client_msg_senders =
                                Arc::clone(&session_client_msg_senders);
                            let state_tx = state_tx.clone();
                            let mut err_tx = err_tx.clone();
                            async move {
                                state_tx.send_modify(|state| {
                                    state.session_num += 1;
                                });

                                match session_task.await {
                                    Ok(_) => (),
                                    Err(err) => {
                                        dbg!(format!("session task failed: {:?}", err));
                                        if let session::server::ServerSessionError::Protocol(err) =
                                            err
                                        {
                                            let _ = err_tx
                                                .send(ConnectionGroupError::SessionProtocol(
                                                    session_id, err,
                                                ))
                                                .await;
                                        }
                                    }
                                };

                                dbg!("session task ended");

                                state_tx.send_modify(|state| {
                                    state.session_num -= 1;
                                });

                                session_client_msg_senders.remove(&session_id);
                            }
                        });

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

            Ok::<_, ConnectionGroupError>(())
        }
    };

    let accepting_client_conns = {
        let client_msg_tx = client_msg_tx.clone();
        let client_write_tx = client_write_tx.clone();
        let err_tx = err_tx.clone();

        async move {
            while let Some((client_read, client_write)) = new_conn_rx.recv().await {
                let (conn_server_msg_tx, conn_server_msg_rx) =
                    handover::channel::<protocol::msg::ServerMsg>();

                tokio::spawn({
                    let client_msg_tx = client_msg_tx.clone();
                    let state_tx = state_tx.clone();
                    let mut err_tx = err_tx.clone();
                    async move {
                        state_tx.send_modify(|state| {
                            state.conn_num += 1;
                        });

                        // TODO: error handling
                        match client_connection_lifetime_task(
                            client_read,
                            client_write,
                            client_msg_tx.clone(),
                            conn_server_msg_rx,
                        )
                        .await
                        {
                            Ok(_) => {
                                dbg!(format!("client connection lifetime task ended"));

                                state_tx.send_modify(|state| {
                                    state.conn_num -= 1;
                                });
                            }
                            Err(_) => {
                                let _ = err_tx.send(ConnectionGroupError::LostConnection).await;
                            }
                        }
                    }
                });

                let _ = client_write_tx.clone().send(conn_server_msg_tx).await;
            }

            Ok::<_, ConnectionGroupError>(())
        }
    };

    let sending_msg_to_client = async move {
        while let Some(server_msg) = server_msg_rx.next().await {
            let mut client_write = client_write_rx.next().await.unwrap();
            tokio::spawn({
                let mut client_write_tx = client_write_tx.clone();
                let mut server_msg_tx = server_msg_tx.clone();
                async move {
                    match client_write.send(server_msg).await {
                        Ok(_) => {
                            // TODO: do I care about error?
                            let _ = client_write_tx.send(client_write).await.unwrap();
                        }
                        Err(server_msg) => {
                            // TODO: piroritize resending?
                            server_msg_tx.send(server_msg).await.unwrap();
                        }
                    }
                }
            });
        }
        Ok::<_, ConnectionGroupError>(())
    };

    let checking_state_ending_condition = async move {
        // wait for initial state set
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let _ = state_rx
            .wait_for(|state| state.conn_num == 0 && state.session_num == 0)
            .await;

        return Ok::<_, ConnectionGroupError>(());
    };

    let checking_err = async move {
        let err = err_rx.next().await.unwrap();
        dbg!(format!("connection group error: {:?}", err));

        return err;
    };

    let ending_conditions = async move {
        match future::select(
            checking_state_ending_condition.boxed(),
            checking_err.boxed(),
        )
        .await
        {
            future::Either::Left(_) => Ok::<_, ConnectionGroupError>(()),
            future::Either::Right((err, _)) => Err(err),
        }
    };

    let loop_tasks = async move {
        tokio::try_join! {
            accepting_client_conns,
            sending_msg_to_client,
            receiving_msg_from_client,
        }
        .map(|_| ())
    };

    match futures::future::select(ending_conditions.boxed(), loop_tasks.boxed()).await {
        future::Either::Left((r, _)) => match r {
            Ok(_) => Ok(channel_ref.clone()),
            Err(err) => Err((channel_ref, err)),
        },
        future::Either::Right((loop_tasks_result, _)) => {
            let err = loop_tasks_result.unwrap_err();
            Err((channel_ref, err))
        }
    }
}

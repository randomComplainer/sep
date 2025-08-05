use std::sync::Arc;

use chacha20::cipher::StreamCipher;
use dashmap::DashMap;
use futures::channel::mpsc;
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

struct State {
    conn_num: usize,
    session_num: usize,
}

#[derive(Clone)]
struct Ctx {
    scope_handle: task_scope::ScopeHandle<ConnectionGroupError>,
    session_client_msg_senders: Arc<DashMap<u16, mpsc::Sender<session::msg::ClientMsg>>>,
    server_msg_tx: mpsc::Sender<protocol::msg::ServerMsg>,
    state_tx: watch::Sender<State>,
}

async fn run_session(
    ctx: Ctx,
    session_id: u16,
    client_msg_rx: mpsc::Receiver<session::msg::ClientMsg>,
) -> Result<(), ConnectionGroupError> {
    let session_task_result = session::server::run(
        client_msg_rx,
        ctx.server_msg_tx
            .clone()
            .with_sync(move |msg| protocol::msg::ServerMsg::SessionMsg(session_id, msg)),
    )
    .await;

    dbg!("session task ended");
    ctx.state_tx.send_modify(|state| {
        state.session_num -= 1;
    });
    ctx.session_client_msg_senders.remove(&session_id);

    match session_task_result {
        Ok(_) => Ok(()),
        Err(err) => {
            dbg!(format!("session task failed: {:?}", err));
            if let session::server::ServerSessionError::Protocol(err) = err {
                Err(ConnectionGroupError::SessionProtocol(session_id, err))
            } else {
                Ok(())
            }
        }
    }
}

async fn receiving_msg_from_client(
    mut ctx: Ctx,
    mut client_msg_rx: mpsc::Receiver<protocol::msg::ClientMsg>,
) -> Result<(), ConnectionGroupError> {
    while let Some(client_msg) = client_msg_rx.next().await {
        if let protocol::msg::ClientMsg::SessionMsg(
            session_id,
            session::msg::ClientMsg::Request(_),
        ) = &client_msg
        {
            // TODO: cache size
            let (client_msg_tx, client_msg_rx) = mpsc::channel(4);

            if ctx.session_client_msg_senders.contains_key(session_id) {
                return Err(ConnectionGroupError::SessionProtocol(
                    *session_id,
                    "duplicated session id".to_string(),
                ));
            }

            ctx.session_client_msg_senders
                .insert(*session_id, client_msg_tx);

            ctx.state_tx.send_modify(|state| {
                state.session_num += 1;
            });

            ctx.scope_handle
                .run_async(run_session(ctx.clone(), *session_id, client_msg_rx))
                .await
                .unwrap();
        };

        match client_msg {
            protocol::msg::ClientMsg::SessionMsg(session_id, session_msg) => {
                // it's fine if send fails
                // session could be ended due to target closed or something
                if let Some(mut client_msg_tx) = ctx.session_client_msg_senders.get_mut(&session_id)
                {
                    let _ = client_msg_tx.send(session_msg).await;
                }
            }
        };
    }

    Ok::<_, ConnectionGroupError>(())
}

async fn client_connection_lifetime_task<ClientStream, Cipher>(
    mut client_read: protocol::server_agent::GreetedRead<ClientStream, Cipher>,
    mut client_write: protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
    mut client_msg_tx: mpsc::Sender<protocol::msg::ClientMsg>,
    mut server_msg_rx: handover::Receiver<protocol::msg::ServerMsg>,
) -> Result<(), std::io::Error>
where
    ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    Cipher: StreamCipher + Unpin + Send + 'static,
{
    //TODO: error handling
    dbg!("client connection lifetime task started");

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
    ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Sync + Send + 'static,
    Cipher: StreamCipher + Unpin + Send + Sync + 'static,
{
    let channel_ref = new_conn_rx.create_channel_ref();

    let session_client_msg_senders =
        Arc::new(DashMap::<u16, mpsc::Sender<session::msg::ClientMsg>>::new());

    let (server_msg_tx, mut server_msg_rx) = mpsc::channel::<protocol::msg::ServerMsg>(4);

    let (client_write_tx, mut client_write_rx) =
        mpsc::channel::<handover::Sender<protocol::msg::ServerMsg>>(16);

    let (client_msg_tx, client_msg_rx) = mpsc::channel::<protocol::msg::ClientMsg>(4);

    let (state_tx, mut state_rx) = watch::channel(State {
        conn_num: 0,
        session_num: 0,
    });

    let (scope_handle, scope_task) = task_scope::new_scope::<ConnectionGroupError>();

    let mut ctx = Ctx {
        scope_handle: scope_handle.clone(),
        session_client_msg_senders: session_client_msg_senders.clone(),
        server_msg_tx: server_msg_tx.clone(),
        state_tx: state_tx.clone(),
    };

    ctx.scope_handle
        .run_async(receiving_msg_from_client(ctx.clone(), client_msg_rx))
        .await
        .unwrap();

    // accepting client conns
    ctx.scope_handle
        .run_async({
            let mut ctx = ctx.clone();
            let client_msg_tx = client_msg_tx.clone();
            let client_write_tx = client_write_tx.clone();

            async move {
                while let Some((client_read, client_write)) = new_conn_rx.recv().await {
                    dbg!("new client connection accepted");
                    let (conn_server_msg_tx, conn_server_msg_rx) =
                        handover::channel::<protocol::msg::ServerMsg>();

                    ctx.scope_handle
                        .run_async({
                            let client_msg_tx = client_msg_tx.clone();
                            let ctx = ctx.clone();
                            async move {
                                ctx.state_tx.send_modify(|state| {
                                    state.conn_num += 1;
                                });

                                {
                                    let r = client_connection_lifetime_task(
                                        client_read,
                                        client_write,
                                        client_msg_tx.clone(),
                                        conn_server_msg_rx,
                                    )
                                    .await;
                                    ctx.state_tx.send_modify(|state| {
                                        state.conn_num -= 1;
                                    });

                                    r
                                }
                                .inspect_err(|err| {
                                    dbg!(format!(
                                        "client connection lifetime task failed: {:?}",
                                        err
                                    ));
                                })
                                .inspect(|_| {
                                    dbg!("client connection lifetime task ended");
                                })
                                .map_err(|_| ConnectionGroupError::LostConnection)
                            }
                        })
                        .await
                        .unwrap();

                    let _ = client_write_tx.clone().send(conn_server_msg_tx).await;
                }

                Ok::<_, ConnectionGroupError>(())
            }
        })
        .await
        .unwrap();

    // sending msg to client
    ctx.scope_handle
        .run_async({
            let mut ctx = ctx.clone();
            async move {
                while let Some(server_msg) = server_msg_rx.next().await {
                    let mut client_write = client_write_rx.next().await.unwrap();
                    ctx.scope_handle
                        .spawn({
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
                                };

                                Ok(())
                            }
                        })
                        .await
                        .unwrap();
                }
                Ok::<_, ConnectionGroupError>(())
            }
        })
        .await
        .unwrap();

    let checking_state_ending_condition = async move {
        // wait for initial state set
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let _ = state_rx
            .wait_for(|state| state.conn_num == 0 && state.session_num == 0)
            .await;

        return Ok::<_, ConnectionGroupError>(());
    };

    match futures::future::select(scope_task.boxed(), checking_state_ending_condition.boxed()).await
    {
        future::Either::Left((e, _)) => Err((channel_ref, e)),
        future::Either::Right(_) => Ok(channel_ref),
    }
}

use std::ops::Add;
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use futures::channel::mpsc;
use futures::prelude::*;
use rand::prelude::*;
use thiserror::Error;
use tokio::sync::watch;
use tracing::*;

use crate::handover;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ConnectionGroupError {
    #[error("lost connection to client")]
    LostConnection,
}

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<session::server::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> session::server::Config<TConnectTarget> {
        session::server::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
            connect_target: self.connect_target,
        }
    }
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

async fn run_session<TConnectTarget>(
    ctx: Ctx,
    session_id: u16,
    client_msg_rx: mpsc::Receiver<session::msg::ClientMsg>,
    config: Config<TConnectTarget>,
) -> Result<(), ConnectionGroupError>
where
    TConnectTarget: ConnectTarget,
{
    let session_task_result = session::server::run(
        client_msg_rx,
        ctx.server_msg_tx
            .clone()
            .with_sync(move |msg| protocol::msg::ServerMsg::SessionMsg(session_id, msg)),
        config.into(),
    )
    .await;

    ctx.state_tx.send_modify(|state| {
        state.session_num -= 1;
    });
    ctx.session_client_msg_senders.remove(&session_id);

    // only known error is client channel broken
    session_task_result
        .inspect_err(|err| error!("session task failed: {:?}", err))
        .map_err(|_| ConnectionGroupError::LostConnection)
}

async fn receiving_msg_from_client<TConnectTarget>(
    mut ctx: Ctx,
    mut client_msg_rx: mpsc::Receiver<protocol::msg::ClientMsg>,
    config: Config<TConnectTarget>,
) -> Result<(), ConnectionGroupError>
where
    TConnectTarget: ConnectTarget,
{
    while let Some(client_msg) = client_msg_rx.next().await {
        if let protocol::msg::ClientMsg::SessionMsg(
            session_id,
            session::msg::ClientMsg::Request(_),
        ) = &client_msg
        {
            let session_span = info_span!(
                "session",
                session_id = session_id,
                start_time = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            );

            // TODO: cache size
            let (client_msg_tx, client_msg_rx) = mpsc::channel(4);

            if ctx.session_client_msg_senders.contains_key(session_id) {
                panic!("duplicated session id: {:?}", session_id);
            }

            ctx.session_client_msg_senders
                .insert(*session_id, client_msg_tx);

            ctx.state_tx.send_modify(|state| {
                state.session_num += 1;
            });

            ctx.scope_handle
                .run_async(
                    run_session(ctx.clone(), *session_id, client_msg_rx, config.clone())
                        .instrument(session_span),
                )
                .await;
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

async fn client_connection_lifetime_task(
    mut client_read: impl protocol::server_agent::GreetedRead,
    mut client_write: impl protocol::server_agent::GreetedWrite,
    mut client_msg_tx: mpsc::Sender<protocol::msg::ClientMsg>,
    mut server_msg_rx: handover::Receiver<protocol::msg::ServerMsg>,
) -> Result<(), std::io::Error> {
    //TODO: error handling
    debug!("client connection lifetime task started");

    let duration = std::time::Duration::from_secs(10).add(std::time::Duration::from_mins(
        rand::rng().random_range(..=10u64),
    ));
    let mut time_limit_task = Box::pin(tokio::time::sleep(duration));

    let reciving_msg_from_client = async move {
        while let Some(msg) = match client_read.recv_msg().await {
            Ok(msg_opt) => msg_opt,
            Err(e) => match e {
                DecodeError::Io(err) => return Err(err),
                DecodeError::InvalidStream(err) => panic!("invalid stream: {:?}", err),
            },
        } {
            debug!("message from client: {:?}", &msg);
            if let Err(_) = client_msg_tx.send(msg).await {
                warn!("failed to forward client message, exiting");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "failed to forward client message",
                ));
            }
        }

        Ok::<_, std::io::Error>(())
    };

    let sending_msg_to_client = async move {
        loop {
            match futures::future::select(time_limit_task, server_msg_rx.recv().boxed()).await {
                futures::future::Either::Left(x) => {
                    debug!("hit connection lifetime limit");
                    drop(x);
                    client_write
                        .send_msg(protocol::msg::ServerMsg::EndOfStream)
                        .await?;
                    // let _ = client_write.close().await;
                    drop(client_write);

                    return Ok::<_, std::io::Error>(());
                }
                futures::future::Either::Right((server_msg_opt, not_yet)) => {
                    if let Some(server_msg) = server_msg_opt {
                        time_limit_task = not_yet;
                        debug!("message to client: {:?}", &server_msg);
                        client_write.send_msg(server_msg).await?;
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

pub async fn run<GreetedRead, GreetedWrite, TConnectTarget>(
    mut new_conn_rx: handover::Receiver<(GreetedRead, GreetedWrite)>,
    config: Config<TConnectTarget>,
) -> Result<
    handover::ChannelRef<(GreetedRead, GreetedWrite)>,
    (
        handover::ChannelRef<(GreetedRead, GreetedWrite)>,
        ConnectionGroupError,
    ),
>
where
    GreetedRead: protocol::server_agent::GreetedRead,
    GreetedWrite: protocol::server_agent::GreetedWrite,
    TConnectTarget: ConnectTarget,
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
        .run_async(receiving_msg_from_client(
            ctx.clone(),
            client_msg_rx,
            config,
        ))
        .await;

    // accepting client conns
    ctx.scope_handle
        .run_async({
            let mut ctx = ctx.clone();
            let client_msg_tx = client_msg_tx.clone();
            let client_write_tx = client_write_tx.clone();

            async move {
                while let Some((client_read, client_write)) = new_conn_rx.recv().await {
                    debug!("new client connection accepted");
                    let (conn_server_msg_tx, conn_server_msg_rx) =
                        handover::channel::<protocol::msg::ServerMsg>();

                    ctx.scope_handle
                        .run_async(
                            {
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
                                        error!("client connection lifetime task failed: {:?}", err);
                                    })
                                    .inspect(|_| {
                                        debug!("client connection lifetime task ended");
                                    })
                                    .map_err(|_| ConnectionGroupError::LostConnection)
                                }
                            }
                            // TODO: record connection identifier in the span
                            .instrument(info_span!("client connection")),
                        )
                        .await;

                    let _ = client_write_tx.clone().send(conn_server_msg_tx).await;
                }

                Ok::<_, ConnectionGroupError>(())
            }
        })
        .await;

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
                        .await;
                }
                Ok::<_, ConnectionGroupError>(())
            }
        })
        .await;

    let checking_state_ending_condition = async move {
        // wait for initial state set
        // TODO: find a better solution
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let _ = state_rx
            .wait_for(|state| state.conn_num == 0 && state.session_num == 0)
            .await;

        return Ok::<_, ConnectionGroupError>(());
    };

    match futures::future::select(
        scope_task.boxed(),
        checking_state_ending_condition
            .instrument(info_span!("check ending condition"))
            .boxed(),
    )
    .await
    {
        future::Either::Left((e, _)) => Err((channel_ref, e)),
        future::Either::Right(_) => Ok(channel_ref),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    #[test_log::test(tokio::test)]
    async fn happy_path() {
        let (mut new_conn_tx, new_conn_rx) = handover::channel();
        let config = Config {
            max_packet_ahead: 4,
            max_packet_size: 1024,
            connect_target: crate::connect_target::make_mock([(
                (ReadRequestAddr::Domain("example.com".into()), 80),
                Ok((
                    tokio_test::io::Builder::new().build(),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 180),
                )),
            )]),
        };

        let main_task = tokio::spawn(run(new_conn_rx, config));

        let (client_agent, server_agent) = protocol::test_utils::create_greeted_pair().await;
        let server_agent = (server_agent.1, server_agent.2);

        new_conn_tx
            .send(server_agent)
            .await
            .map_err(|_| ())
            .unwrap();

        let (mut client_agent_read, mut client_agent_write) = client_agent;

        client_agent_write
            .send_msg(
                (
                    2,
                    session::msg::Request {
                        addr: decode::ReadRequestAddr::Domain("example.com".into()),
                        port: 80,
                    }
                    .into(),
                )
                    .into(),
            )
            .await
            .unwrap();

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            (
                2,
                session::msg::Reply {
                    bound_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 180),
                }
                .into()
            )
                .into()
        );

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            (2, session::msg::Eof { seq: 0 }.into()).into()
        );

        client_agent_write
            .send_msg((2, session::msg::Ack { seq: 0 }.into()).into())
            .await
            .unwrap();

        client_agent_write
            .send_msg((2, session::msg::Eof { seq: 0 }.into()).into())
            .await
            .unwrap();

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            protocol::msg::ServerMsg::SessionMsg(2, session::msg::Ack { seq: 0 }.into())
        );

        // let connection timeout run out
        tokio::time::pause();

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            protocol::msg::ServerMsg::EndOfStream
        );

        drop(client_agent_read);
        drop(client_agent_write);

        let result = main_task.await;
        assert!(result.unwrap().is_ok());
    }

    // TODO: test more
}

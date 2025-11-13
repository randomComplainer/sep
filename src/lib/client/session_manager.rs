use dashmap::DashMap;
use futures::prelude::*;
use tracing::*;

use crate::prelude::*;

#[derive(Debug)]
pub enum Command {
    ServerMsg(u16, session::msg::ServerMsg),
}

pub enum Event {
    New(u16),
    Ended(u16),
    ClientMsg(u16, session::msg::ClientMsg),
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
}

impl Into<session::client::Config> for Config {
    fn into(self) -> session::client::Config {
        session::client::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
        }
    }
}

pub async fn run<ProxyeeStream>(
    mut new_proxyee_rx: impl Stream<Item = (u16, socks5::agent::Init<ProxyeeStream>)>
    + Unpin
    + Send
    + 'static,
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    mut evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Clone + Unpin + Send + 'static,
    config: Config,
) where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sessions_scope_handle, sessions_scope_task) = task_scope::new_scope::<()>();
    tokio::pin!(sessions_scope_task);

    // relay session ending events so that
    // we don't have to access the hashmap in multiple tasks
    let (local_session_ending_tx, mut local_session_ending_rx) =
        futures::channel::mpsc::channel::<u16>(config.max_packet_ahead as usize);

    let session_server_msg_senders =
        DashMap::<u16, futures::channel::mpsc::Sender<session::msg::ServerMsg>>::new();

    let main_loop = async move {
        loop {
            tokio::select! {
                proxyee = new_proxyee_rx.next() => {
                    let (session_id, proxyee) = proxyee.unwrap();
                    let (session_server_msg_tx, session_server_msg_rx) =
                        futures::channel::mpsc::channel(config.max_packet_ahead as usize);
                    session_server_msg_senders.insert(session_id, session_server_msg_tx);

                    evt_tx.send(Event::New(session_id))
                        .instrument(info_span!("nofity session creation", session_id = session_id))
                        .await.expect("evt_tx is broken");

                    sessions_scope_handle.run_async({
                        let evt_tx = evt_tx.clone();
                        let mut local_session_ending_tx = local_session_ending_tx.clone();

                        async move {
                            let session_result = session::client::run(
                                proxyee,
                                session_server_msg_rx,
                                evt_tx.clone().with_sync(move |client_msg| Event::ClientMsg(session_id, client_msg)),
                                config.into(),
                            )
                                .instrument(info_span!("session", session_id = session_id))
                                .await;
                            local_session_ending_tx.send(session_id).await.expect("local_session_ending_tx is broken");
                            match session_result {
                                Ok(_) => Ok(()),
                                Err(err) => {
                                    error!("session task failed: {:?}", err);
                                    Ok(())
                                }
                            }
                        }
                    }).await;
                },

                proxyee_ending = local_session_ending_rx.next() => {
                    let proxyee_id = proxyee_ending.unwrap();
                    session_server_msg_senders.remove(&proxyee_id);
                    evt_tx.send(Event::Ended(proxyee_id))
                        .instrument(info_span!("nofity session ending", session_id = proxyee_id))
                        .await.expect("evt_tx is broken");
                    },

                    cmd = cmd_rx.next() => {
                        let cmd = match cmd {
                            Some(cmd) => cmd,
                            None => {
                                debug!("end of cmd stream, exiting");
                                break;
                            },
                        };

                        debug!("cmd: {:?}", cmd);

                        match cmd {
                            Command::ServerMsg(session_id, server_msg) => {
                                if let Some(mut session_server_msg_sender) = session_server_msg_senders.get_mut(&session_id) {
                                    let span = info_span!("send server msg to session", session_id = session_id, ?server_msg);
                                    if let Err(_) = session_server_msg_sender.send(server_msg)
                                        .instrument(span)
                                            .await {
                                                debug!("session server msg reciver is dropped, drop server msg");
                                    }
                                } else {
                                    debug!("session server msg receiver does not exist, drop server msg");
                                }
                            },
                        }
                    },
            }
        }
    };

    tokio::select! {
        x = main_loop.instrument(info_span!("main loop")) => {
            x
        },
        _ = sessions_scope_task => {
            unreachable!("session scope task ended unexpectedly");
        }
    }
}

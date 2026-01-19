use std::{collections::HashMap, time::SystemTime};

use futures::{channel::mpsc, prelude::*};
use tracing::*;

use crate::prelude::*;
use protocol::SessionId;

#[derive(Debug)]
pub enum Command {
    ClientMsg(SessionId, session::msg::ClientMsg),
}

#[derive(Debug)]
pub enum Event {
    ServerMsg(SessionId, session::msg::ServerMsg),
    Started(SessionId),
    Ended(SessionId),
}

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<session::server::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> session::server::Config<TConnectTarget> {
        session::server::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
            connect_target: self.connect_target,
        }
    }
}

pub async fn run<TConnectTarget>(
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Clone + Unpin + Send + 'static,
    config: Config<TConnectTarget>,
) where
    TConnectTarget: ConnectTarget,
{
    let (mut session_scope_handle, session_scope_task) = task_scope::new_scope::<()>();

    let receiving_client_msg = {
        let config = config.clone();
        let mut evt_tx = evt_tx.clone();

        async move {
            let mut session_client_msg_senders =
                HashMap::<SessionId, mpsc::UnboundedSender<session::msg::ClientMsg>>::new();

            while let Some(cmd) = cmd_rx.next().await {
                debug!(?cmd, "new cmd incoming");
                match cmd {
                    Command::ClientMsg(session_id, client_msg) => {
                        if let session::msg::ClientMsg::Request(_) = &client_msg {
                            if session_client_msg_senders.contains_key(&session_id) {
                                warn!(
                                    ?session_id,
                                    "duplicated session id, dropping previous session"
                                );
                                session_client_msg_senders.remove(&session_id);
                            }

                            // TODO: cache size
                            let (client_msg_tx, client_msg_rx) = mpsc::unbounded();
                            session_client_msg_senders.insert(session_id, client_msg_tx);

                            session_scope_handle
                                .run_async({
                                    let mut evt_tx = evt_tx.clone();
                                    let config = config.clone();

                                    let session_span = info_span!(
                                        "session",
                                        ?session_id,
                                        start_time = SystemTime::now()
                                            .duration_since(SystemTime::UNIX_EPOCH)
                                            .unwrap()
                                            .as_secs()
                                    );

                                    async move {
                                        // ignore error
                                        // it's target io error, it's fine
                                        let _ = session::server::run(
                                            client_msg_rx,
                                            evt_tx.clone().with_sync(move |evt| {
                                                Event::ServerMsg(session_id, evt)
                                            }),
                                            config.into(),
                                        )
                                        .await;

                                        evt_tx
                                            .send(Event::Ended(session_id))
                                            .await
                                            .map_err(|_| ())
                                            .inspect_err(|_| warn!("evt_tx is broken, exiting"))
                                    }
                                    .instrument(session_span)
                                })
                                .await;

                            if let Err(_) = evt_tx.send(Event::Started(session_id)).await {
                                warn!("evt_tx is broken, exiting");
                                return;
                            }
                        }

                        let session_client_msg_sender =
                            match session_client_msg_senders.get_mut(&session_id) {
                                None => {
                                    warn!(?session_id, "session does not exist, drop client msg");
                                    continue;
                                }
                                Some(x) => x,
                            };

                        let span = info_span!(
                            "send client msg to session",
                            ?session_id,
                            ?client_msg
                        );

                        if let Err(_) = session_client_msg_sender
                            .send(client_msg)
                            .instrument(span)
                            .await
                        {
                            warn!("session client msg sender is broken, dropping session");
                            session_client_msg_senders.remove(&session_id);
                        }
                    }
                };
            }
        }
    }
    .instrument_with_result(info_span!("receiving client msg"));

    // both future end on borken pipeline (~= broken server connection)
    tokio::select! {
        _ = receiving_client_msg => (),
        _ = session_scope_task.instrument(info_span!("session scope task")) => ()
    }
}

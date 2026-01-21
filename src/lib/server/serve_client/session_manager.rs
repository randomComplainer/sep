use std::collections::HashMap;

use futures::{SinkExt, TryFutureExt, channel::mpsc};
use tracing::Instrument as _;

use crate::{
    prelude::*,
    protocol::{ConnId, SessionId},
};

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

pub enum Event {
    SessionEnded(SessionId),
}

pub struct SessionEntry {
    pub client_msg_tx: mpsc::UnboundedSender<session::msg::ClientMsg>,
}

pub struct State<TConnectTarget> {
    config: Config<TConnectTarget>,
    sessions: HashMap<SessionId, SessionEntry>,
    sessions_scope_handle: task_scope::ScopeHandle<Never>,
    evt_tx: mpsc::UnboundedSender<Event>,
    server_msg_sender_rx: async_channel::Receiver<(
        ConnId,
        oneshot_with_ack::RawSender<protocol::msg::ServerMsg>,
    )>,
}

impl<TConnectTarget> State<TConnectTarget>
where
    TConnectTarget: ConnectTarget,
{
    pub fn new(
        config: Config<TConnectTarget>,
        evt_tx: mpsc::UnboundedSender<Event>,
        server_msg_sender_rx: async_channel::Receiver<(
            ConnId,
            oneshot_with_ack::RawSender<protocol::msg::ServerMsg>,
        )>,
    ) -> (
        Self,
        impl Future<Output = Result<(), Never>> + Send + 'static,
    ) {
        let (sessions_scope_handle, sessions_scope_task) = task_scope::new_scope::<Never>();

        (
            Self {
                config,
                sessions: HashMap::new(),
                sessions_scope_handle,
                evt_tx,
                server_msg_sender_rx,
            },
            sessions_scope_task,
        )
    }

    pub async fn on_client_msg(&mut self, session_id: SessionId, msg: session::msg::ClientMsg) {
        if let session::msg::ClientMsg::Request(_) = &msg {
            assert!(!self.sessions.contains_key(&session_id));

            let (session_client_msg_tx, session_client_msg_rx) = mpsc::unbounded();
            let (session_server_msg_sending_queue_tx, session_server_msg_sending_queue_rx) =
                mpsc::unbounded();

            let server_msg_sending_task = crate::dispatch_with_max_concurrency::run(
                session_server_msg_sending_queue_rx,
                self.server_msg_sender_rx.clone(),
            )
            .map_err(|e| match e {
                crate::dispatch_with_max_concurrency::Error::MessageLost => {
                    std::io::Error::new(std::io::ErrorKind::Other, "message lost")
                }
            });

            let client_handling_task = session::server::run(
                session_client_msg_rx,
                session_server_msg_sending_queue_tx.with_sync(move |server_msg| {
                    protocol::msg::ServerMsg::SessionMsg(session_id, server_msg)
                }),
                self.config.clone().into(),
            );

            let mut evt_tx = self.evt_tx.clone();
            let backup_server_msg_sender_rx = self.server_msg_sender_rx.clone();
            let session_span = tracing::trace_span!("session", ?session_id);
            let session_task = async move {
                tokio::select! {
                    r = client_handling_task => if let Err(err) = r {
                        tracing::error!(?err, "client handling error");
                    },
                    r = server_msg_sending_task => if let Err(err) = r {
                        tracing::error!(?err, "server msg sending error");

                        // message loss!
                        loop {
                            let (conn_id, server_msg_tx) = match backup_server_msg_sender_rx.recv().await {
                                Ok(x) => x,
                                Err(_) => {
                                    tracing::error!("server_msg_sender_rx is broken, exiting");
                                    break;
                                },
                            };

                            match server_msg_tx.send(protocol::msg::ServerMsg::KillSession(session_id))
                                .instrument(tracing::debug_span!("send kill session message", ?session_id, ?conn_id))
                                .await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        tracing::warn!(?session_id, ?conn_id, "failed to send kill session message, retry it");
                                        continue;
                                    },
                                };
                        }
                    }
                };

                if let Err(_) = evt_tx.send(Event::SessionEnded(session_id)).await {
                    tracing::warn!("end of session tx is broken");
                }

                Ok(())
            }
            .instrument(session_span);

            self.sessions_scope_handle.run_async(session_task).await;
            self.sessions.insert(
                session_id,
                SessionEntry {
                    client_msg_tx: session_client_msg_tx,
                },
            );
        };

        let entry = match self.sessions.get_mut(&session_id) {
            Some(entry) => entry,
            None => {
                tracing::warn!(?session_id, "session does not exist, drop client msg");
                return;
            }
        };

        if let Err(_) = entry.client_msg_tx.send(msg).await {
            tracing::warn!("client_msg_tx is broken, drop client msg");
        }
    }

    pub async fn on_session_end(&mut self, session_id: SessionId) {
        let _ = self.sessions.remove(&session_id);
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions.len()
    }
}

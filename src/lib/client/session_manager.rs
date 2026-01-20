use std::collections::HashMap;

use futures::{FutureExt, Sink, SinkExt, TryFutureExt, channel::mpsc};
use tracing::Instrument as _;

use crate::{
    prelude::*,
    protocol::{ConnId, SessionId},
};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
}

impl Into<session::client::Config> for Config {
    fn into(self) -> session::client::Config {
        session::client::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
        }
    }
}

pub enum Event {
    SessionEnded(SessionId),
}

struct SessionEntry {
    pub server_msg_tx: mpsc::UnboundedSender<session::msg::ServerMsg>,
}

pub struct State<EvtTx> {
    config: Config,
    sessions: HashMap<SessionId, SessionEntry>,
    sessions_scope_handle: task_scope::ScopeHandle<Never>,
    evt_tx: EvtTx,
    client_msg_sender_rx: async_channel::Receiver<(
        ConnId,
        oneshot_with_ack::RawSender<protocol::msg::ClientMsg>,
    )>,
}

impl<EvtTx> State<EvtTx>
where
    EvtTx: Sink<Event> + Unpin + Send + Clone + 'static,
{
    pub fn new(
        config: Config,
        evt_tx: EvtTx,
        client_msg_sender_rx: async_channel::Receiver<(
            ConnId,
            crate::oneshot_with_ack::RawSender<protocol::msg::ClientMsg>,
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
                client_msg_sender_rx,
            },
            sessions_scope_task,
        )
    }

    pub async fn new_session(
        &mut self,
        session_id: SessionId,
        agent: impl socks5::server_agent::Init,
    ) {
        assert!(!self.sessions.contains_key(&session_id));

        let (session_server_msg_tx, session_server_msg_rx) = mpsc::unbounded();
        let (session_client_msg_sending_queue_tx, session_client_msg_sending_queue_rx) =
            mpsc::unbounded();

        let session_task = session::client::run(
            agent,
            session_server_msg_rx,
            session_client_msg_sending_queue_tx.with_sync(move |client_msg| {
                protocol::msg::ClientMsg::SessionMsg(session_id, client_msg)
            }),
            self.config.into(),
        )
        .map(|()| Ok::<_, std::io::Error>(()));
        // .instrument(tracing::trace_span!("proxyee handling"));

        let client_msg_sending_task = crate::dispatch_with_max_concurrency::run(
            session_client_msg_sending_queue_rx,
            self.client_msg_sender_rx.clone(),
        )
        .map_err(|e| match e {
            crate::dispatch_with_max_concurrency::Error::MessageLost => {
                std::io::Error::new(std::io::ErrorKind::Other, "message lost")
            }
        });
        // .instrument(tracing::trace_span!("client msg dispatching"));

        let mut evt_tx = self.evt_tx.clone();
        let session_span = tracing::trace_span!("session", ?session_id);
        let session_task = async move {
            tokio::select! {
                r = session_task => if let Err(err) = r {
                    tracing::error!(?err, "session task error");
                },
                r = client_msg_sending_task => if let Err(err) = r {
                    tracing::error!(?err, "client msg sending task error");
                },
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
                server_msg_tx: session_server_msg_tx,
            },
        );
    }

    pub async fn server_msg_to_session(
        &mut self,
        session_id: SessionId,
        server_msg: session::msg::ServerMsg,
    ) {
        let entry = match self.sessions.get_mut(&session_id) {
            Some(entry) => entry,
            None => {
                tracing::warn!("session does not exist, drop server msg");
                return;
            }
        };

        if let Err(_) = entry.server_msg_tx.send(server_msg).await {
            tracing::warn!("server_msg_tx is broken, exiting");
        }
    }

    pub async fn end_session(&mut self, session_id: SessionId) {
        assert!(self.sessions.remove(&session_id).is_some());
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions.len()
    }
}

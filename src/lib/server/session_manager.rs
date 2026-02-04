use std::collections::{HashMap, HashSet};

use futures::{channel::mpsc, prelude::*};
use tracing::Instrument as _;

use crate::{prelude::*, protocol_conn_lifetime::WriteHandle};
use super::target_io;

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<target_io::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> target_io::Config<TConnectTarget> {
        target_io::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
            connect_target: self.connect_target,
        }
    }
}

pub enum Event {
    SessionEnded(SessionId),
    SessionErrored(SessionId),
    RequestWriteHandle(SessionId, ConnId),
}

pub struct SessionEntry {
    pub client_msg_tx: mpsc::UnboundedSender<protocol::msg::session::ClientMsg>,
    pub session_server_writer_tx:
        mpsc::UnboundedSender<(ConnId, WriteHandle<protocol::msg::ServerMsg>)>,
    pub conns: HashSet<ConnId>,
}

pub struct State<TConnectTarget> {
    config: Config<TConnectTarget>,
    sessions: HashMap<SessionId, SessionEntry>,
    sessions_scope_handle: task_scope::ScopeHandle<Never>,
    evt_tx: mpsc::UnboundedSender<Event>,
}

impl<TConnectTarget> State<TConnectTarget>
where
    TConnectTarget: ConnectTarget,
{
    pub fn new(
        config: Config<TConnectTarget>,
        evt_tx: mpsc::UnboundedSender<Event>,
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
            },
            sessions_scope_task,
        )
    }

    pub async fn client_msg_to_session(
        &mut self,
        conn_id: ConnId,
        session_id: SessionId,
        msg: protocol::msg::session::ClientMsg,
    ) {
        if let protocol::msg::session::ClientMsg::Request(_) = &msg {
            assert!(!self.sessions.contains_key(&session_id));

            let (session_client_msg_tx, session_client_msg_rx) = mpsc::unbounded();
            let (session_server_msg_tx, session_server_msg_rx) = mpsc::unbounded();

            let target_io_task = target_io::run(
                session_client_msg_rx,
                session_server_msg_tx.with_sync(move |server_msg| {
                    protocol::msg::ServerMsg::SessionMsg(session_id, server_msg)
                }),
                self.config.clone().into(),
            );

            let (session_conn_write_tx, session_conn_write_rx) = mpsc::unbounded();

            let server_msg_sending_task = crate::protocol_conn_lifetime::WriteHandle::loop_through(
                session_server_msg_rx,
                session_conn_write_rx,
            );

            let mut evt_tx = self.evt_tx.clone();

            let session_task = async move {
                let evt = match tokio::try_join! {
                    target_io_task.inspect(|result| tracing::debug!(?result, "test 1")).map_err(|_| ()),
                    server_msg_sending_task.inspect(|result| tracing::debug!(?result, "test 2")).map_err(|_| ()) }
                {
                    Ok(_) => Event::SessionEnded(session_id),
                    Err(_) => Event::SessionErrored(session_id),
                };

                if let Err(_) = evt_tx.send(evt).await {
                    tracing::warn!("evt_tx is broken");
                }

                Ok(())
            }
            .instrument(tracing::trace_span!("session", ?session_id));

            self.sessions_scope_handle.run_async(session_task).await;
            self.sessions.insert(
                session_id,
                SessionEntry {
                    client_msg_tx: session_client_msg_tx,
                    session_server_writer_tx: session_conn_write_tx,
                    conns: Default::default(),
                },
            );
        }

        let entry = match self.sessions.get_mut(&session_id) {
            Some(entry) => entry,
            None => {
                tracing::warn!(?session_id, "session does not exist, drop client msg");
                return;
            }
        };

        if !entry.conns.contains(&conn_id) {
            if let Err(_) = self
                .evt_tx
                .send(Event::RequestWriteHandle(session_id, conn_id))
                .await
            {
                tracing::warn!("evt_tx is broken");
            }
        }

        if let Err(_) = entry.client_msg_tx.send(msg).await {
            tracing::warn!("client_msg_tx is broken, drop client msg");
        }
    }

    pub async fn assign_conn_to_session(
        &mut self,
        session_id: SessionId,
        conn_id: ConnId,
        write_handle: WriteHandle<protocol::msg::ServerMsg>,
    ) {
        if let Some(entry) = self.sessions.get_mut(&session_id) {
            if entry.conns.contains(&conn_id) {
                return;
            }

            tracing::debug!(?session_id, ?conn_id, "assign conn to session");
            entry.conns.insert(conn_id);
            if let Err(_) = entry
                .session_server_writer_tx
                .send((conn_id, write_handle))
                .await
            {
                tracing::warn!("session_server_writer_tx is broken");
            }
        }
    }

    pub async fn on_conn_errored(&mut self, conn_id: ConnId) {
        // TODO: inefficient
        let session_ids = self
            .sessions
            .iter()
            .filter(|(_, entry)| entry.conns.contains(&conn_id))
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();

        for session_id in session_ids {
            tracing::debug!(?session_id, ?conn_id, "terminate session due to conn error");
            let _ = self.evt_tx.send(Event::SessionErrored(session_id)).await;
        }
    }

    pub async fn close_session(&mut self, session_id: SessionId) {
        let _ = self.sessions.remove(&session_id);
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions.len()
    }
}

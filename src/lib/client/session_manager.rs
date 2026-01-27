use std::collections::HashMap;

use futures::channel::mpsc;
use futures::prelude::*;
use tracing::Instrument as _;

use crate::{prelude::*, protocol_conn_lifetime_new::WriteHandle};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
    pub conn_per_session: usize,
    pub max_connection: usize,
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
    ExpectedToHaveConns(usize),
    SessionEnded(SessionId),
    SessionErrored(SessionId),
}

struct SessionEntry {
    pub server_msg_tx: mpsc::UnboundedSender<session::msg::ServerMsg>,
    pub client_msg_sender_tx:
        mpsc::UnboundedSender<(ConnId, WriteHandle<protocol::msg::ClientMsg>)>,
}

pub struct State {
    config: Config,
    sessions: HashMap<SessionId, SessionEntry>,
    sessions_scope_handle: task_scope::ScopeHandle<Never>,
    evt_tx: mpsc::UnboundedSender<Event>,
}

impl State {
    pub fn new(
        config: Config,
        evt_tx: mpsc::UnboundedSender<Event>,
    ) -> (Self, impl Future<Output = Result<(), Never>>) {
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

    pub async fn new_session(
        &mut self,
        session_id: SessionId,
        agent: impl socks5::server_agent::Init,
    ) {
        assert!(!self.sessions.contains_key(&session_id));
        let (session_server_msg_tx, session_server_msg_rx) = mpsc::unbounded();
        let (session_client_msg_tx, session_client_msg_rx) = mpsc::unbounded();

        let proxyee_io_task = session::client::run(
            agent,
            session_server_msg_rx,
            session_client_msg_tx.with_sync(move |msg: session::msg::ClientMsg| {
                protocol::msg::ClientMsg::SessionMsg(session_id, msg)
            }),
            self.config.into(),
        );

        let (session_conn_write_tx, session_conn_write_rx) = mpsc::unbounded();

        let client_msg_sending_task =
            client_msg_sending_loop(session_client_msg_rx, session_conn_write_rx);

        let mut evt_tx = self.evt_tx.clone();
        let session_task = async move {
            let evt = match tokio::try_join! {
                proxyee_io_task.map(|()| Ok::<_, ConnId>(())),
                client_msg_sending_task,
            } {
                Ok(_) => Event::SessionEnded(session_id),
                Err(_) => Event::SessionErrored(session_id),
            };

            if let Err(_) = evt_tx.send(evt).await {
                tracing::warn!("evt_tx is broken");
            }

            Ok(())
        }
        .instrument(tracing::debug_span!("session", session_id=?session_id));

        self.sessions_scope_handle.run_async(session_task).await;
        self.sessions.insert(
            session_id,
            SessionEntry {
                server_msg_tx: session_server_msg_tx,
                client_msg_sender_tx: session_conn_write_tx,
            },
        );

        let _ = self
            .evt_tx
            .send(Event::ExpectedToHaveConns(std::cmp::min(
                self.config.conn_per_session * self.sessions.len(),
                self.config.max_connection,
            )))
            .await;
    }

    pub async fn server_msg_to_session(
        &mut self,
        session_id: SessionId,
        server_msg: session::msg::ServerMsg,
    ) {
        let entry = match self.sessions.get_mut(&session_id) {
            Some(entry) => entry,
            None => {
                tracing::warn!(?session_id, "session does not exist, drop server msg");
                return;
            }
        };

        if let Err(_) = entry.server_msg_tx.send(server_msg).await {
            tracing::warn!("server_msg_tx is broken");
        }
    }

    pub async fn assign_conn_to_session(
        &mut self,
        session_id: SessionId,
        conn_id: ConnId,
        write_handle: WriteHandle<protocol::msg::ClientMsg>,
    ) {
        if let Some(entry) = self.sessions.get_mut(&session_id) {
            // tracing::trace!(conn_id=?conn_id, session_id=?session_id, "assign conn to session");
            if let Err(_) = entry
                .client_msg_sender_tx
                .send((conn_id, write_handle))
                .await
            {
                tracing::warn!("client_msg_sender_tx is broken");
            }
        }
    }

    pub async fn end_session(&mut self, session_id: SessionId) {
        let _ = self.sessions.remove(&session_id);
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions.len()
    }
}

// Err = Conn IO Error
async fn client_msg_sending_loop(
    mut msg_queue: impl Stream<Item = protocol::msg::ClientMsg> + Unpin,
    mut conn_writer_rx: impl Stream<Item = (ConnId, WriteHandle<protocol::msg::ClientMsg>)> + Unpin,
) -> Result<(), ConnId> {
    use NextSender::*;

    let mut handles: Vec<(ConnId, WriteHandle<protocol::msg::ClientMsg>)> = Vec::new();

    let mut head_msg = None::<protocol::msg::ClientMsg>;

    loop {
        let msg = match head_msg.take() {
            Some(msg) => msg,
            None => match msg_queue.next().await {
                Some(msg) => msg,
                None => {
                    tracing::debug!("end of messages");
                    return Ok(());
                }
            },
        };

        tracing::trace!(?msg, handles_count = handles.len(), "dispatch client msg");

        let get_next_sender: std::pin::Pin<Box<dyn Future<Output = NextSender> + Send>> =
            if handles.is_empty() {
                Box::pin(std::future::pending())
            } else {
                Box::pin(next_sender(handles.iter()))
            };

        tokio::select! {
            next_sender_result = get_next_sender  => {
                let (conn_id, sender ) = match next_sender_result {
                    Found(conn_id, sender) => (conn_id, sender),
                    Closed(conn_id) => {
                        let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                        handles.swap_remove(idx);
                        continue;
                    },
                    IoError(conn_id) => return Err(conn_id),
                };

                tracing::trace!(?conn_id, ?msg, "send msg to conn");
                match sender.send(msg) {
                    Ok(_) => continue,
                    Err(msg) => {
                        // Closed
                        tracing::trace!("conn closed, retry sending msg to another conn");
                        head_msg = Some(msg);
                        let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                        handles.swap_remove(idx);
                        continue;
                    }
                }
            },

            conn = conn_writer_rx.next() => {
                let (conn_id, write_handle) = match conn {
                    Some(conn) => conn,
                    None => {
                        tracing::warn!("end of conn_writer_rx, exiting");
                        return Ok(());
                    }
                };

                head_msg = Some(msg);
                handles.push((conn_id, write_handle));
                tracing::trace!(?conn_id, "aquired conn");
                continue;
            },
        }
    }
}

pub enum NextSender {
    Found(
        ConnId,
        tokio::sync::oneshot::Sender<protocol::msg::ClientMsg>,
    ),
    Closed(ConnId),
    IoError(ConnId),
}

pub async fn next_sender(
    write_handles: impl Iterator<Item = &(ConnId, WriteHandle<protocol::msg::ClientMsg>)>,
) -> NextSender {
    use NextSender::*;

    let select_all =
        futures::future::select_all(write_handles.map(|(conn_id, write_handle)| {
            Box::pin(write_handle.get_sender().map(
                |write_handle_result| match write_handle_result {
                    Ok(Some(sender)) => Found(*conn_id, sender),
                    Ok(None) => Closed(*conn_id),
                    Err(_) => IoError(*conn_id),
                },
            ))
        }));

    select_all.await.0
}

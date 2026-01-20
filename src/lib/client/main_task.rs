use futures::{channel::mpsc, prelude::*};
use thiserror::Error;
use tracing::*;

use super::{server_connection_manager as conn_manager, session_manager};
use crate::client::ServerConnector;
use crate::prelude::*;
use crate::protocol::{ConnId, SessionId};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("session protocol error: session id {0}: {1}")]
    SessionProtocol(u16, String),
    #[error("lost connection to server")]
    LostServerConnection,
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_size: u16,
    pub max_server_conn: u16,
    pub max_bytes_ahead: u32,
}

impl Into<session_manager::Config> for Config {
    fn into(self) -> session_manager::Config {
        session_manager::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
        }
    }
}

struct State<SessionEvtTx, ServerConnector>
where
    ServerConnector: super::ServerConnector,
{
    config: Config,
    sessions_state: session_manager::State<SessionEvtTx>,
    conns_state: conn_manager::State<ServerConnector>,
}

impl<SessionEvtTx, ServerConnector> State<SessionEvtTx, ServerConnector>
where
    SessionEvtTx: Sink<session_manager::Event> + Unpin + Send + Clone + 'static,
    ServerConnector: super::ServerConnector,
{
    pub fn new(
        config: Config,
        sessions_state: session_manager::State<SessionEvtTx>,
        conns_state: conn_manager::State<ServerConnector>,
    ) -> Self {
        Self {
            config,
            sessions_state,
            conns_state,
        }
    }
    pub async fn set_expected_conn_count(&mut self) {
        let expected_conn_count = std::cmp::min(
            self.config.max_server_conn as usize,
            self.sessions_state.active_session_count() * 2,
        );

        self.conns_state
            .set_expected_conn_count(expected_conn_count)
            .await
    }

    pub async fn new_session(
        &mut self,
        session_id: SessionId,
        agent: impl socks5::server_agent::Init,
    ) {
        self.sessions_state.new_session(session_id, agent).await;
        self.set_expected_conn_count().await
    }

    pub async fn server_msg_to_session(
        &mut self,
        session_id: SessionId,
        server_msg: session::msg::ServerMsg,
    ) {
        self.sessions_state
            .server_msg_to_session(session_id, server_msg)
            .await;
    }

    pub async fn end_session(&mut self, session_id: SessionId) {
        self.sessions_state.end_session(session_id).await;
        self.set_expected_conn_count().await;
    }

    pub async fn connected(
        &mut self,
        conn_id: ConnId,
        greeted_read: ServerConnector::GreetedRead,
        greeted_write: ServerConnector::GreetedWrite,
    ) {
        self.conns_state
            .on_connected(Ok((conn_id, greeted_read, greeted_write)))
            .await
            .unwrap();
    }

    pub async fn conn_closed(&mut self, conn_id: ConnId) {
        self.conns_state.on_connection_closed(conn_id).await;
    }
}

pub async fn run<TServerConnector>(
    mut new_proxyee_rx: impl Stream<Item = (SessionId, impl socks5::server_agent::Init)>
    + Unpin
    + Send
    + 'static,
    connect_to_server: TServerConnector,
    config: Config,
) -> std::io::Result<()>
where
    TServerConnector: ServerConnector + Send,
{
    let (client_msg_sender_tx, client_msg_sender_rx) = async_channel::unbounded();

    let (sessions_evt_tx, mut sessions_evt_rx) = futures::channel::mpsc::unbounded();
    let (sessions_state, sessions_task) =
        session_manager::State::new(config.into(), sessions_evt_tx, client_msg_sender_rx);

    let (conns_evt_tx, mut conns_evt_rx) = futures::channel::mpsc::unbounded();
    let (conns_state, conns_task) =
        conn_manager::State::new(connect_to_server, conns_evt_tx, client_msg_sender_tx);

    let mut state = State::new(config, sessions_state, conns_state);

    // TODO: exit condition
    let main_loop = async move {
        loop {
            tokio::select! {
                session_evt = sessions_evt_rx.next() => {
                    let session_evt = match session_evt {
                        Some(session_evt) => session_evt,
                        None => {
                            tracing::warn!("session_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    match session_evt {
                        session_manager::Event::SessionEnded(session_id) => {
                            state.end_session(session_id).await;
                        }
                    };
                },
                new_session = new_proxyee_rx.next() => {
                    let new_session = match new_session {
                        Some(new_session) => new_session,
                        None => {
                            tracing::warn!("new_proxyee_rx is broken, exiting");
                            return;
                        }
                    };
                    state.new_session(new_session.0, new_session.1).await;
                },

                conn_evt = conns_evt_rx.next() => {
                    let conn_evt = match conn_evt {
                        Some(conn_evt) => conn_evt,
                        None => {
                            tracing::warn!("conns_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    match conn_evt {
                        conn_manager::Event::Connected(conn_id, greeted_read, greeted_write) =>
                            state.connected(conn_id, greeted_read, greeted_write).await,
                        conn_manager::Event::ServerMsg(server_msg) => {
                            match server_msg {
                                protocol::msg::ServerMsg::SessionMsg(session_id, server_msg) =>
                                    state.server_msg_to_session(session_id, server_msg).await,
                            };
                        },
                        conn_manager::Event::Closed(conn_id) =>
                            state.conn_closed(conn_id).await,
                    };
                }
            }
        }
    };

    tokio::try_join! {
        sessions_task
            .instrument(tracing::trace_span!("session manager"))
            .map(|x| Ok::<_, std::io::Error>(x)),
        conns_task
            .instrument(tracing::trace_span!("conn manager")),
        main_loop
            .map(|_| Ok::<_, std::io::Error>(())) ,
    }
    .map(|_| ())
}

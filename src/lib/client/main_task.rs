use futures::prelude::*;
use thiserror::Error;
use tracing::*;

use super::{assignment, conn_manager, session_manager};
use crate::client::ServerConnector;
use crate::global_msg_manager;
use crate::prelude::*;
use crate::protocol::msg::AtLeastOnce;
use crate::protocol::msg::GlobalCmd;
use crate::protocol::msg::ServerMsg;
use crate::protocol::{ConnId, SessionId};
use crate::protocol_conn_lifetime::WriteHandle;

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
    pub max_server_conn: usize,
    pub conn_per_session: usize,
    pub max_bytes_ahead: u32,
}

impl Into<session_manager::Config> for Config {
    fn into(self) -> session_manager::Config {
        session_manager::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
            conn_per_session: self.conn_per_session,
            max_connection: self.max_server_conn,
        }
    }
}

struct State<ServerConnector>
where
    ServerConnector: super::ServerConnector,
{
    config: Config,
    sessions_state: session_manager::State,
    conns_state: conn_manager::State<ServerConnector>,
    assignment_state: assignment::State,
    global_message_handle: global_msg_manager::Handle<GlobalCmd, protocol::msg::ClientMsg>,
}

impl<ServerConnector> State<ServerConnector>
where
    ServerConnector: super::ServerConnector,
{
    // pub fn new(
    //     config: Config,
    //     sessions_state: session_manager::State,
    //     conns_state: conn_manager::State<ServerConnector>,
    // ) -> Self {
    //     Self {
    //         config,
    //         sessions_state,
    //         conns_state,
    //     }
    // }

    pub async fn new_session(
        &mut self,
        session_id: SessionId,
        agent: impl socks5::server_agent::Init,
    ) {
        self.sessions_state.new_session(session_id, agent).await;
        let actions = self.assignment_state.new_session(session_id);
        self.apply_assignment_actions(actions).await;
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
        self.assignment_state.session_ended_or_errored(session_id);
    }

    pub async fn expected_to_have_conns(&mut self, expected_conn_count: usize) {
        self.conns_state
            .set_expected_conn_count(expected_conn_count)
            .await;
    }

    pub async fn connected(
        &mut self,
        conn_id: ConnId,
        write_handle: WriteHandle<protocol::msg::ClientMsg>,
    ) {
        self.conns_state
            .on_connected(conn_id, write_handle.clone())
            .await;

        self.global_message_handle
            .new_conn(conn_id, write_handle.clone())
            .await;

        let actions = self.assignment_state.new_conn(conn_id, write_handle);
        self.apply_assignment_actions(actions).await;
    }

    pub async fn connect_attempt_failed(&mut self) {
        self.conns_state.on_connection_attempt_failed().await;
    }

    pub async fn conn_closed(&mut self, conn_id: ConnId) {
        self.conns_state.on_connection_closed(conn_id).await;
        let actions = self.assignment_state.close_conn(conn_id);
        self.apply_assignment_actions(actions).await;
    }

    pub async fn conn_errored(&mut self, conn_id: ConnId) {
        self.conns_state.on_connection_closed(conn_id).await;
        let actions = self.assignment_state.purge_errored_conn(conn_id);
        self.apply_assignment_actions(actions).await;
    }

    async fn apply_assignment_actions(&mut self, actions: Vec<assignment::Action>) {
        for action in actions {
            self.apply_assignment_action(action).await;
        }
    }

    async fn apply_assignment_action(&mut self, action: assignment::Action) {
        match action {
            assignment::Action::Assign(session_id, conn_id, write_handle) => {
                self.sessions_state
                    .assign_conn_to_session(session_id, conn_id, write_handle)
                    .await;
            }
            assignment::Action::Kill(session_id) => {
                self.sessions_state.end_session(session_id).await;
                self.global_message_handle
                    .send_msg(GlobalCmd::KillSession(session_id))
                    .await;
            }
        }
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
    let (sessions_evt_tx, mut sessions_evt_rx) = futures::channel::mpsc::unbounded();
    let (sessions_state, sessions_task) =
        session_manager::State::new(config.into(), sessions_evt_tx);

    let (conns_evt_tx, mut conns_evt_rx) = futures::channel::mpsc::unbounded();
    let (conns_state, conns_task) = conn_manager::State::new(connect_to_server, conns_evt_tx);

    let assignment_state = assignment::State::new(config.conn_per_session);

    let (global_message_task, global_message_handle) = global_msg_manager::run();

    let mut state = State {
        config,
        sessions_state,
        conns_state,
        assignment_state,
        global_message_handle,
    };

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
                        session_manager::Event::SessionEnded(session_id)=>{
                            state.end_session(session_id).await;
                        }
                        session_manager::Event::ExpectedToHaveConns(n) => {
                            state.expected_to_have_conns(n).await;
                        },
                        session_manager::Event::SessionErrored(session_id) => {
                            state.end_session(session_id).await;
                            state.global_message_handle.send_msg(GlobalCmd::KillSession(session_id)).await;
                        },
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
                        conn_manager::Event::Connected(conn_id, write_handle) =>
                            state.connected(conn_id, write_handle).await,
                        conn_manager::Event::ConnectAttemptFailed => {
                            state.connect_attempt_failed().await;
                        }
                        conn_manager::Event::ServerMsg(server_msg) => {
                            match server_msg {
                                ServerMsg::SessionMsg(session_id, server_msg) =>
                                    state.server_msg_to_session(session_id, server_msg).await,

                                ServerMsg::GlobalCmd(global_cmd) => {
                                    match global_cmd {
                                        AtLeastOnce::Ack(ack) =>
                                            state.global_message_handle.recv_ack(ack).await,
                                        AtLeastOnce::Msg(seq, cmd) => {
                                            state.global_message_handle.send_ack(seq).await;
                                            tracing::debug!(?cmd, "global cmd from server");
                                            match cmd {
                                                GlobalCmd::KillSession(session_id) =>
                                                    state.end_session(session_id).await,
                                            };
                                        },
                                    };
                                }
                            };
                        },
                        conn_manager::Event::Closed(conn_id) => {
                            state.conn_closed(conn_id).await;

                            if state.conns_state.active_conn_count() == 0  &&
                                state.sessions_state.active_session_count() == 0
                            {
                                tracing::info!("all conns & sessions ended, exiting");
                                return;
                            }
                        },
                        conn_manager::Event::Errored(conn_id) => {
                            state.conn_errored(conn_id).await;
                        },
                    };
                }
            }
        }
    };

    tokio::try_join! {
        sessions_task
            .map(|x| Ok(x.unwrap_never()))
            .instrument(tracing::trace_span!("session manager")),
        conns_task
            .map(|x| Ok(x.unwrap_never()))
            .instrument(tracing::trace_span!("conn manager")),
        global_message_task
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "failed sending global msg"))
            .instrument(tracing::trace_span!("global message manager")),
        main_loop
            .map(|_| Ok::<_, std::io::Error>(())) ,
    }
    .map(|_| ())
}

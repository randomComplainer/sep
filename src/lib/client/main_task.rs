use futures::channel::mpsc;
use futures::prelude::*;
use thiserror::Error;
use tracing::Instrument as _;

use super::{conn_host, proxyee_io, session_host};
use crate::prelude::*;
use crate::protocol::msg::AtLeastOnce;
use crate::protocol::msg::ServerMsg;
use crate::{assignment, global_cmd_manager};

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
    pub max_bytes_ahead_per_conn: u32,
    pub max_conn_per_session: u8,
}

impl Into<session_host::Config> for Config {
    fn into(self) -> session_host::Config {
        session_host::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead_per_conn: self.max_bytes_ahead_per_conn,
        }
    }
}

impl Into<assignment::Config> for Config {
    fn into(self) -> assignment::Config {
        assignment::Config {
            max_conn_per_session: self.max_conn_per_session,
        }
    }
}

struct State<ServerConnector, SessionEvtTx, ConnEvtTx> {
    config: Config,
    session_handle: session_host::Handle<SessionEvtTx>,
    conn_handle: conn_host::Handle<ConnEvtTx, ServerConnector>,
    global_cmd_handle: global_cmd_manager::Handle<protocol::msg::global_cmd::ClientCmd>,
    assignment: assignment::State<protocol::msg::ClientMsg, proxyee_io::Cmd>,
    attempting_conn_count: usize,
}

impl<ServerConnector, SessionEvtTx, ConnEvtTx, ConnEvtTxErr>
    State<ServerConnector, SessionEvtTx, ConnEvtTx>
where
    ServerConnector: super::ServerConnector,
    SessionEvtTx: Sink<session_host::Event> + Unpin + Send + Clone + 'static,
    ConnEvtTx: Sink<conn_host::Event, Error = ConnEvtTxErr> + Unpin + Send + Clone + 'static,
    ConnEvtTxErr: std::fmt::Debug + Send,
{
    pub fn new(
        config: Config,
        session_handle: session_host::Handle<SessionEvtTx>,
        conn_handle: conn_host::Handle<ConnEvtTx, ServerConnector>,
        global_cmd_handle: global_cmd_manager::Handle<protocol::msg::global_cmd::ClientCmd>,
    ) -> Self {
        Self {
            config,
            session_handle,
            conn_handle,
            global_cmd_handle,
            assignment: assignment::State::new(config.into()),
            attempting_conn_count: 0,
        }
    }

    pub async fn handle_new_proxyee(
        &mut self,
        session_id: SessionId,
        proxyee: impl socks5::server_agent::Init,
    ) {
        let session_msg_tx = self.session_handle.new_session(session_id, proxyee).await;
        self.assignment.on_new_session(session_id, session_msg_tx);
    }

    pub async fn handle_session_evt(&mut self, evt: session_host::Event) {
        match evt {
            session_host::Event::SessionEnded(session_id) => {
                self.assignment.on_session_ended(&session_id);
            }
            session_host::Event::ClientMsg(session_id, client_session_msg) => {
                let actions = self.assignment.new_outgoing_session_msg(
                    &session_id,
                    protocol::msg::ClientMsg::SessionMsg(session_id, client_session_msg),
                );

                self.handle_assignment_actions(actions).await;
            }
        };
    }

    pub async fn handle_conn_evt(&mut self, evt: conn_host::Event) {
        match evt {
            conn_host::Event::ServerConnected(conn_id) => {
                self.attempting_conn_count -= 1;
                self.assignment.on_conn_created(conn_id);
            }
            conn_host::Event::ClientMsgSenderReady(conn_id, sender) => {
                let actions = self.assignment.conn_ready_to_send(&conn_id, sender);
                self.handle_assignment_actions(actions).await;
            }
            conn_host::Event::ConnectionAttemptFailed => {
                // TODO: delay? retry limitation?
                self.conn_handle.create_connection().await;
            }
            conn_host::Event::ConnectionErrored(conn_id) => {
                let actions = self.assignment.on_conn_errored(&conn_id);
                self.handle_assignment_actions(actions).await;
            }
            conn_host::Event::ConnectionEnded(conn_id) => {
                self.assignment.on_conn_closed(&conn_id);
            }
            conn_host::Event::ServerMsg(conn_id, server_msg) => {
                match server_msg {
                    ServerMsg::SessionMsg(session_id, server_msg) => {
                        let actions = self
                            .assignment
                            .on_remote_msg_to_session(&conn_id, &session_id, server_msg.into())
                            .await;

                        self.handle_assignment_actions(actions).await;
                    }
                    ServerMsg::GlobalCmd(at_least_once) => {
                        match at_least_once {
                            AtLeastOnce::Ack(seq) => {
                                self.global_cmd_handle.ack(seq).await;
                            }
                            AtLeastOnce::Msg(seq, msg) => {
                                self.assignment.new_outgoing_global_msg(
                                    protocol::msg::ClientMsg::GlobalCmd(
                                        protocol::msg::AtLeastOnce::Ack(seq),
                                    ),
                                );

                                match msg {
                                    protocol::msg::global_cmd::ServerCmd::KillSession(
                                        session_id,
                                    ) => {
                                        self.assignment.on_session_ended(&session_id);
                                    }
                                    protocol::msg::global_cmd::ServerCmd::ConnectMore {
                                        expected,
                                    } => {
                                        self.match_expected_conn_count(expected.into()).await;
                                    }
                                };
                            }
                        };
                    }
                };
            }
        };
    }

    pub fn handle_global_cmd_event(
        &mut self,
        evt: global_cmd_manager::Event<protocol::msg::global_cmd::ClientCmd>,
    ) {
        match evt {
            global_cmd_manager::Event::Send(at_least_once) => {
                self.assignment
                    .new_outgoing_global_msg(at_least_once.into());
            }
        };
    }

    async fn handle_assignment_actions(
        &mut self,
        actions: impl IntoIterator<Item = assignment::Action>,
    ) {
        for action in actions.into_iter() {
            match action {
                assignment::Action::KillSession(session_id) => {
                    self.global_cmd_handle
                        .queue(protocol::msg::global_cmd::ClientCmd::KillSession(
                            session_id,
                        ))
                        .await;
                }
                assignment::Action::Assigned {
                    session_id,
                    assigned_conn_count,
                } => {
                    self.assignment
                        .on_local_msg_to_session(
                            &session_id,
                            proxyee_io::Cmd::UpdateConnCount(
                                assigned_conn_count.try_into().unwrap(),
                            ),
                        )
                        .await;
                }
                assignment::Action::ConnectMore { expected } => {
                    self.match_expected_conn_count(expected).await;
                }
            };
        }
    }

    async fn match_expected_conn_count(&mut self, expectation: usize) {
        while (self.attempting_conn_count + self.assignment.conn_count())
            < std::cmp::min(self.config.max_server_conn, expectation)
        {
            self.conn_handle.create_connection().await;
            self.attempting_conn_count += 1;
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
    TServerConnector: super::ServerConnector + Send,
{
    let (session_evt_tx, mut session_evt_rx) = mpsc::unbounded::<session_host::Event>();
    let (session_host_fut, session_handle) = session_host::create(config.into(), session_evt_tx);

    let (conn_evt_tx, mut conn_evt_rx) = mpsc::unbounded::<conn_host::Event>();
    let (conn_host_fut, conn_handle) = conn_host::create(conn_evt_tx, connect_to_server);

    let (global_cmd_evt_tx, mut global_cmd_evt_rx) = mpsc::unbounded();
    let (global_cmd_fut, global_cmd_handle) = global_cmd_manager::run(global_cmd_evt_tx);

    let mut state = State::new(config, session_handle, conn_handle, global_cmd_handle);

    let main_loop = async move {
        loop {
            tokio::select! {
                proxyee = new_proxyee_rx.next() => {
                    let (session_id, proxyee ) = match proxyee {
                        Some(x) => x,
                        None => {
                            tracing::warn!("new_proxyee_rx is broken, exiting");
                            return;
                        },
                    };

                    state.handle_new_proxyee(session_id, proxyee).await;
                },

                session_evt = session_evt_rx.next() => {
                    let session_evt = match session_evt {
                        Some(session_evt) => session_evt,
                        None => {
                            tracing::warn!("session_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    state.handle_session_evt(session_evt).await;
                },

                conn_evt = conn_evt_rx.next() => {
                    let conn_evt = match conn_evt {
                        Some(conn_evt) => conn_evt,
                        None => {
                            tracing::warn!("conns_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    state.handle_conn_evt(conn_evt).await;
                },

                global_cmd_evt = global_cmd_evt_rx.next() => {
                    let global_cmd_evt = match global_cmd_evt {
                        Some(global_cmd_evt) => global_cmd_evt,
                        None => {
                            tracing::warn!("global_cmd_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    state.handle_global_cmd_event(global_cmd_evt);
                },
            }
        }
    };

    tokio::try_join! {
        session_host_fut
            .map(|_| Ok::<_, std::io::Error>(()))
            .instrument(tracing::trace_span!("session host")),
        conn_host_fut
            .map(|_| Ok(()))
            .instrument(tracing::trace_span!("conn host")),
        global_cmd_fut
            .instrument(tracing::trace_span!("global cmd")),
        main_loop.map(|_| Ok(()))
            .instrument(tracing::trace_span!("main loop")),
    }
    .map(|_| ())
}

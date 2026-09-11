use futures::channel::mpsc;
use futures::prelude::*;
use thiserror::Error;
use tracing::Instrument as _;

use super::{conn_host, proxyee_io, session_host};
use crate::buffer_pool;
use crate::msg::{self, group, protocol::AtLeastOnce};
use crate::prelude::*;
use crate::some_or;
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
    pub max_bytes_ahead: u64,
    pub max_conn_per_session: u8,
}

impl Into<session_host::Config> for Config {
    fn into(self) -> session_host::Config {
        session_host::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
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

struct State<SessionEvtTx> {
    config: Config,
    session_handle: session_host::Handle<SessionEvtTx>,
    conn_handle: conn_host::Handle,
    global_cmd_handle: global_cmd_manager::Handle<group::ClientCmd>,
    assignment: assignment::State<msg::protocol::ClientMsg, proxyee_io::Cmd>,
    buf_pool: buffer_pool::BufferPool,
}

impl<SessionEvtTx> State<SessionEvtTx>
where
    SessionEvtTx: Sink<session_host::Event> + Unpin + Send + Sync + Clone + 'static,
{
    pub fn new(
        config: Config,
        session_handle: session_host::Handle<SessionEvtTx>,
        conn_handle: conn_host::Handle,
        global_cmd_handle: global_cmd_manager::Handle<group::ClientCmd>,
        buf_pool: buffer_pool::BufferPool,
    ) -> Self {
        Self {
            config,
            session_handle,
            conn_handle,
            global_cmd_handle,
            assignment: assignment::State::new(config.into()),
            buf_pool,
        }
    }

    pub async fn handle_new_proxyee(
        &mut self,
        session_id: SessionId,
        proxyee: proxy_interface::Init<
            impl tokio::io::AsyncRead + tokio::io::AsyncWrite + 'static + Unpin + Send + Sync,
        >,
    ) {
        let session_msg_tx = self
            .session_handle
            .new_session(session_id, proxyee, self.buf_pool.clone())
            .await;
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
                    msg::protocol::ClientMsg::SessionMsg(session_id, client_session_msg),
                );

                self.handle_assignment_actions(actions).await;
            }
        };
    }

    pub async fn handle_conn_evt(&mut self, evt: conn_host::Event) {
        match evt {
            conn_host::Event::ServerConnected(conn_id) => {
                self.assignment.on_conn_created(conn_id);
            }
            conn_host::Event::ClientMsgSenderReady(conn_id, sender) => {
                let actions = self.assignment.conn_ready_to_send(&conn_id, sender);
                self.handle_assignment_actions(actions).await;
            }
            conn_host::Event::ConnectionErrored(conn_id) => {
                let actions = self.assignment.on_conn_errored(&conn_id);
                self.handle_assignment_actions(actions).await;
            }
            conn_host::Event::ConnectionEnded(conn_id) => {
                let actions = self.assignment.on_conn_closed(&conn_id);
                self.handle_assignment_actions(actions).await;
            }
            conn_host::Event::ServerMsg(conn_id, server_msg) => {
                match server_msg {
                    msg::protocol::ServerMsg::SessionMsg(session_id, server_msg) => {
                        let actions = self
                            .assignment
                            .on_remote_msg_to_session(&conn_id, &session_id, server_msg.into())
                            .await;

                        self.handle_assignment_actions(actions).await;
                    }
                    msg::protocol::ServerMsg::GlobalCmd(at_least_once) => {
                        match at_least_once {
                            AtLeastOnce::Ack(seq) => {
                                self.global_cmd_handle.ack(seq).await;
                            }
                            AtLeastOnce::Msg(seq, msg) => {
                                let actions = self.assignment.new_outgoing_global_msg(
                                    msg::protocol::ClientMsg::GlobalCmd(AtLeastOnce::Ack(seq)),
                                );

                                self.handle_assignment_actions(actions).await;

                                match msg {
                                    msg::group::ServerCmd::KillSession(session_id) => {
                                        self.assignment.on_session_ended(&session_id);
                                    }
                                    msg::group::ServerCmd::ConnectMore { expected } => {
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
        evt: global_cmd_manager::Event<msg::group::ClientCmd>,
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
                        .queue(msg::group::ClientCmd::KillSession(session_id))
                        .await;
                }
                assignment::Action::ConnectMore { expected } => {
                    self.match_expected_conn_count(expected).await;
                }
            };
        }
    }

    async fn match_expected_conn_count(&mut self, expectation: usize) {
        // TODO: use u8 everywhere instead of usize
        self.conn_handle.expect_conn(
            std::cmp::min(self.config.max_server_conn, expectation)
                .try_into()
                .unwrap(),
        );
    }
}

pub async fn run<TServerConnector>(
    mut new_proxyee_rx: impl Stream<
        Item = (
            SessionId,
            proxy_interface::Init<
                impl tokio::io::AsyncRead + tokio::io::AsyncWrite + 'static + Unpin + Send + Sync,
            >,
        ),
    > + Unpin
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

    let (buffer_pool_fut, buffer_pool) = buffer_pool::BufferPool::create(buffer_pool::Config {
        buf_size: config.max_packet_size,
        pool_size: config.max_server_conn * 2 + 4,
    });

    let mut state = State::new(
        config,
        session_handle,
        conn_handle,
        global_cmd_handle,
        buffer_pool,
    );

    let main_loop = async move {
        loop {
            tokio::select! {
                proxyee = new_proxyee_rx.next() => {
                    let (session_id, proxyee ) = some_or!(proxyee, return) ;

                    state.handle_new_proxyee(session_id, proxyee).await;
                },

                session_evt = session_evt_rx.next() => {
                    let session_evt = some_or!(session_evt, return) ;

                    state.handle_session_evt(session_evt).await;
                },

                conn_evt = conn_evt_rx.next() => {
                    let conn_evt = some_or!(conn_evt, return) ;

                    state.handle_conn_evt(conn_evt).await;
                },

                global_cmd_evt = global_cmd_evt_rx.next() => {
                    let global_cmd_evt = some_or!(global_cmd_evt, return) ;

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
        buffer_pool_fut.map(|_| Ok(())),
    }
    .map(|_| ())
}

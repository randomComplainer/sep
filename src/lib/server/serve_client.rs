use futures::channel::mpsc;
use futures::prelude::*;
use tracing::Instrument as _;

use super::target_io;
use super::{conn_host, session_host};
use crate::buffer_pool;
use crate::codec::{MsgReader, MsgWriter};
use crate::prelude::*;
use crate::protocol::msg::ClientMsg;
use crate::protocol::msg::{self, AtLeastOnce};
use crate::{assignment, global_cmd_manager};

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u64,
    pub connect_target: TConnectTarget,
    pub max_conn_per_session: u8,
    pub buf_pool_size: usize,
}

impl<TConnectTarget> Into<session_host::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> session_host::Config<TConnectTarget> {
        session_host::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
            connect_target: self.connect_target,
        }
    }
}

impl<TConnectTarget> Into<assignment::Config> for Config<TConnectTarget> {
    fn into(self) -> assignment::Config {
        assignment::Config {
            max_conn_per_session: self.max_conn_per_session,
        }
    }
}

struct State<TConnectTarget, SessionEvtTx, ConnEvtTx> {
    config: Config<TConnectTarget>,
    session_handle: session_host::Handle<SessionEvtTx, TConnectTarget>,
    conn_handle: conn_host::Handle<ConnEvtTx>,
    global_cmd_handle: global_cmd_manager::Handle<protocol::msg::group::ServerCmd>,
    assignment: assignment::State<protocol::msg::ServerMsg, target_io::Cmd>,
}

impl<TConnectTarget, SessionEvtTx, ConnEvtTx, ConnEvtTxErr>
    State<TConnectTarget, SessionEvtTx, ConnEvtTx>
where
    TConnectTarget: crate::connect_target::ConnectTarget,
    SessionEvtTx: Sink<session_host::Event> + Unpin + Send + Clone + 'static,
    ConnEvtTx: Sink<conn_host::Event, Error = ConnEvtTxErr> + Unpin + Send + Clone + 'static,
    ConnEvtTxErr: std::fmt::Debug + Send + 'static,
{
    pub fn new(
        config: Config<TConnectTarget>,
        session_handle: session_host::Handle<SessionEvtTx, TConnectTarget>,
        conn_handle: conn_host::Handle<ConnEvtTx>,
        global_cmd_handle: global_cmd_manager::Handle<protocol::msg::group::ServerCmd>,
    ) -> Self {
        Self {
            config: config.clone(),
            session_handle,
            conn_handle,
            global_cmd_handle,
            assignment: assignment::State::new(config.into()),
        }
    }

    pub async fn handle_new_connection<ClientRead, ClientWrite>(
        &mut self,
        conn_id: ConnId,
        client_read: ClientRead,
        client_write: ClientWrite,
    ) where
        ClientRead: MsgReader<msg::conn::ConnMsg<msg::ClientMsg>>,
        ClientWrite: MsgWriter,
    {
        self.conn_handle
            .new_conn(conn_id, client_read, client_write)
            .await;
        self.assignment.on_conn_created(conn_id);
    }

    pub async fn handle_session_evt(&mut self, evt: session_host::Event) {
        match evt {
            session_host::Event::SessionEnded(session_id) => {
                self.assignment.on_session_ended(&session_id);
            }
            session_host::Event::ServerMsg(session_id, server_msg) => {
                let actions = self.assignment.new_outgoing_session_msg(
                    &session_id,
                    protocol::msg::ServerMsg::SessionMsg(session_id, server_msg),
                );

                self.handle_assignment_actions(actions).await;
            }
        };
    }

    pub async fn handle_conn_evt(&mut self, evt: conn_host::Event) {
        match evt {
            conn_host::Event::ServerMsgSenderReady(conn_id, sender) => {
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
            conn_host::Event::ClientMsg(conn_id, client_msg) => {
                match client_msg {
                    ClientMsg::SessionMsg(session_id, client_msg) => {
                        if let protocol::msg::session::ClientMsg::Request(_) = &client_msg {
                            let session_client_msg_rx =
                                self.session_handle.new_session(session_id).await;
                            self.assignment
                                .on_new_session(session_id, session_client_msg_rx);
                        }

                        let actions = self
                            .assignment
                            .on_remote_msg_to_session(&conn_id, &session_id, client_msg.into())
                            .await;

                        self.handle_assignment_actions(actions).await;
                    }
                    ClientMsg::GlobalCmd(at_least_once) => {
                        match at_least_once {
                            AtLeastOnce::Ack(seq) => {
                                self.global_cmd_handle.ack(seq).await;
                            }
                            AtLeastOnce::Msg(seq, msg) => {
                                let actions = self.assignment.new_outgoing_global_msg(
                                    protocol::msg::ServerMsg::GlobalCmd(
                                        protocol::msg::AtLeastOnce::Ack(seq),
                                    ),
                                );

                                self.handle_assignment_actions(actions).await;

                                match msg {
                                    protocol::msg::group::ClientCmd::KillSession(session_id) => {
                                        self.assignment.on_session_ended(&session_id);
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
        evt: global_cmd_manager::Event<protocol::msg::group::ServerCmd>,
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
                        .queue(protocol::msg::group::ServerCmd::KillSession(session_id))
                        .await;
                }
                assignment::Action::ConnectMore { expected } => {
                    self.global_cmd_handle
                        .queue(protocol::msg::group::ServerCmd::ConnectMore {
                            expected: expected.try_into().unwrap(),
                        })
                        .await
                }
            };
        }
    }
}

pub async fn run<GreetedRead, GreetedWrite, TConnectTarget>(
    mut new_conn_rx: tokio::sync::mpsc::UnboundedReceiver<(ConnId, GreetedRead, GreetedWrite)>,
    config: Config<TConnectTarget>,
) -> Result<(), std::io::Error>
where
    GreetedRead: MsgReader<msg::conn::ConnMsg<msg::ClientMsg>>,
    GreetedWrite: MsgWriter,
    TConnectTarget: ConnectTarget,
{
    let (buffer_pool_fut, buffer_pool) = buffer_pool::BufferPool::create(buffer_pool::Config {
        buf_size: config.max_packet_size,
        pool_size: config.buf_pool_size,
    });

    let (session_evt_tx, mut session_evt_rx) = mpsc::unbounded::<session_host::Event>();
    let (session_host_fut, session_handle) =
        session_host::create(config.clone().into(), session_evt_tx, buffer_pool);

    let (conn_evt_tx, mut conn_evt_rx) = mpsc::unbounded::<conn_host::Event>();
    let (conn_host_fut, conn_handle) = conn_host::create(conn_evt_tx);

    let (global_cmd_evt_tx, mut global_cmd_evt_rx) = mpsc::unbounded();
    let (global_cmd_fut, global_cmd_handle) = global_cmd_manager::run(global_cmd_evt_tx);

    let mut state = State::new(config, session_handle, conn_handle, global_cmd_handle);

    let main_loop = async move {
        loop {
            tokio::select! {
                conn = new_conn_rx.recv() => {
                    let (conn_id, client_read, client_write) = match conn {
                        Some(x) => x,
                        None => {
                            tracing::warn!("new_conn_rx is broken, exiting");
                            return;
                        },
                    };

                    state.handle_new_connection(conn_id, client_read, client_write).await;
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
        buffer_pool_fut.map(|_| Ok(())),
    }
    .map(|_| ())
}

use futures::channel::mpsc;
use futures::prelude::*;
use tracing::Instrument as _;

use super::{conn_host, session_host};
use crate::handover;
use crate::prelude::*;
use crate::protocol::msg::AtLeastOnce;
use crate::protocol::msg::ClientMsg;
use crate::{assignment, global_cmd_manager};

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
    pub connect_target: TConnectTarget,
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

struct State<TConnectTarget, SessionEvtTx, ConnEvtTx> {
    config: Config<TConnectTarget>,
    session_handle: session_host::Handle<SessionEvtTx, TConnectTarget>,
    conn_handle: conn_host::Handle<ConnEvtTx>,
    global_cmd_handle: global_cmd_manager::Handle<protocol::msg::global_cmd::ServerCmd>,
    assignment: assignment::State<protocol::msg::ServerMsg, protocol::msg::session::ClientMsg>,
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
        global_cmd_handle: global_cmd_manager::Handle<protocol::msg::global_cmd::ServerCmd>,
    ) -> Self {
        Self {
            config,
            session_handle,
            conn_handle,
            global_cmd_handle,
            assignment: assignment::State::new(),
        }
    }

    pub async fn handle_new_connection<ClientRead, ClientWrite>(
        &mut self,
        conn_id: ConnId,
        client_read: ClientRead,
        client_write: ClientWrite,
    ) where
        ClientRead: protocol::MessageReader<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>,
            >,
        ClientWrite: protocol::MessageWriter<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>,
            >,
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
                self.assignment.conn_ready_to_send(&conn_id, sender);
            }
            conn_host::Event::ConnectionErrored(conn_id) => {
                let actions = self.assignment.on_conn_errored(&conn_id);
                self.handle_assignment_actions(actions).await;
            }
            conn_host::Event::ConnectionEnded(conn_id) => {
                self.assignment.on_conn_closed(&conn_id);
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

                        self.assignment
                            .on_msg_to_session(&conn_id, &session_id, client_msg)
                            .await;
                    }
                    ClientMsg::GlobalCmd(at_least_once) => {
                        match at_least_once {
                            AtLeastOnce::Ack(seq) => {
                                self.global_cmd_handle.ack(seq).await;
                            }
                            AtLeastOnce::Msg(seq, msg) => {
                                self.assignment.new_outgoing_global_msg(
                                    protocol::msg::ServerMsg::GlobalCmd(
                                        protocol::msg::AtLeastOnce::Ack(seq),
                                    ),
                                );

                                match msg {
                                    protocol::msg::global_cmd::ClientCmd::KillSession(
                                        session_id,
                                    ) => {
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
        evt: global_cmd_manager::Event<protocol::msg::global_cmd::ServerCmd>,
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
                assignment::Action::UpgradeSession {
                    session_id,
                    conn_count_to_keep,
                } => {
                    self.global_cmd_handle
                        .queue(protocol::msg::global_cmd::ServerCmd::UpgradeSession {
                            session_id,
                            conn_count_to_keep: conn_count_to_keep.try_into().unwrap(),
                        })
                        .await
                }
                assignment::Action::KillSession(session_id) => {
                    self.global_cmd_handle
                        .queue(protocol::msg::global_cmd::ServerCmd::KillSession(
                            session_id,
                        ))
                        .await;
                }
            };
        }
    }
}

pub async fn run<GreetedRead, GreetedWrite, TConnectTarget>(
    mut new_conn_rx: handover::Receiver<(ConnId, GreetedRead, GreetedWrite)>,
    config: Config<TConnectTarget>,
) -> Result<(), std::io::Error>
where
    GreetedRead:
        protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>,
    GreetedWrite:
        protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>,
    TConnectTarget: ConnectTarget,
{
    let (session_evt_tx, mut session_evt_rx) = mpsc::unbounded::<session_host::Event>();
    let (session_host_fut, session_handle) =
        session_host::create(config.clone().into(), session_evt_tx);

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
    }
    .map(|_| ())
}

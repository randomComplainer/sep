use futures::channel::mpsc;
use futures::prelude::*;
use tracing::*;

use crate::handover;
use crate::prelude::*;
use crate::protocol::ConnId;
use crate::protocol::SessionId;

mod conn_manager;
mod session_manager;

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

impl<TConnectTarget> Into<session_manager::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> session_manager::Config<TConnectTarget> {
        session_manager::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
            connect_target: self.connect_target,
        }
    }
}

pub struct State<TConnectTarget> {
    sessions_state: session_manager::State<TConnectTarget>,
    conns_state: conn_manager::State,
}

impl<TConnectTarget> State<TConnectTarget>
where
    TConnectTarget: ConnectTarget,
{
    pub fn new(
        sessions_state: session_manager::State<TConnectTarget>,
        conns_state: conn_manager::State,
    ) -> Self {
        Self {
            sessions_state,
            conns_state,
        }
    }

    pub async fn on_client_conn<ClientRead, ClientWrite>(
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
        self.conns_state
            .on_new_connection(conn_id, client_read, client_write)
            .await
    }

    pub async fn client_msg_to_session(
        &mut self,
        session_id: SessionId,
        msg: session::msg::ClientMsg,
    ) {
        self.sessions_state.on_client_msg(session_id, msg).await
    }

    pub async fn on_session_end(&mut self, session_id: SessionId) {
        self.sessions_state.on_session_end(session_id).await
    }

    pub async fn on_conn_closed(&mut self, conn_id: ConnId) {
        self.conns_state.on_connection_closed(conn_id).await
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
    let (server_msg_sender_tx, server_msg_sender_rx) = async_channel::unbounded();

    let (sessions_evt_tx, mut sessions_evt_rx) = mpsc::unbounded();
    let (conns_evt_tx, mut conns_evt_rx) = mpsc::unbounded();

    let (sessions_state, sessions_task) =
        session_manager::State::new(config.clone().into(), sessions_evt_tx, server_msg_sender_rx);

    let (conns_state, conns_task) = conn_manager::State::new(conns_evt_tx, server_msg_sender_tx);

    let mut state = State::new(sessions_state, conns_state);

    let main_loop = async move {
        loop {
            tokio::select! {
                new_conn = new_conn_rx.recv() => {
                    let new_conn = match new_conn {
                        Some(new_conn) => new_conn,
                        None => {
                            tracing::warn!("conns_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    state.on_client_conn(new_conn.0, new_conn.1, new_conn.2).await;
                },

                sessions_evt = sessions_evt_rx.next() => {
                    let sessions_evt = match sessions_evt {
                        Some(sessions_evt) => sessions_evt,
                        None => {
                            tracing::warn!("sessions_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    match sessions_evt {
                        session_manager::Event::SessionEnded(session_id) =>
                            state.on_session_end(session_id).await ,
                    };
                },

                conns_evt = conns_evt_rx.next() => {
                    let conns_evt = match conns_evt {
                        Some(conns_evt) => conns_evt,
                        None => {
                            tracing::warn!("conns_evt_rx is broken, exiting");
                            return;
                        }
                    };

                    match conns_evt {
                        conn_manager::Event::Closed(conn_id) => {
                            state.on_conn_closed(conn_id).await;
                            if state.conns_state.conn_count() == 0  &&
                                state.sessions_state.active_session_count() == 0
                            {
                                return;
                            }
                        },
                        conn_manager::Event::ClientMsg(client_msg) => {
                            match client_msg {
                                protocol::msg::ClientMsg::SessionMsg(session_id, client_msg) =>
                                    state.client_msg_to_session(session_id, client_msg).await,
                            };
                        },
                    };
                }
            }
        }
    };

    tokio::try_join!(
        sessions_task
            .map(|x| Ok(x.unwrap_never()))
            .instrument(tracing::trace_span!("session manager")),
        conns_task
            .map(|x| Ok(x.unwrap_never()))
            .instrument(tracing::trace_span!("conn manager")),
        main_loop.map(|_| Ok::<_, std::io::Error>(()))
    )
    .map(|_| ())
}

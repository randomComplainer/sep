use futures::prelude::*;
use thiserror::Error;
use tracing::*;

use crate::prelude::*;

use super::{server_connection_lifetime, server_connection_manager, session_manager};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("session protocol error: session id {0}: {1}")]
    SessionProtocol(u16, String),
    #[error("lost connection to server")]
    LostServerConnection,
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
    pub max_server_conn: u16,
}

impl Into<session_manager::Config> for Config {
    fn into(self) -> session_manager::Config {
        session_manager::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
        }
    }
}

impl Into<server_connection_manager::Config> for Config {
    fn into(self) -> server_connection_manager::Config {
        server_connection_manager::Config {
            max_server_conn: self.max_server_conn,
        }
    }
}

// Exits only on server connection io error
// protocol error panics
pub async fn run<ServerConnector>(
    new_proxee_rx: impl Stream<Item = (u16, impl socks5::server_agent::Init)> + Unpin + Send + 'static,
    connect_to_server: ServerConnector,
    config: Config,
) -> std::io::Error
where
    ServerConnector: server_connection_lifetime::ServerConnector + Send,
{
    let (session_evt_tx, mut session_evt_rx) = futures::channel::mpsc::unbounded();

    let (mut session_cmd_tx, session_cmd_rx) =
        futures::channel::mpsc::unbounded::<session_manager::Command>();

    let (server_conn_evt_tx, mut server_conn_evt_rx) = futures::channel::mpsc::unbounded();

    let (mut server_conn_cmd_tx, server_conn_cmd_rx) = futures::channel::mpsc::unbounded();

    let session_manager_task =
        session_manager::run(new_proxee_rx, session_cmd_rx, session_evt_tx, config.into());

    let handle_session_evt = async move {
        while let Some(evt) = session_evt_rx.next().await {
            match evt {
                session_manager::Event::New(session_id) => {
                    server_conn_cmd_tx
                        .send(server_connection_manager::Command::SessionStarted(
                            session_id,
                        ))
                        .await
                        .unwrap();
                }
                session_manager::Event::Ended(session_id) => {
                    server_conn_cmd_tx
                        .send(server_connection_manager::Command::SessionEnded(session_id))
                        .await
                        .unwrap();
                }
                session_manager::Event::ClientMsg(session_id, client_msg) => {
                    server_conn_cmd_tx
                        .send(server_connection_manager::Command::SendClientMsg((
                            session_id, client_msg,
                        )))
                        .await
                        .unwrap();
                }
            }
        }
    };

    let server_conn_task = server_connection_manager::run(
        connect_to_server,
        server_conn_cmd_rx,
        server_conn_evt_tx,
        config.into(),
    );

    let handle_server_conn_evt = async move {
        while let Some(evt) = server_conn_evt_rx.next().await {
            match evt {
                server_connection_manager::Event::ServerMsg((session_id, server_msg)) => {
                    let msg = session_manager::Command::ServerMsg(session_id, server_msg);
                    let span = info_span!("forward server msg to session manager", msg = ?msg);
                    session_cmd_tx.send(msg).instrument(span).await.unwrap();
                }
            }
        }
    };

    tokio::try_join! {
        session_manager_task
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("session_manager")),
        handle_session_evt
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("handle_session_evt")),
        server_conn_task
            .map(|e| Err::<(), _>(e))
            .instrument(info_span!("server_conn_task")),
        handle_server_conn_evt
            .map(|x| Ok::<_, std::io::Error>(x))
            .instrument(info_span!("handle_server_conn_evt")),
    }
    .unwrap_err()
}

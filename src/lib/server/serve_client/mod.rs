use futures::channel::mpsc;
use futures::prelude::*;
use tracing::*;

use crate::handover;
use crate::prelude::*;

mod client_conn_lifetime;
mod conn_manager;
mod session_manager;

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<session::server::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> session::server::Config<TConnectTarget> {
        session::server::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
            connect_target: self.connect_target,
        }
    }
}

impl<TConnectTarget> Into<session_manager::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> session_manager::Config<TConnectTarget> {
        session_manager::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
            connect_target: self.connect_target,
        }
    }
}

pub async fn run<GreetedRead, GreetedWrite, TConnectTarget>(
    new_conn_rx: handover::Receiver<(GreetedRead, GreetedWrite)>,
    config: Config<TConnectTarget>,
) -> Result<(), std::io::Error>
where
    GreetedRead: protocol::server_agent::GreetedRead,
    GreetedWrite: protocol::server_agent::GreetedWrite,
    TConnectTarget: ConnectTarget,
{
    let (session_manager_evt_tx, mut session_manager_evt_rx) =
        mpsc::unbounded::<session_manager::Event>();

    let (mut session_manager_cmd_tx, session_manager_cmd_rx) =
        mpsc::unbounded::<session_manager::Command>();

    let session_manager_task = session_manager::run(
        session_manager_cmd_rx,
        session_manager_evt_tx.clone(),
        config.clone().into(),
    )
    .instrument(info_span!("session manager"));

    let (conn_manager_evt_tx, mut conn_manager_evt_rx) = mpsc::unbounded::<conn_manager::Event>();

    let (mut conn_manager_cmd_tx, conn_manager_cmd_rx) = mpsc::unbounded::<conn_manager::Command>();

    let conn_manager_task = conn_manager::run(
        conn_manager_cmd_rx,
        conn_manager_evt_tx.clone(),
        new_conn_rx,
    )
    .instrument(info_span!("conn manager"));

    let main_loop = {
        async move {
            let mut conn_num = 0;
            let mut session_num = 0;

            macro_rules! check_state {
                () => {
                    debug!(conn_num, session_num, "checking state");
                    if conn_num == 0 && session_num == 0 {
                        debug!("ending state reached");
                        return;
                    }
                };
            }

            loop {
                tokio::select! {
                    e = session_manager_evt_rx.next() => {
                        let e = match e {
                            Some(e) => e,
                            None => {
                                warn!("session manager evt rx is broken, exiting");
                                return;
                            },
                        };

                        match e {
                            session_manager::Event::ServerMsg(session_id, server_msg) => {
                                if let Err(_) = conn_manager_cmd_tx.send(conn_manager::Command::ServerMsg(
                                        (session_id, server_msg).into()
                                )).await {
                                    warn!("conn manager cmd tx is broken, exiting");
                                    return;
                                }
                            },
                            session_manager::Event::Started(_) => {
                                session_num += 1;
                                check_state!();
                            },
                            session_manager::Event::Ended(_) => {
                                session_num -= 1;
                                check_state!();
                            },
                        };
                    },

                    e = conn_manager_evt_rx.next() => {
                        let e = match e {
                            Some(e) => e,
                            None => {
                                warn!("conn manager evt rx is broken, exiting");
                                return;
                            }
                        };

                        match e {
                            conn_manager::Event::ClientMsg(client_msg) => {
                                let (session_id, client_msg) = match client_msg {
                                    protocol::msg::ClientMsg::SessionMsg(session_id, client_msg) => (session_id, client_msg),
                                };

                                if let Err(_) = session_manager_cmd_tx.send(session_manager::Command::ClientMsg(session_id, client_msg))
                                    .await {
                                        warn!("session manager cmd tx is broken, exiting");
                                        return;
                                }
                            },
                            conn_manager::Event::Started => {
                                conn_num += 1;
                                check_state!();
                            },
                            conn_manager::Event::Ended => {
                                conn_num -= 1;
                                check_state!();
                            },
                        };
                    }
                }
            }
        }
    }
    .instrument(info_span!("main loop"));

    // all theses tasks only end when ending state is reached
    // or message channel is broken
    tokio::select! {
        _ = main_loop => Ok(()),
        _ = session_manager_task => Ok(()),
        r = conn_manager_task => r,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    #[test_log::test(tokio::test)]
    async fn happy_path() {
        let (mut new_conn_tx, new_conn_rx) = handover::channel();
        let config = Config {
            max_packet_ahead: 4,
            max_packet_size: 1024,
            connect_target: crate::connect_target::make_mock([(
                (ReadRequestAddr::Domain("example.com".into()), 80),
                Ok((
                    tokio_test::io::Builder::new().build(),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 180),
                )),
            )]),
        };

        let main_task = tokio::spawn(run(new_conn_rx, config));

        let (client_agent, server_agent) = protocol::test_utils::create_greeted_pair().await;
        let server_agent = (server_agent.1, server_agent.2);

        new_conn_tx
            .send(server_agent)
            .await
            .map_err(|_| ())
            .unwrap();

        let (mut client_agent_read, mut client_agent_write) = client_agent;

        client_agent_write
            .send_msg(
                (
                    2,
                    session::msg::Request {
                        addr: decode::ReadRequestAddr::Domain("example.com".into()),
                        port: 80,
                    }
                    .into(),
                )
                    .into(),
            )
            .await
            .unwrap();

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            (
                2,
                session::msg::Reply {
                    bound_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 180),
                }
                .into()
            )
                .into()
        );

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            (2, session::msg::Eof { seq: 0 }.into()).into()
        );

        client_agent_write
            .send_msg((2, session::msg::Ack { seq: 0 }.into()).into())
            .await
            .unwrap();

        client_agent_write
            .send_msg((2, session::msg::Eof { seq: 0 }.into()).into())
            .await
            .unwrap();

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            protocol::msg::ServerMsg::SessionMsg(2, session::msg::Ack { seq: 0 }.into())
        );

        // let connection timeout run out
        tokio::time::pause();

        assert_eq!(
            client_agent_read.recv_msg().await.unwrap().unwrap(),
            protocol::msg::ServerMsg::EndOfStream
        );

        drop(client_agent_read);
        drop(client_agent_write);

        let result = main_task.await;
        assert!(result.unwrap().is_ok());
    }

    // TODO: test more
}

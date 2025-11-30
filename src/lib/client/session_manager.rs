use dashmap::DashMap;
use futures::prelude::*;
use tracing::*;

use crate::prelude::*;

#[derive(Debug)]
pub enum Command {
    ServerMsg(u16, session::msg::ServerMsg),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    New(u16),
    Ended(u16),
    ClientMsg(u16, session::msg::ClientMsg),
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
}

impl Into<session::client::Config> for Config {
    fn into(self) -> session::client::Config {
        session::client::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
        }
    }
}

pub async fn run(
    mut new_proxyee_rx: impl Stream<Item = (u16, impl socks5::server_agent::Init)>
    + Unpin
    + Send
    + 'static,
    mut cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    mut evt_tx: impl Sink<Event, Error = impl std::fmt::Debug> + Clone + Unpin + Send + 'static,
    config: Config,
) {
    let (mut sessions_scope_handle, sessions_scope_task) = task_scope::new_scope::<()>();
    tokio::pin!(sessions_scope_task);

    // relay session ending events so that
    // we don't have to access the hashmap in multiple tasks
    let (local_session_ending_tx, mut local_session_ending_rx) =
        futures::channel::mpsc::channel::<u16>(config.max_packet_ahead as usize);

    let session_server_msg_senders =
        DashMap::<u16, futures::channel::mpsc::Sender<session::msg::ServerMsg>>::new();

    let main_loop = async move {
        loop {
            tokio::select! {
                proxyee = new_proxyee_rx.next() => {
                    // TODO: gracefully shutdown
                    let (session_id, proxyee) = proxyee.unwrap();

                    if session_server_msg_senders.contains_key(&session_id) {
                        panic!("session id {} is duplicated", session_id);
                    }

                    let (session_server_msg_tx, session_server_msg_rx) =
                        futures::channel::mpsc::channel(config.max_packet_ahead as usize);

                    session_server_msg_senders
                        .insert(session_id, session_server_msg_tx);

                    evt_tx.send(Event::New(session_id))
                        .instrument(info_span!("nofity session creation", session_id = session_id))
                        .await.expect("evt_tx is broken");

                    sessions_scope_handle.run_async({
                        let evt_tx = evt_tx.clone();
                        let mut local_session_ending_tx = local_session_ending_tx.clone();

                        async move {
                            let _ = session::client::run(
                                proxyee,
                                session_server_msg_rx,
                                evt_tx.clone().with_sync(move |client_msg| Event::ClientMsg(session_id, client_msg)),
                                config.into(),
                            )
                                .instrument(info_span!("session", session_id = session_id))
                                .await;
                            local_session_ending_tx.send(session_id).await.expect("local_session_ending_tx is broken");
                            Ok(())
                        }
                    }).await;
                },

                proxyee_ending = local_session_ending_rx.next() => {
                    let proxyee_id = proxyee_ending.unwrap();
                    session_server_msg_senders.remove(&proxyee_id);
                    evt_tx.send(Event::Ended(proxyee_id))
                        .instrument(info_span!("nofity session ending", session_id = proxyee_id))
                        .await.expect("evt_tx is broken");
                    },

                    cmd = cmd_rx.next() => {
                        let cmd = match cmd {
                            Some(cmd) => cmd,
                            None => {
                                debug!("end of cmd stream, exiting");
                                break;
                            },
                        };

                        debug!("cmd: {:?}", cmd);

                        match cmd {
                            Command::ServerMsg(session_id, server_msg) => {
                                if let Some(mut session_server_msg_sender) = session_server_msg_senders.get_mut(&session_id) {
                                    let span = info_span!("send server msg to session", session_id = session_id, ?server_msg);
                                    if let Err(_) = session_server_msg_sender.send(server_msg)
                                        .instrument(span)
                                            .await {
                                                debug!("session server msg reciver is dropped, drop server msg");
                                    }
                                } else {
                                    debug!("session does not exist, drop server msg");
                                }
                            },
                        }
                    },
            }
        }
    };

    tokio::select! {
        x = main_loop.instrument(info_span!("main loop")) => {
            x
        },
        _ = sessions_scope_task => {
            unreachable!("session scope task ended unexpectedly");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;

    use bytes::BytesMut;
    use futures::channel::mpsc;

    #[tokio::test]
    async fn happy_path() {
        let (mut new_proxyee_tx, new_proxyee_rx) = mpsc::channel(1);
        let (mut cmd_tx, cmd_rx) = mpsc::channel(1);
        let (evt_tx, mut evt_rx) = mpsc::channel(1);

        let main_task = run(
            new_proxyee_rx,
            cmd_rx,
            evt_tx,
            Config {
                max_packet_ahead: 1024,
                max_packet_size: 1024,
            },
        );

        let main_task = tokio::spawn(main_task);

        let proxyee_stream = tokio_test::io::Builder::new()
            .read(&[1, 2, 3, 4])
            .write(&[4, 3, 2, 1])
            .build();

        let (proxyee_stream_read, proxyee_stream_write) = tokio::io::split(proxyee_stream);

        let proxyee = socks5::server_agent::mock::script()
            .provide_greeting_message(socks5::msg::ClientGreeting {
                ver: 5,
                methods: [0].as_ref().into(),
            })
            .expect_method_selection(0)
            .provide_request_message(socks5::msg::ClientRequest {
                ver: 5,
                cmd: 1,
                rsv: 0,
                addr: ReadRequestAddr::Domain("example.com".into()),
                port: 80,
            })
            .expect_reply(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 1180))
            .provide_stream(BytesMut::new(), proxyee_stream_read, proxyee_stream_write);

        new_proxyee_tx.send((8, proxyee)).await.unwrap();

        assert_eq!(evt_rx.next().await.unwrap(), super::Event::New(8));

        assert_eq!(
            evt_rx.next().await.unwrap(),
            super::Event::ClientMsg(
                8,
                session::msg::Request {
                    addr: ReadRequestAddr::Domain("example.com".into()),
                    port: 80,
                }
                .into()
            )
        );

        cmd_tx
            .send(Command::ServerMsg(
                8,
                session::msg::Reply {
                    bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 1180),
                }
                .into(),
            ))
            .await
            .unwrap();

        assert_eq!(
            evt_rx.next().await.unwrap(),
            super::Event::ClientMsg(
                8,
                session::msg::Data {
                    seq: 0,
                    data: [1, 2, 3, 4].as_ref().into(),
                }
                .into()
            )
        );

        cmd_tx
            .send(Command::ServerMsg(8, session::msg::Ack { seq: 0 }.into()))
            .await
            .unwrap();

        cmd_tx
            .send(Command::ServerMsg(
                8,
                session::msg::Data {
                    seq: 0,
                    data: [4, 3, 2, 1].as_ref().into(),
                }
                .into(),
            ))
            .await
            .unwrap();

        assert_eq!(
            evt_rx.next().await.unwrap(),
            super::Event::ClientMsg(8, session::msg::Ack { seq: 0 }.into())
        );

        cmd_tx
            .send(Command::ServerMsg(8, session::msg::Eof { seq: 1 }.into()))
            .await
            .unwrap();

        assert_eq!(
            evt_rx.next().await.unwrap(),
            super::Event::ClientMsg(8, session::msg::Eof { seq: 1 }.into())
        );

        assert_eq!(
            evt_rx.next().await.unwrap(),
            super::Event::ClientMsg(8, session::msg::Ack { seq: 1 }.into())
        );

        cmd_tx
            .send(Command::ServerMsg(8, session::msg::Ack { seq: 1 }.into()))
            .await
            .unwrap();

        // Trust that Ended event means that the proxyee has been dropped, for now.
        assert_eq!(evt_rx.next().await.unwrap(), super::Event::Ended(8));

        drop(cmd_tx);
        assert!(evt_rx.next().await.is_none());
        main_task.await.unwrap();
    }

    #[tokio::test]
    async fn end_sssion_on_proxyee_io_error() {
        let (mut new_proxyee_tx, new_proxyee_rx) = mpsc::channel(1);
        let (mut cmd_tx, cmd_rx) = mpsc::channel(1);
        let (evt_tx, mut evt_rx) = mpsc::channel(1);

        let main_task = run(
            new_proxyee_rx,
            cmd_rx,
            evt_tx,
            Config {
                max_packet_ahead: 1024,
                max_packet_size: 1024,
            },
        );

        let main_task = tokio::spawn(main_task);

        let proxyee_stream = tokio_test::io::Builder::new()
            .read_error(std::io::Error::new(
                std::io::ErrorKind::Other,
                "proxyee read error",
            ))
            .build();

        let (proxyee_stream_read, proxyee_stream_write) = tokio::io::split(proxyee_stream);

        let proxyee = socks5::server_agent::mock::script()
            .provide_greeting_message(socks5::msg::ClientGreeting {
                ver: 5,
                methods: [0].as_ref().into(),
            })
            .expect_method_selection(0)
            .provide_request_message(socks5::msg::ClientRequest {
                ver: 5,
                cmd: 1,
                rsv: 0,
                addr: ReadRequestAddr::Domain("example.com".into()),
                port: 80,
            })
            .expect_reply(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 1180))
            .provide_stream(BytesMut::new(), proxyee_stream_read, proxyee_stream_write);

        new_proxyee_tx.send((8, proxyee)).await.unwrap();

        assert_eq!(evt_rx.next().await.unwrap(), super::Event::New(8));

        assert_eq!(
            evt_rx.next().await.unwrap(),
            super::Event::ClientMsg(
                8,
                session::msg::Request {
                    addr: ReadRequestAddr::Domain("example.com".into()),
                    port: 80,
                }
                .into()
            )
        );

        cmd_tx
            .send(Command::ServerMsg(
                8,
                session::msg::Reply {
                    bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 1180),
                }
                .into(),
            ))
            .await
            .unwrap();

        assert_eq!(
            evt_rx.next().await.unwrap(),
            super::Event::ClientMsg(8, session::msg::IoError.into())
        );

        assert_eq!(evt_rx.next().await.unwrap(), super::Event::Ended(8));
        main_task.abort();
    }

    #[tokio::test]
    async fn panic_on_duplicated_session_id() {
        let (mut new_proxyee_tx, new_proxyee_rx) = mpsc::channel(1);
        let (mut cmd_tx, cmd_rx) = mpsc::channel(1);
        let (evt_tx, mut evt_rx) = mpsc::channel(1);

        let main_task = run(
            new_proxyee_rx,
            cmd_rx,
            evt_tx,
            Config {
                max_packet_ahead: 1024,
                max_packet_size: 1024,
            },
        );

        let main_task = tokio::spawn(main_task);

        let create_proxyee = || {
            let stream = tokio_test::io::Builder::new().build();
            let (stream_read, stream_write) = tokio::io::split(stream);

            socks5::server_agent::mock::script()
                .provide_greeting_message(socks5::msg::ClientGreeting {
                    ver: 5,
                    methods: [0].as_ref().into(),
                })
                .expect_method_selection(0)
                .provide_request_message(socks5::msg::ClientRequest {
                    ver: 5,
                    cmd: 1,
                    rsv: 0,
                    addr: ReadRequestAddr::Domain("example.com".into()),
                    port: 80,
                })
                .expect_reply(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 1180))
                .provide_stream(BytesMut::new(), stream_read, stream_write)
        };

        new_proxyee_tx.send((8, create_proxyee())).await.unwrap();
        new_proxyee_tx.send((8, create_proxyee())).await.unwrap();

        let result = main_task.await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn reuse_session_id_after_previous_session_ended() {
        let (mut new_proxyee_tx, new_proxyee_rx) = mpsc::channel(1);
        let (mut cmd_tx, cmd_rx) = mpsc::channel(1);
        let (evt_tx, mut evt_rx) = mpsc::channel(1);

        let main_task = run(
            new_proxyee_rx,
            cmd_rx,
            evt_tx,
            Config {
                max_packet_ahead: 1024,
                max_packet_size: 1024,
            },
        );

        tokio::spawn(main_task);

        let create_proxyee = || {
            let stream = tokio_test::io::Builder::new().build();
            let (stream_read, stream_write) = tokio::io::split(stream);

            socks5::server_agent::mock::script()
                .provide_greeting_message(socks5::msg::ClientGreeting {
                    ver: 5,
                    methods: [0].as_ref().into(),
                })
                .expect_method_selection(0)
                .provide_request_message(socks5::msg::ClientRequest {
                    ver: 5,
                    cmd: 1,
                    rsv: 0,
                    addr: ReadRequestAddr::Domain("example.com".into()),
                    port: 80,
                })
                .expect_reply(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 1180))
                .provide_stream(BytesMut::new(), stream_read, stream_write)
        };

        for _ in 0..5 {
            new_proxyee_tx.send((8, create_proxyee())).await.unwrap();
            assert_eq!(evt_rx.next().await.unwrap(), super::Event::New(8));

            assert_eq!(
                evt_rx.next().await.unwrap(),
                super::Event::ClientMsg(
                    8,
                    session::msg::Request {
                        addr: ReadRequestAddr::Domain("example.com".into()),
                        port: 80,
                    }
                    .into()
                )
            );

            cmd_tx
                .send(Command::ServerMsg(
                    8,
                    session::msg::Reply {
                        bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 1180),
                    }
                    .into(),
                ))
                .await
                .unwrap();

            assert_eq!(
                evt_rx.next().await.unwrap(),
                super::Event::ClientMsg(8, session::msg::Eof { seq: 0 }.into())
            );

            cmd_tx
                .send(Command::ServerMsg(8, session::msg::Ack { seq: 0 }.into()))
                .await
                .unwrap();

            cmd_tx
                .send(Command::ServerMsg(8, session::msg::Eof { seq: 0 }.into()))
                .await
                .unwrap();

            assert_eq!(
                evt_rx.next().await.unwrap(),
                super::Event::ClientMsg(8, session::msg::Ack { seq: 0 }.into())
            );

            assert_eq!(evt_rx.next().await.unwrap(), super::Event::Ended(8));
        }
    }
}

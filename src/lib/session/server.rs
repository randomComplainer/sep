use futures::StreamExt;
use futures::prelude::*;
use tracing::*;

use super::msg;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<session::sequenced_to_stream::Config> for Config<TConnectTarget> {
    fn into(self) -> session::sequenced_to_stream::Config {
        session::sequenced_to_stream::Config {
            max_packet_ahead: self.max_packet_ahead,
        }
    }
}

impl<TConnectTarget> Into<session::stream_to_sequenced::Config> for Config<TConnectTarget> {
    fn into(self) -> session::stream_to_sequenced::Config {
        session::stream_to_sequenced::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
        }
    }
}

pub async fn run<TConnectTarget>(
    mut client_msg_read: impl Stream<Item = msg::ClientMsg> + Unpin,
    mut server_msg_write: impl Sink<msg::ServerMsg, Error = futures::channel::mpsc::SendError>
    + Unpin
    + Clone
    + Send
    + 'static,
    config: Config<TConnectTarget>,
) -> Result<(), std::io::Error>
where
    TConnectTarget: ConnectTarget,
{
    let req = match client_msg_read
        .next()
        .instrument(info_span!("receive request from client"))
        .await
    {
        Some(msg) => match msg {
            msg::ClientMsg::Request(msg) => msg,
            _ => {
                panic!("unexpected client msg: [{:?}], expected request", msg);
            }
        },
        None => {
            error!("client read is broken, exiting");
            return Ok(());
        }
    };

    info!(
        "request received, addr = {:?}, port = {}",
        req.addr, req.port
    );

    let (target_stream, local_addr) = match config
        .connect_target
        .connect(req.addr, req.port)
        .instrument(info_span!("conneet to target"))
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            error!("connect target failed: {:?}", err);

            match server_msg_write
                .send(msg::ServerMsg::ReplyError(err.into()))
                .instrument(info_span!("send reply error to client"))
                .await
            {
                Ok(_) => {
                    return Ok(());
                }
                Err(err) => {
                    error!("falied to send reply error to client: {:?}", err);
                    return Ok(());
                }
            }
        }
    };

    if let Err(_) = server_msg_write
        .send(msg::ServerMsg::Reply(msg::Reply {
            bound_addr: local_addr,
        }))
        .instrument(info_span!("send reply to client"))
        .await
    {
        error!("falied to send reply to client, exiting");
        return Ok(());
    };

    let (target_read, target_write) = tokio::io::split(target_stream);

    let (mut target_to_client_cmd_tx, cmd_target_to_client_cmd_rx) =
        futures::channel::mpsc::channel(1);

    let target_to_client = session::stream_to_sequenced::run(
        cmd_target_to_client_cmd_rx,
        server_msg_write.clone().with_sync(|evt| match evt {
            session::stream_to_sequenced::Event::Data(data) => data.into(),
            session::stream_to_sequenced::Event::Eof(eof) => eof.into(),
            session::stream_to_sequenced::Event::IoError(err) => err.into(),
        }),
        target_read,
        None,
        config.clone().into(),
    )
    .instrument_with_result(info_span!("target to client"));

    let (mut client_to_target_cmd_tx, client_to_target_cmd_rx) = futures::channel::mpsc::channel(1);
    let client_to_target = session::sequenced_to_stream::run(
        client_to_target_cmd_rx,
        server_msg_write.clone().with_sync(|evt| match evt {
            session::sequenced_to_stream::Event::Ack(ack) => ack.into(),
        }),
        target_write,
        config.into(),
    )
    .instrument_with_result(info_span!("client to target"));

    let client_msg_handling = async move {
        while let Some(msg) = client_msg_read.next().await {
            match msg {
                msg::ClientMsg::Data(data) => {
                    if let Err(_) = client_to_target_cmd_tx.send(data.into()).await {
                        debug!("client to target command channel is broken");
                        continue;
                    }
                }
                msg::ClientMsg::Ack(ack) => {
                    if let Err(_) = target_to_client_cmd_tx.send(ack.into()).await {
                        debug!("target to client command channel is broken");
                        continue;
                    }
                }
                msg::ClientMsg::Eof(eof) => {
                    if let Err(_) = client_to_target_cmd_tx.send(eof.into()).await {
                        debug!("client to target command channel is broken");
                        continue;
                    }
                }
                msg::ClientMsg::ProxyeeIoError(_) => {
                    return;
                }
                _ => panic!("unexpected client msg: {:?}", msg),
            }
        }
    }
    .instrument_with_result(info_span!("client message handling"));

    let streaming = async move { tokio::try_join!(target_to_client, client_to_target).map(|_| ()) };

    tokio::select! {
        r = streaming => r,
        _ = client_msg_handling => Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use bytes::BytesMut;
    use futures::channel::mpsc;

    use super::*;

    struct ServerMessageExpectation {
        pub bound_addr: SocketAddr,
        pub ack_till: u16,
        pub data_sequence: Vec<BytesMut>,
        pub io_error: bool,
    }

    type Expectation = ServerMessageExpectation;

    trait ServerMessageExpectationExt {
        fn expect(
            self,
            exp: ServerMessageExpectation,
            client_msg_tx: impl Sink<msg::ClientMsg, Error = impl std::fmt::Debug> + Unpin,
        ) -> impl Future<Output = ()>;
    }

    impl<T> ServerMessageExpectationExt for T
    where
        T: Stream<Item = msg::ServerMsg> + Unpin,
    {
        fn expect(
            mut self,
            exp: ServerMessageExpectation,
            mut client_msg_tx: impl Sink<msg::ClientMsg, Error = impl std::fmt::Debug> + Unpin,
        ) -> impl Future<Output = ()> {
            let mut next_ack = 0;
            let mut next_packate_seq = 0;
            let mut error_received = false;

            async move {
                let msg::ServerMsg::Reply(msg::Reply { bound_addr }) = self.next().await.unwrap()
                else {
                    panic!("unexpected reply");
                };

                assert_eq!(bound_addr, exp.bound_addr);

                while let Some(msg) = self.next().await {
                    match msg {
                        msg::ServerMsg::Reply(_) => {
                            panic!("unexpected reply");
                        }
                        msg::ServerMsg::ReplyError(_) => {
                            panic!("unexpected reply error");
                        }
                        msg::ServerMsg::Data(data) => {
                            assert_eq!(next_packate_seq, data.seq);
                            assert_eq!(exp.data_sequence[next_packate_seq as usize], data.data);
                            client_msg_tx
                                .send(msg::Ack { seq: data.seq }.into())
                                .await
                                .unwrap();
                            next_packate_seq += 1;
                        }
                        msg::ServerMsg::Eof(eof) => {
                            assert_eq!(next_packate_seq, eof.seq);
                            assert_eq!(eof.seq, exp.data_sequence.len() as u16);
                            client_msg_tx
                                .send(msg::Ack { seq: eof.seq }.into())
                                .await
                                .unwrap();
                            next_packate_seq += 1;
                        }
                        msg::ServerMsg::Ack(ack) => {
                            assert_eq!(next_ack, ack.seq);
                            assert!(ack.seq <= exp.ack_till);
                            next_ack += 1;
                        }
                        msg::ServerMsg::TargetIoError(_) => {
                            assert_eq!(next_packate_seq, exp.data_sequence.len() as u16);
                            assert!(exp.io_error);
                            error_received = true;
                        }
                    }
                }

                // last data packate = exp.data_sequence.len() - 1
                // eof  = exp.data_sequence.len()
                // next seq = exp.data_sequence.len() + 1
                // skip eof when io_error
                assert_eq!(
                    next_packate_seq,
                    exp.data_sequence.len() as u16 + (if exp.io_error { 0 } else { 1 })
                );
                assert_eq!(next_ack, exp.ack_till + (if exp.io_error { 0 } else { 1 }));
                assert_eq!(exp.io_error, error_received);
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn happy_path() {
        let (client_msg_tx, client_msg_rx) = mpsc::channel(1);
        let (server_msg_tx, server_msg_rx) = mpsc::channel(1);

        let main_task = run(
            client_msg_rx,
            server_msg_tx,
            Config {
                max_packet_ahead: 1024,
                max_packet_size: 1024,
                connect_target: crate::connect_target::make_mock([(
                    (ReadRequestAddr::Domain("example.com".into()), 8080),
                    Ok((
                        // target_stream,
                        tokio_test::io::Builder::new()
                            .read(&[1, 2, 3, 4])
                            .write(&[4, 3, 2, 1])
                            .build(),
                        SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
                    )),
                )]),
            },
        );

        let verify_server_msg = server_msg_rx.expect(
            Expectation {
                bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
                ack_till: 1,
                data_sequence: vec![[1, 2, 3, 4].as_ref().into()],
                io_error: false,
            },
            client_msg_tx.clone(),
        );

        let client_task = {
            let mut client_msg_tx = client_msg_tx.clone();
            async move {
                client_msg_tx
                    .send(msg::ClientMsg::Request(msg::Request {
                        addr: ReadRequestAddr::Domain("example.com".into()),
                        port: 8080,
                    }))
                    .await
                    .unwrap();

                client_msg_tx
                    .send(
                        msg::Data {
                            seq: 0,
                            data: [4, 3, 2, 1].as_ref().into(),
                        }
                        .into(),
                    )
                    .await
                    .unwrap();

                client_msg_tx
                    .send(msg::Eof { seq: 1 }.into())
                    .await
                    .unwrap();
            }
        };

        tokio::join!(
            main_task.map(|r| r.unwrap()),
            verify_server_msg,
            client_task
        );

        drop(client_msg_tx);
    }

    #[cfg(test)]
    mod interact_with_target {
        use super::*;

        #[tokio::test]
        async fn reply_target_conn_error() {
            let (mut client_msg_tx, client_msg_rx) = mpsc::channel(1);
            let (server_msg_tx, mut server_msg_rx) = mpsc::channel(1);
            let main_task = run(
                client_msg_rx,
                server_msg_tx,
                Config {
                    max_packet_ahead: 1024,
                    max_packet_size: 1024,
                    connect_target: crate::connect_target::make_mock::<tokio_test::io::Mock>([(
                        (ReadRequestAddr::Domain("example.com".into()), 80),
                        Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "not found",
                        )),
                    )]),
                },
            );

            let operate_task = async move {
                client_msg_tx
                    .send(msg::ClientMsg::Request(msg::Request {
                        addr: ReadRequestAddr::Domain("example.com".into()),
                        port: 80,
                    }))
                    .await
                    .unwrap();

                let _ = match server_msg_rx.next().await.unwrap() {
                    msg::ServerMsg::ReplyError(e) => e,
                    x => panic!("unexpected server msg: {:?}", x),
                };

                drop(client_msg_tx);

                assert!(server_msg_rx.next().await.is_none());
            };

            tokio::join!(main_task.map(|r| r.unwrap()), operate_task);
        }

        #[tokio::test]
        async fn forawrd_proxyee_io_error_during_streaming() {
            let (client_msg_tx, client_msg_rx) = mpsc::channel(1);
            let (server_msg_tx, mut server_msg_rx) = mpsc::channel(1);

            let main_task = run(
                client_msg_rx,
                server_msg_tx,
                Config {
                    max_packet_ahead: 1024,
                    max_packet_size: 1024,
                    connect_target: crate::connect_target::make_mock([(
                        (ReadRequestAddr::Domain("example.com".into()), 80),
                        Ok((
                            tokio_test::io::Builder::new().build(),
                            SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
                        )),
                    )]),
                },
            );

            let operate_task = {
                let mut client_msg_tx = client_msg_tx.clone();
                async move {
                    client_msg_tx
                        .send(msg::ClientMsg::Request(msg::Request {
                            addr: ReadRequestAddr::Domain("example.com".into()),
                            port: 80,
                        }))
                        .await
                        .unwrap();

                    assert_eq!(
                        server_msg_rx.next().await.unwrap(),
                        msg::ServerMsg::Reply(msg::Reply {
                            bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999)
                        })
                    );

                    client_msg_tx.send(msg::IoError.into()).await.unwrap();
                }
            };

            tokio::join!(main_task.map(|r| r.unwrap()), operate_task);
            drop(client_msg_tx);
        }
    }

    mod interact_with_client {
        use super::*;

        #[tokio::test]
        async fn reply_error() {
            let (mut client_msg_tx, client_msg_rx) = mpsc::channel(1);
            let (server_msg_tx, mut server_msg_rx) = mpsc::channel(1);

            let main_task = run(
                client_msg_rx,
                server_msg_tx,
                Config {
                    max_packet_ahead: 1024,
                    max_packet_size: 1024,
                    connect_target: crate::connect_target::make_mock::<tokio_test::io::Mock>([(
                        (ReadRequestAddr::Domain("example.com".into()), 80),
                        Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "not found",
                        )),
                    )]),
                },
            );

            let operate_task = async move {
                client_msg_tx
                    .send(msg::ClientMsg::Request(msg::Request {
                        addr: ReadRequestAddr::Domain("example.com".into()),
                        port: 80,
                    }))
                    .await
                    .unwrap();

                assert_eq!(
                    server_msg_rx.next().await.unwrap(),
                    msg::ConnectionError::General.into()
                );

                assert!(server_msg_rx.next().await.is_none());
            };

            tokio::join!(main_task.map(|r| r.unwrap()), operate_task);
        }

        #[tokio::test]
        async fn forward_target_io_error_during_streaming() {
            let (client_msg_tx, client_msg_rx) = mpsc::channel(1);
            let (server_msg_tx, server_msg_rx) = mpsc::channel(1);

            let main_task = run(
                client_msg_rx,
                server_msg_tx,
                Config {
                    max_packet_ahead: 1024,
                    max_packet_size: 1024,
                    connect_target: crate::connect_target::make_mock([(
                        (ReadRequestAddr::Domain("example.com".into()), 80),
                        Ok((
                            tokio_test::io::Builder::new()
                                .read_error(std::io::Error::new(std::io::ErrorKind::Other, "other"))
                                .build(),
                            SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
                        )),
                    )]),
                },
            );

            let verify_server_msg = server_msg_rx.expect(
                Expectation {
                    bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
                    ack_till: 0,
                    data_sequence: vec![],
                    io_error: true,
                },
                client_msg_tx.clone(),
            );

            let operate_task = {
                let mut client_msg_tx = client_msg_tx.clone();
                async move {
                    client_msg_tx
                        .send(msg::ClientMsg::Request(msg::Request {
                            addr: ReadRequestAddr::Domain("example.com".into()),
                            port: 80,
                        }))
                        .await
                        .unwrap();
                }
            };

            tokio::join!(
                main_task.map(|r| r.unwrap_err()),
                verify_server_msg,
                operate_task
            );
        }
    }
}

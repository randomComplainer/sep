#![feature(assert_matches)]
use std::net::SocketAddr;

use sep_lib::prelude::*;

use protocol::test_utils::create_greeted_pair as create_pair;

#[tokio::test]
async fn client_req_v4() {
    let req_ip = std::net::Ipv4Addr::new(129, 0, 0, 1);
    let req_port = 1234;

    let ((_client_read, mut client_write), (_, mut server_read, _server_write)) =
        create_pair().await;

    let client = async move {
        client_write
            .send_msg(
                session::msg::ClientMsg::Request(
                    session::msg::Request {
                        addr: decode::ReadRequestAddr::Ipv4(req_ip),
                        port: req_port,
                    }
                    .into(),
                )
                .with_session_id(0)
                .into(),
            )
            .await?;

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        let msg = server_read.recv_msg().await.unwrap().unwrap();
        match msg {
            protocol::msg::conn::ClientMsg::Protocol(protocol::msg::ClientMsg::SessionMsg(
                proxyee_id,
                session::msg::ClientMsg::Request(session::msg::Request {
                    addr: decode::ReadRequestAddr::Ipv4(addr),
                    port,
                }),
            )) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(addr, req_ip);
                assert_eq!(port, req_port);
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_req_v6() {
    let req_ip = std::net::Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x1);
    let req_port = 1234;

    let ((_client_read, mut client_write), (_, mut server_read, _server_write)) =
        create_pair().await;

    let client = async move {
        client_write
            .send_msg(
                session::msg::ClientMsg::Request(
                    session::msg::Request {
                        addr: decode::ReadRequestAddr::Ipv6(req_ip),
                        port: req_port,
                    }
                    .into(),
                )
                .with_session_id(0)
                .into(),
            )
            .await?;

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        let msg = server_read.recv_msg().await.unwrap().unwrap();
        match msg {
            protocol::msg::conn::ClientMsg::Protocol(protocol::msg::ClientMsg::SessionMsg(
                proxyee_id,
                session::msg::ClientMsg::Request(session::msg::Request {
                    addr: decode::ReadRequestAddr::Ipv6(addr),
                    port,
                }),
            )) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(addr, req_ip);
                assert_eq!(port, req_port);
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_req_domain() {
    let req_domain = "example.com";
    let req_port = 1234;

    let ((_client_read, mut client_write), (_, mut server_read, _server_write)) =
        create_pair().await;

    let client = async move {
        client_write
            .send_msg(
                session::msg::ClientMsg::Request(session::msg::Request {
                    addr: decode::ReadRequestAddr::Domain(req_domain.into()),
                    port: req_port,
                })
                .with_session_id(0)
                .into(),
            )
            .await?;

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        let msg = server_read.recv_msg().await.unwrap().unwrap();
        match msg {
            protocol::msg::conn::ClientMsg::Protocol(protocol::msg::ClientMsg::SessionMsg(
                proxyee_id,
                session::msg::ClientMsg::Request(session::msg::Request {
                    addr: decode::ReadRequestAddr::Domain(addr),
                    port,
                }),
            )) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(addr, req_domain);
                assert_eq!(port, req_port);
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn server_reply_v4() {
    let reply_ip = std::net::Ipv4Addr::new(129, 0, 0, 1);
    let reply_port = 1234;

    let ((mut client_read, _client_write), (_, _server_read, mut server_write)) =
        create_pair().await;

    let client = async move {
        let msg = client_read.recv_msg().await.unwrap().unwrap();

        match msg {
            protocol::msg::conn::ServerMsg::Protocol(protocol::msg::ServerMsg::SessionMsg(
                proxyee_id,
                session::msg::ServerMsg::Reply(session::msg::Reply {
                    bound_addr: recv_addr,
                }),
            )) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(recv_addr, SocketAddr::new(reply_ip.into(), reply_port));
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        server_write
            .send_msg(
                session::msg::ServerMsg::Reply(session::msg::Reply {
                    bound_addr: SocketAddr::new(reply_ip.into(), reply_port),
                })
                .with_session_id(0)
                .into(),
            )
            .await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_data() {
    let data = vec![0x01, 0x02, 0x03, 0x04].into_boxed_slice();

    let ((_client_read, mut client_write), (_, mut server_read, _server_write)) =
        create_pair().await;

    let client = {
        let data = data.clone();
        async move {
            client_write
                .send_msg(
                    session::msg::ClientMsg::Data(session::msg::Data {
                        seq: 1,
                        data: bytes::BytesMut::from(data.as_ref()),
                    })
                    .with_session_id(0)
                    .into(),
                )
                .await?;

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        let msg = server_read.recv_msg().await.unwrap().unwrap();
        match msg {
            protocol::msg::conn::ClientMsg::Protocol(protocol::msg::ClientMsg::SessionMsg(
                proxyee_id,
                session::msg::ClientMsg::Data(session::msg::Data {
                    seq,
                    data: recv_data,
                }),
            )) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(seq, 1);
                assert_eq!(recv_data.as_ref(), data.as_ref());
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn server_data() {
    let data = vec![0x01, 0x02, 0x03, 0x04].into_boxed_slice();
    let ((mut client_read, _client_write), (_, _server_read, mut server_write)) =
        create_pair().await;

    let client = {
        let data = data.clone();
        async move {
            let msg = client_read.recv_msg().await.unwrap().unwrap();

            match msg {
                protocol::msg::conn::ServerMsg::Protocol(protocol::msg::ServerMsg::SessionMsg(
                    proxyee_id,
                    session::msg::ServerMsg::Data(session::msg::Data {
                        seq,
                        data: recv_data,
                    }),
                )) => {
                    assert_eq!(proxyee_id, 0);
                    assert_eq!(seq, 1);
                    assert_eq!(recv_data.as_ref(), data.as_ref());
                }
                _ => panic!("unexpected msg"),
            };

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        server_write
            .send_msg(
                session::msg::ServerMsg::Data(session::msg::Data {
                    seq: 1,
                    data: bytes::BytesMut::from(data.as_ref()),
                })
                .with_session_id(0)
                .into(),
            )
            .await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_ack() {
    let ((_client_read, mut client_write), (_, mut server_read, _server_write)) =
        create_pair().await;

    let client = {
        async move {
            client_write
                .send_msg(
                    session::msg::ClientMsg::Ack(session::msg::Ack { bytes: 4 })
                        .with_session_id(1)
                        .into(),
                )
                .await?;

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        let msg = server_read.recv_msg().await.unwrap().unwrap();
        match msg {
            protocol::msg::conn::ClientMsg::Protocol(protocol::msg::ClientMsg::SessionMsg(
                proxyee_id,
                session::msg::ClientMsg::Ack(session::msg::Ack { bytes }),
            )) => {
                assert_eq!(proxyee_id, 1);
                assert_eq!(bytes, 4);
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn server_ack() {
    let ((mut client_read, _client_write), (_, _server_read, mut server_write)) =
        create_pair().await;

    let client = {
        async move {
            let msg = client_read.recv_msg().await.unwrap().unwrap();
            match msg {
                protocol::msg::conn::ServerMsg::Protocol(protocol::msg::ServerMsg::SessionMsg(
                    proxyee_id,
                    session::msg::ServerMsg::Ack(session::msg::Ack { bytes }),
                )) => {
                    assert_eq!(proxyee_id, 0);
                    assert_eq!(bytes, 4);
                }
                _ => panic!("unexpected msg"),
            };

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        server_write
            .send_msg(
                session::msg::ServerMsg::Ack(session::msg::Ack { bytes: 4 })
                    .with_session_id(0)
                    .into(),
            )
            .await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_eof() {
    let ((_client_read, mut client_write), (_, mut server_read, _server_write)) =
        create_pair().await;

    let client = {
        async move {
            client_write
                .send_msg(
                    session::msg::ClientMsg::Eof(session::msg::Eof { seq: 5 })
                        .with_session_id(0)
                        .into(),
                )
                .await?;

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        let msg = server_read.recv_msg().await.unwrap().unwrap();
        match msg {
            protocol::msg::conn::ClientMsg::Protocol(protocol::msg::ClientMsg::SessionMsg(
                proxyee_id,
                session::msg::ClientMsg::Eof(session::msg::Eof { seq }),
            )) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(seq, 5);
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn server_eof() {
    let ((mut client_read, _client_write), (_, _server_read, mut server_write)) =
        create_pair().await;

    let client = {
        async move {
            let msg = client_read.recv_msg().await.unwrap().unwrap();
            match msg {
                protocol::msg::conn::ServerMsg::Protocol(protocol::msg::ServerMsg::SessionMsg(
                    proxyee_id,
                    session::msg::ServerMsg::Eof(session::msg::Eof { seq }),
                )) => {
                    assert_eq!(proxyee_id, 0);
                    assert_eq!(seq, 5);
                }
                _ => panic!("unexpected msg"),
            };

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        server_write
            .send_msg(
                session::msg::ServerMsg::Eof(session::msg::Eof { seq: 5 })
                    .with_session_id(0)
                    .into(),
            )
            .await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

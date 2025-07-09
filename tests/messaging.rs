#![feature(assert_matches)]
use std::net::SocketAddr;
use std::sync::Arc;

use chacha20::ChaCha20;
use tokio::io::{DuplexStream, duplex};

use protocol::{client_agent, server_agent};
use sep_lib::prelude::*;

async fn create_pair() -> (
    (
        client_agent::GreetedRead<DuplexStream, ChaCha20>,
        client_agent::GreetedWrite<DuplexStream, ChaCha20>,
    ),
    (
        server_agent::GreetedRead<DuplexStream, ChaCha20>,
        server_agent::GreetedWrite<DuplexStream, ChaCha20>,
    ),
) {
    let key: Arc<protocol::Key> = protocol::key_from_string("000").into();
    let nonce: Box<protocol::Nonce> = vec![1u8; 12].try_into().unwrap();

    let (client_steam, server_stream) = duplex(8 * 1024);

    let client_agent = protocol::client_agent::Init::new(key.clone(), nonce, client_steam);
    let server_agent = protocol::server_agent::Init::new(key, server_stream);

    let client_agent = client_agent.send_greeting(12).await.unwrap();
    let server_agent = server_agent.recv_greeting(12).await.unwrap();

    (client_agent, server_agent)
}

fn ipv4_to_bytes(addr: std::net::Ipv4Addr, port: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + 2);
    buf.push(0x01);
    buf.extend_from_slice(&addr.octets());
    buf.extend_from_slice(&port.to_be_bytes());
    buf
}

fn domain_to_bytes(domain: &str, port: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 1 + domain.len() + 2);
    buf.push(0x03);
    buf.push(domain.len() as u8);
    buf.extend_from_slice(domain.as_bytes());
    buf.extend_from_slice(&port.to_be_bytes());
    buf
}

#[tokio::test]
async fn client_req_v4() {
    let req_ip = std::net::Ipv4Addr::new(129, 0, 0, 1);
    let req_port = 1234;

    let ((_client_read, mut client_write), (mut server_read, _server_write)) = create_pair().await;

    let client = async move {
        let mut req_buf = ipv4_to_bytes(req_ip, req_port);
        client_write.send_request(0, &mut req_buf).await?;

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        let msg = server_read.recv_msg().await?;
        match msg {
            server_agent::ClientMsg::Request(server_agent::msg::Request {
                proxyee_id,
                addr: decode::ReadRequestAddr::Ipv4(addr),
                port,
            }) => {
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

    let ((_client_read, mut client_write), (mut server_read, _server_write)) = create_pair().await;

    let client = async move {
        let mut req_buf = domain_to_bytes(req_domain, req_port);
        client_write.send_request(0, &mut req_buf).await?;

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        let msg = server_read.recv_msg().await?;
        match msg {
            server_agent::ClientMsg::Request(server_agent::msg::Request {
                proxyee_id,
                addr: decode::ReadRequestAddr::Domain(addr),
                port,
            }) => {
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

    let ((mut client_read, _client_write), (_server_read, mut server_write)) = create_pair().await;

    let client = async move {
        let msg = client_read.recv_msg().await?;

        match msg {
            client_agent::ServerMsg::Reply(client_agent::msg::Reply {
                proxyee_id,
                bound_addr: recv_addr,
            }) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(recv_addr, SocketAddr::new(reply_ip.into(), reply_port));
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        server_write
            .send_reply(0, SocketAddr::new(reply_ip.into(), reply_port))
            .await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_data() {
    let data = vec![0x01, 0x02, 0x03, 0x04].into_boxed_slice();

    let ((_client_read, mut client_write), (mut server_read, _server_write)) = create_pair().await;

    let client = {
        let data = data.clone();
        async move {
            client_write.send_data(0, 1, &mut data.clone()).await?;

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        let msg = server_read.recv_msg().await?;
        match msg {
            server_agent::ClientMsg::Data(server_agent::msg::Data {
                proxyee_id,
                seq,
                data: recv_data,
            }) => {
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
    let ((mut client_read, _client_write), (_server_read, mut server_write)) = create_pair().await;

    let client = {
        let data = data.clone();
        async move {
            let msg = client_read.recv_msg().await?;

            match msg {
                client_agent::ServerMsg::Data(client_agent::msg::Data {
                    proxyee_id,
                    seq,
                    data: recv_data,
                }) => {
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
        server_write.send_data(0, 1, &mut data.clone()).await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_ack() {
    let ((_client_read, mut client_write), (mut server_read, _server_write)) = create_pair().await;

    let client = {
        async move {
            client_write.send_ack(0, 4).await?;

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        let msg = server_read.recv_msg().await?;
        match msg {
            server_agent::ClientMsg::Ack(server_agent::msg::Ack { proxyee_id, seq }) => {
                assert_eq!(proxyee_id, 0);
                assert_eq!(seq, 4);
            }
            _ => panic!("unexpected msg"),
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn server_ack() {
    let ((mut client_read, _client_write), (_server_read, mut server_write)) = create_pair().await;

    let client = {
        async move {
            let msg = client_read.recv_msg().await?;
            match msg {
                client_agent::ServerMsg::Ack(client_agent::msg::Ack { proxyee_id, seq }) => {
                    assert_eq!(proxyee_id, 0);
                    assert_eq!(seq, 4);
                }
                _ => panic!("unexpected msg"),
            };

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        server_write.send_ack(0, 4).await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn client_eof() {
    let ((_client_read, mut client_write), (mut server_read, _server_write)) = create_pair().await;

    let client = {
        async move {
            client_write.send_eof(0, 5).await?;

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        let msg = server_read.recv_msg().await?;
        match msg {
            server_agent::ClientMsg::Eof(server_agent::msg::Eof { proxyee_id, seq }) => {
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
    let ((mut client_read, _client_write), (_server_read, mut server_write)) = create_pair().await;

    let client = {
        async move {
            let msg = client_read.recv_msg().await?;
            match msg {
                client_agent::ServerMsg::Eof(client_agent::msg::Eof { proxyee_id, seq }) => {
                    assert_eq!(proxyee_id, 0);
                    assert_eq!(seq, 5);
                }
                _ => panic!("unexpected msg"),
            };

            Ok::<_, std::io::Error>(())
        }
    };

    let server = async move {
        server_write.send_eof(0, 5).await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#![feature(assert_matches)]
use std::assert_matches::assert_matches;
use std::net::SocketAddr;

use chacha20::ChaCha20;
use tokio::io::{DuplexStream, duplex};

use sep_lib::prelude::*;

async fn create_pair() -> (
    protocol::client_agent::Greeted<DuplexStream, ChaCha20>,
    protocol::server_agent::Greeted<DuplexStream, ChaCha20>,
) {
    let key: Box<[u8; 32]> = vec![0u8; 32].try_into().unwrap();
    let nonce: Box<[u8; 12]> = vec![1u8; 12].try_into().unwrap();

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
async fn req_v4_reply_v4() {
    let req_ip = std::net::Ipv4Addr::new(129, 0, 0, 1);
    let req_port = 1234;

    let bound_ip = std::net::Ipv4Addr::new(129, 0, 0, 2);
    let bound_port = 4321;

    let (client_agent, server_agent) = create_pair().await;

    let client = async move {
        let mut req_buf = ipv4_to_bytes(req_ip, req_port);
        let (recv_bond_addr, client_agent) = client_agent.send_request(&mut req_buf).await?;
        assert_eq!(recv_bond_addr, SocketAddr::new(bound_ip.into(), bound_port));

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        let (recv_req, server_agent) = server_agent.recv_request().await?;
        assert_matches!(recv_req.addr(), sep_lib::decode::ViewAddr::Ipv4(addr) if addr == req_ip);

        server_agent
            .reply(SocketAddr::new(bound_ip.into(), bound_port))
            .await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

#[tokio::test]
async fn req_domain_reply_v4() {
    let req_domain = "example.com";
    let req_port = 1234;

    let bound_ip = std::net::Ipv4Addr::new(129, 0, 0, 2);
    let bound_port = 4321;

    let (client_agent, server_agent) = create_pair().await;

    let client = async move {
        let mut req_buf = domain_to_bytes(req_domain, req_port);
        let (recv_bond_addr, client_agent) = client_agent.send_request(&mut req_buf).await?;
        assert_eq!(recv_bond_addr, SocketAddr::new(bound_ip.into(), bound_port));

        Ok::<_, std::io::Error>(())
    };

    let server = async move {
        let (recv_req, server_agent) = server_agent.recv_request().await?;
        assert_matches!(recv_req.addr(), sep_lib::decode::ViewAddr::Domain(addr) if addr == req_domain);

        server_agent
            .reply(SocketAddr::new(bound_ip.into(), bound_port))
            .await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client, server).unwrap();
}

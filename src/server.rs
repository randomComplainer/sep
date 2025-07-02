use std::iter;
use std::net::SocketAddr;

use sep_lib::prelude::*;

#[tokio::main]
async fn main() {
    println!("Hello world from server");

    let listener = protocol::server_agent::TcpListener::bind(
        "127.0.0.1:1081".parse().unwrap(),
        [0u8; 32].into(),
    )
    .await
    .unwrap();

    println!("Listening...");

    while let Ok((agent, addr)) = listener.accept().await {
        tokio::spawn(async move {
            handle_client(agent, addr).await;
        });
    }
}

async fn resolve_addrs(req: protocol::msg::ViewRequest) -> Result<Vec<SocketAddr>, std::io::Error> {
    let addrs = match req.addr() {
        decode::ViewAddr::Ipv4(addr) => {
            vec![SocketAddr::new(addr.into(), req.port())]
        }
        decode::ViewAddr::Ipv6(addr) => {
            vec![SocketAddr::new(addr.into(), req.port())]
        }
        decode::ViewAddr::Domain(addr) => {
            tokio::net::lookup_host((addr, req.port())).await?.collect()
        }
    };

    Ok(addrs)
}

async fn connect_target(addrs: Vec<SocketAddr>) -> Result<tokio::net::TcpStream, std::io::Error> {
    for addr in addrs {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => {
                return Ok(stream);
            }
            // TODO: collect errors?
            Err(_err) => {
                continue;
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "connect target failed",
    ))
}

pub async fn handle_client<Stream>(agent: protocol::server_agent::Init<Stream>, addr: SocketAddr)
where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let agent = agent
        .recv_greeting(protocol::get_timestamp())
        .await
        .unwrap();

    let (req, agent) = agent.recv_request().await.unwrap();

    let addrs = resolve_addrs(req).await.unwrap();

    let target_stream = connect_target(addrs).await.unwrap();

    let _ = agent
        .reply(target_stream.local_addr().unwrap())
        .await
        .unwrap();
}

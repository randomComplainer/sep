use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use sep_lib::prelude::*;

#[tokio::main]
async fn main() {
    println!("Hello world from server");

    let listener = protocol::server_agent::TcpListener::bind(
        "0.0.0.0:1081".parse().unwrap(),
        protocol::key_from_string("password").into(),
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

pub async fn handle_client<Stream>(
    agent: protocol::server_agent::Init<Stream>,
    _local_addr: SocketAddr,
) where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    dbg!("incoming");
    let agent = agent
        .recv_greeting(protocol::get_timestamp())
        .await
        .unwrap();

    let (req, agent) = agent.recv_request().await.unwrap();

    dbg!(req.addr());

    let addrs = resolve_addrs(req).await.unwrap();

    let target_stream = connect_target(addrs).await.unwrap();

    dbg!("target connected");

    let (client_read, mut client_write) = agent
        .reply(target_stream.local_addr().unwrap())
        .await
        .unwrap();

    let (mut target_read, mut target_write) = target_stream.into_split();

    let (client_read_buf, mut client_read) = client_read.into_parts();

    let client_to_target = async move {
        let mut bytes_forwarede_to_target = 0;
        let mut log_bytes_forwarede_to_target = |n: usize| {
            bytes_forwarede_to_target += n;
            dbg!(bytes_forwarede_to_target);
        };

        target_write.write_all(client_read_buf.as_ref()).await?;
        log_bytes_forwarede_to_target(client_read_buf.len());

        // tokio::io::copy(&mut client_read, &mut target_write).await?;

        let mut buf = vec![0u8; 1024 * 4];
        loop {
            let n = client_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            target_write.write_all(&mut buf[..n]).await?;
            log_bytes_forwarede_to_target(n);
        }

        Ok::<_, std::io::Error>(())
    };

    let target_to_client = async move {
        let mut bytes_forwarede_to_client = 0;
        let mut log_bytes_forwarede_to_client = |n: usize| {
            bytes_forwarede_to_client += n;
            dbg!(bytes_forwarede_to_client);
        };
        // TODO: magic size
        let mut buf = vec![0u8; 1024 * 4];
        loop {
            let n = target_read.read(&mut buf).await?;
            log_bytes_forwarede_to_client(n);
            if n == 0 {
                break;
            }
            client_write.write_all(&mut buf[..n]).await?;
        }

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(client_to_target, target_to_client).unwrap();
}

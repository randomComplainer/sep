use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use sep_lib::prelude::*;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    key: String,

    #[arg(short, long="addr", default_value_t = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))]
    bound_addr: IpAddr,

    #[arg(short, long, default_value_t = 1081)]
    port: u16,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    dbg!(&args);

    let bound_addr = SocketAddr::new(args.bound_addr, args.port);

    let listener = protocol::server_agent::TcpListener::bind(
        bound_addr,
        protocol::key_from_string(&args.key).into(),
    )
    .await
    .unwrap();

    println!("Listening at {}...", bound_addr);

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

    let agent = match agent.recv_greeting(protocol::get_timestamp()).await {
        Ok(agent) => agent,
        Err(err) => {
            dbg!(format!("{}", err));
            dbg!("failed to receive greeting");
            return;
        }
    };

    let (req, agent) = match agent.recv_request().await {
        Ok(req) => req,
        Err(err) => {
            dbg!(err);
            dbg!("failed to receive request");
            return;
        }
    };

    dbg!(req.addr());

    let addrs = match resolve_addrs(req).await {
        Ok(addrs) => addrs,
        Err(err) => {
            dbg!(err);
            dbg!("failed to resolve addrs");
            return;
        }
    };

    let target_stream = match connect_target(addrs).await {
        Ok(target_stream) => target_stream,
        Err(err) => {
            dbg!(err);
            dbg!("failed to connect target");
            return;
        }
    };

    dbg!("target connected");

    let (client_read, mut client_write) =
        match agent.reply(target_stream.local_addr().unwrap()).await {
            Ok(client) => client,
            Err(err) => {
                dbg!(err);
                dbg!("failed to reply");
                return;
            }
        };

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

    match tokio::try_join!(client_to_target, target_to_client) {
        Ok(_) => {}
        Err(err) => {
            dbg!(err);
            dbg!("error while forwarding data");
        }
    }
}

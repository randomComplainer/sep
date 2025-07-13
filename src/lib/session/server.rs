use std::net::SocketAddr;

use futures::prelude::*;
use thiserror::Error;

use super::msg;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ServerSessionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("client write closed")]
    ClientWriteClosed,
}

// pub fn create_task(
//     client_read_rx: impl Stream<Item = msg::ClientMsg> + Unpin,
// ) -> (
//     impl Stream<Item = msg::ServerMsg> + Unpin,
//     impl std::future::Future<Output = Result<(), ServerSessionError>>,
// ) {
//     // TODO: magic queue size
//     let (client_write_tx, client_write_rx) = futures::channel::mpsc::channel(4);
//
//     (client_write_rx, run_task(client_read_rx, client_write_tx))
// }

async fn resolve_addrs(
    addr: decode::ReadRequestAddr,
    port: u16,
) -> Result<Vec<SocketAddr>, std::io::Error> {
    let addrs = match addr {
        decode::ReadRequestAddr::Ipv4(addr) => {
            vec![SocketAddr::new(addr.into(), port)]
        }
        decode::ReadRequestAddr::Ipv6(addr) => {
            vec![SocketAddr::new(addr.into(), port)]
        }
        decode::ReadRequestAddr::Domain(addr) => {
            tokio::net::lookup_host((str::from_utf8(addr.as_ref()).unwrap(), port))
                .await?
                .collect()
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

pub async fn create_task(
    mut client_read: impl Stream<Item = msg::ClientMsg> + Unpin,
    mut client_write: impl Sink<msg::ServerMsg, Error = futures::channel::mpsc::SendError> + Unpin,
) -> Result<(), ServerSessionError> {
    let req = match client_read.next().await {
        Some(msg) => match msg {
            msg::ClientMsg::Request(msg) => msg,
            _ => {
                return Err(ServerSessionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected msg",
                )));
            }
        },
        None => {
            return Err(ServerSessionError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            )));
        }
    };

    dbg!(req.addr.read());

    let addrs = resolve_addrs(req.addr, req.port).await?;
    let target_stream = connect_target(addrs).await?;

    client_write
        .send(msg::ServerMsg::Reply(msg::Reply {
            bound_addr: target_stream.local_addr().unwrap(),
        }))
        .await
        .map_err(|_| ServerSessionError::ClientWriteClosed)?;

    return Ok(());
}

use std::net::SocketAddr;

use futures::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::msg;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ServerSessionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("client write closed")]
    ClientWriteClosed,
}

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

    dbg!(&req.addr);

    let addrs = resolve_addrs(req.addr, req.port).await?;
    let target_stream = connect_target(addrs).await?;

    client_write
        .send(msg::ServerMsg::Reply(msg::Reply {
            bound_addr: target_stream.local_addr().unwrap(),
        }))
        .await
        .map_err(|_| ServerSessionError::ClientWriteClosed)?;

    let (mut target_read, mut target_write) = target_stream.into_split();

    let target_to_client = {
        async move {
            let mut seq = 0;
            loop {
                // TODO: reuse buf & buf size
                let mut buf = bytes::BytesMut::with_capacity(1024 * 4);
                let n = target_read.read_buf(&mut buf).await?;
                dbg!(n);
                if n == 0 {
                    break;
                }

                client_write
                    .send(msg::Data { seq, data: buf }.into())
                    .await
                    .unwrap();

                seq += 1;
            }
            dbg!("hit");

            client_write.send(msg::Eof { seq }.into()).await.unwrap();
            Ok::<_, std::io::Error>(())
        }
    };

    let client_to_target = {
        async move {
            // TODO: magic capacity
            let mut heap = std::collections::BinaryHeap::<
                std::cmp::Reverse<super::client::StreamEntry>,
            >::with_capacity(8);

            let mut next_seq = 0u16;

            while let Some(msg) = client_read.next().await {
                match msg {
                    msg::ClientMsg::Data(data) => {
                        if heap.len() == heap.capacity() {
                            panic!("too many data");
                        }

                        heap.push(std::cmp::Reverse(super::client::StreamEntry::data(
                            data.seq, data.data,
                        )));
                    }
                    msg::ClientMsg::Ack(ack) => {
                        // TODO
                    }
                    msg::ClientMsg::Eof(eof) => {
                        if heap.len() == heap.capacity() {
                            panic!("too many eof");
                        }

                        heap.push(std::cmp::Reverse(super::client::StreamEntry::eof(eof.seq)));
                    }
                    _ => panic!("unexpected msg"),
                };

                while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                    let super::client::StreamEntry(_seq, entry) = heap.pop().unwrap().0;

                    match entry {
                        super::client::StreamEntryValue::Data(data) => {
                            dbg!(data.as_ref());
                            dbg!(format!("data to target [{}]", data.len()));
                            target_write.write_all(data.as_ref()).await?;
                        }
                        super::client::StreamEntryValue::Eof => {
                            dbg!("eof");
                            target_write.flush().await?;
                            drop(target_write);
                            return Ok(());
                        }
                    }

                    next_seq += 1;
                }
            }

            Ok::<_, std::io::Error>(())
        }
    };

    tokio::try_join! {
        target_to_client,
        client_to_target,
    }
    .unwrap();

    Ok(())
}

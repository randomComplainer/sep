use std::net::SocketAddr;

use futures::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::msg;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ServerSessionError {
    #[error("target io error")]
    TargetIoError(std::io::Error),

    #[error("client write closed")]
    ClientWriteClosed,

    #[error("client read closed")]
    ClientReadClosed,

    #[error("protocol error: {0}")]
    Protocol(String),
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

pub async fn run(
    mut client_read: impl Stream<Item = msg::ClientMsg> + Unpin,
    mut client_write: impl Sink<msg::ServerMsg, Error = futures::channel::mpsc::SendError> + Unpin,
) -> Result<(), ServerSessionError> {
    let req = match client_read.next().await {
        Some(msg) => match msg {
            msg::ClientMsg::Request(msg) => msg,
            _ => {
                return Err(ServerSessionError::Protocol(format!(
                    "unexpected client msg: [{:?}], expected request",
                    msg
                )));
            }
        },
        None => {
            return Err(ServerSessionError::ClientReadClosed);
        }
    };

    let target_stream = match resolve_addrs(req.addr, req.port)
        .and_then(connect_target)
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            let _ = client_write
                .send(msg::ServerMsg::ReplyError(err.into()))
                .await;
            return Ok(());
        }
    };

    client_write
        .send(msg::ServerMsg::Reply(msg::Reply {
            bound_addr: target_stream.local_addr().unwrap(),
        }))
        .await
        .map_err(|_| ServerSessionError::ClientWriteClosed)?;

    let (mut target_read, mut target_write) = target_stream.into_split();

    let (max_client_acked_tx, mut max_client_acked_rx) = tokio::sync::watch::channel(0);

    let (mut client_write_funnel_tx, mut client_write_funnel_rx) =
        futures::channel::mpsc::channel::<msg::ServerMsg>(1);

    let funneling_server_msg = async move {
        while let Some(msg) = client_write_funnel_rx.next().await {
            client_write
                .send(msg)
                .await
                .map_err(|_| ServerSessionError::ClientWriteClosed)?;
        }

        Ok::<_, ServerSessionError>(())
    };

    let target_to_client = {
        let mut client_write_funnel_tx = client_write_funnel_tx.clone();
        async move {
            let mut seq = 0;
            loop {
                // TODO: magic number
                max_client_acked_rx
                    .wait_for(|acked| seq - acked < 14)
                    .await
                    .unwrap();

                dbg!(format!(
                    "next seq: {}, acked: {}",
                    seq,
                    *max_client_acked_rx.borrow()
                ));

                // TODO: reuse buf & buf size
                let mut buf = bytes::BytesMut::with_capacity(1024 * 8);
                let n = match target_read.read_buf(&mut buf).await {
                    Ok(n) => n,
                    Err(err) => {
                        dbg!(format!("target read error: {:?}", err));
                        let _ = client_write_funnel_tx.send(msg::Eof { seq }.into()).await;
                        return Err(ServerSessionError::TargetIoError(err));
                    }
                };

                if n == 0 {
                    break;
                }

                client_write_funnel_tx
                    .send(msg::Data { seq, data: buf }.into())
                    .await
                    .map_err(|_| ServerSessionError::ClientWriteClosed)?;

                seq += 1;
            }

            client_write_funnel_tx
                .send(msg::Eof { seq }.into())
                .await
                .map_err(|_| ServerSessionError::ClientWriteClosed)?;

            Ok::<_, ServerSessionError>(())
        }
    };

    let client_to_target = {
        async move {
            // TODO: magic capacity
            let mut heap = std::collections::BinaryHeap::<
                std::cmp::Reverse<super::client::StreamEntry>,
            >::with_capacity(16);

            let mut next_seq = 0u16;

            while let Some(msg) = client_read.next().await {
                match msg {
                    msg::ClientMsg::Data(data) => {
                        if heap.len() == heap.capacity() {
                            return Err(ServerSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(super::client::StreamEntry::data(
                            data.seq, data.data,
                        )));
                    }
                    msg::ClientMsg::Ack(ack) => {
                        let _ = max_client_acked_tx.send(ack.seq);
                    }
                    msg::ClientMsg::Eof(eof) => {
                        if heap.len() == heap.capacity() {
                            return Err(ServerSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(super::client::StreamEntry::eof(eof.seq)));
                    }
                    msg::ClientMsg::ProxyeeIoError(_) => {
                        return Ok(());
                    }
                    _ => {
                        return Err(ServerSessionError::Protocol(format!(
                            "unexpected client msg: [{:?}], expected data/eof/err",
                            msg
                        )));
                    }
                };

                while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                    let super::client::StreamEntry(seq, entry) = heap.pop().unwrap().0;

                    match entry {
                        super::client::StreamEntryValue::Data(data) => {
                            target_write
                                .write_all(data.as_ref())
                                .await
                                .inspect_err(|err| {
                                    dbg!(format!("target write error: {:?}", err));
                                })
                                .map_err(|err| ServerSessionError::TargetIoError(err))?;

                            let _ = client_write_funnel_tx.send(msg::Ack { seq }.into()).await;
                        }
                        super::client::StreamEntryValue::Eof => {
                            return Ok(());
                        }
                    }

                    next_seq += 1;
                }
            }

            Ok::<_, ServerSessionError>(())
        }
    };

    tokio::try_join! {
        funneling_server_msg,
        target_to_client,
        client_to_target,
    }
    .map(|_| ())
}

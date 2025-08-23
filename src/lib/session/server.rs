use std::net::SocketAddr;

use futures::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tracing::*;

use super::msg;
use super::sequence::*;
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
    mut client_write: impl Sink<msg::ServerMsg, Error = futures::channel::mpsc::SendError>
    + Unpin
    + Clone,
) -> Result<(), ServerSessionError> {
    let req = match client_read
        .next()
        .instrument(info_span!("receive request from client"))
        .await
    {
        Some(msg) => match msg {
            msg::ClientMsg::Request(msg) => msg,
            _ => {
                error!("unexpected client msg: {:?}, expected request", msg);

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

    info!(
        "request received, addr = {:?}, port = {}",
        req.addr, req.port
    );

    let target_stream = match resolve_addrs(req.addr, req.port)
        .instrument(info_span!("resolve target address"))
        .and_then(|resolved| connect_target(resolved).instrument(info_span!("connect target")))
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            error!("connect target failed: {:?}", err);

            let _ = client_write
                .send(msg::ServerMsg::ReplyError(err.into()))
                .instrument(info_span!("send reply error to client"))
                .await;
            return Ok(());
        }
    };

    client_write
        .send(msg::ServerMsg::Reply(msg::Reply {
            bound_addr: target_stream.local_addr().unwrap(),
        }))
        .instrument(info_span!("send reply to client"))
        .await
        .map_err(|_| ServerSessionError::ClientWriteClosed)?;

    let (mut target_read, mut target_write) = target_stream.into_split();

    let (max_client_acked_tx, mut max_client_acked_rx) = tokio::sync::watch::channel(0);

    let target_to_client = {
        let mut client_write = client_write.clone();
        async move {
            let mut seq = 0;
            loop {
                max_client_acked_rx
                    .wait_for(|acked| seq - acked < super::MAX_DATA_AHEAD)
                    .instrument(info_span!("wait for ack", current = seq))
                    .await
                    .unwrap();

                // TODO: reuse buf
                let mut buf = bytes::BytesMut::with_capacity(super::DATA_BUFF_SIZE);
                let n = match target_read
                    .read_buf(&mut buf)
                    .instrument(info_span!("read data from target", seq))
                    .await
                {
                    Ok(n) => n,
                    Err(err) => {
                        error!("target read error: {:?}", err);
                        let _ = client_write
                            .send(msg::Eof { seq }.into())
                            .instrument(info_span!("send eof to client"))
                            .await;
                        return Err(ServerSessionError::TargetIoError(err));
                    }
                };

                if n == 0 {
                    break;
                }

                client_write
                    .send(msg::Data { seq, data: buf }.into())
                    .instrument(info_span!("send data to client", seq, len = n))
                    .await
                    .map_err(|_| ServerSessionError::ClientWriteClosed)?;

                seq += 1;
            }

            client_write
                .send(msg::Eof { seq }.into())
                .instrument(info_span!("send eof to client", seq))
                .await
                .map_err(|_| ServerSessionError::ClientWriteClosed)?;

            Ok::<_, ServerSessionError>(())
        }
    };

    let client_to_target = {
        async move {
            let mut heap =
                std::collections::BinaryHeap::<std::cmp::Reverse<StreamEntry>>::with_capacity(
                    super::MAX_DATA_AHEAD as usize,
                );

            let mut next_seq = 0u16;

            while let Some(msg) = client_read
                .next()
                .instrument(info_span!("receive message from client"))
                .await
            {
                match msg {
                    msg::ClientMsg::Data(data) => {
                        info!(
                            seq = data.seq,
                            len = data.data.len(),
                            "data message from client"
                        );
                        if heap.len() == heap.capacity() {
                            return Err(ServerSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));
                    }
                    msg::ClientMsg::Ack(ack) => {
                        info!(seq = ack.seq, "ack message from client");
                        let _ = max_client_acked_tx.send(ack.seq);
                    }
                    msg::ClientMsg::Eof(eof) => {
                        info!(seq = eof.seq, "eof message from client");
                        if heap.len() == heap.capacity() {
                            return Err(ServerSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
                    }
                    msg::ClientMsg::ProxyeeIoError(_) => {
                        info!("proxyee io error message from client");
                        return Ok(());
                    }
                    _ => {
                        error!("unexpected client msg: {:?}", msg);
                        return Err(ServerSessionError::Protocol(format!(
                            "unexpected client msg: [{:?}], expected data/eof/err",
                            msg
                        )));
                    }
                };

                while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                    let StreamEntry(seq, entry) = heap.pop().unwrap().0;

                    match entry {
                        StreamEntryValue::Data(data) => {
                            target_write
                                .write_all(data.as_ref())
                                .instrument(info_span!(
                                    "send data to target",
                                    seq,
                                    len = data.len()
                                ))
                                .await
                                .inspect_err(|err| {
                                    error!("target write error: {:?}", err);
                                })
                                .map_err(|err| ServerSessionError::TargetIoError(err))?;

                            let _ = client_write
                                .send(msg::Ack { seq }.into())
                                .instrument(info_span!("send ack to client", seq))
                                .await;
                        }
                        StreamEntryValue::Eof => {
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
        target_to_client,
        client_to_target,
    }
    .map(|_| ())
}

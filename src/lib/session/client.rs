use futures::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tracing::*;

use super::msg;
use super::sequence::*;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ClientSessionError {
    #[error("Prxyee socks5 error: {0}")]
    ProxyeeSocks5(#[from] socks5::Socks5Error),

    #[error("server write closed")]
    ServerWriteClosed,

    #[error("server read closed")]
    ServerReadClosed,

    #[error("protocol error: {0}")]
    Protocol(String),
}

pub async fn run<ProxyeeStream>(
    proxyee: socks5::agent::Init<ProxyeeStream>,
    mut server_read: impl Stream<Item = msg::ServerMsg> + Unpin,
    mut server_write: impl Sink<msg::ClientMsg, Error = futures::channel::mpsc::SendError>
    + Unpin
    + Clone,
) -> Result<(), ClientSessionError>
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let result = run_inner(proxyee, &mut server_read, server_write.clone()).await;

    if let Err(ClientSessionError::ProxyeeSocks5(_)) = &result {
        let _ = server_write
            .send(msg::ClientMsg::ProxyeeIoError(msg::IoError))
            .await;
    }

    return result;
}

async fn run_inner<ProxyeeStream>(
    proxyee: socks5::agent::Init<ProxyeeStream>,
    server_read: &mut (impl Stream<Item = msg::ServerMsg> + Unpin),
    mut server_write: impl Sink<msg::ClientMsg, Error = futures::channel::mpsc::SendError>
    + Unpin
    + Clone,
) -> Result<(), ClientSessionError>
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (_, proxyee) = proxyee
        .receive_greeting_message()
        .instrument(info_span!("receive greeting message from proxyee"))
        .await?;

    let (proxyee_req, proxyee) = proxyee.send_method_selection_message(0).await?;

    info!(addr = ?proxyee_req.addr, port = proxyee_req.port, "request addr");

    server_write
        .send(msg::ClientMsg::Request(msg::Request {
            addr: proxyee_req.addr,
            port: proxyee_req.port,
        }))
        .instrument(info_span!("send request to server"))
        .await
        .map_err(|_| ClientSessionError::ServerWriteClosed)?;

    let reply = match server_read
        .next()
        .instrument(info_span!("receive reply from server"))
        .await
    {
        Some(msg) => match msg {
            msg::ServerMsg::Reply(msg) => msg,
            msg::ServerMsg::ReplyError(err) => {
                use session::msg::ConnectionError::*;
                let _ = proxyee
                    .reply_error(match err {
                        General => 1,
                        NetworkUnreachable => 3,
                        HostUnreachable => 4,
                        ConnectionRefused => 5,
                        TtlExpired => 6,
                    })
                    .instrument(info_span!("send reply error to proxyee"))
                    .await;
                return Ok(());
            }
            msg => {
                return Err(ClientSessionError::Protocol(format!(
                    "unexpected server msg: [{:?}], expected reply/reply_error",
                    msg
                )));
            }
        },
        None => {
            return Err(ClientSessionError::ServerReadClosed);
        }
    };

    let (proxyee_read, mut proxyee_write) = proxyee
        .reply(&reply.bound_addr)
        .instrument(info_span!("reply to proxyee"))
        .await
        .map_err(|err| ClientSessionError::ProxyeeSocks5(err.into()))?;

    let (max_server_acked_tx, mut max_server_acked_rx) = tokio::sync::watch::channel(0);

    let proxyee_to_server = {
        let mut server_write = server_write.clone();
        async move {
            let (buf, mut proxyee_read) = proxyee_read.into_parts();
            let mut seq = 0;
            let len = buf.len();

            server_write
                .send(msg::Data { seq, data: buf }.into())
                .instrument(info_span!("send data to server", seq, len))
                .await
                .map_err(|_| ClientSessionError::ServerWriteClosed)?;

            seq += 1;

            loop {
                // TODO: magic number
                max_server_acked_rx
                    .wait_for(|acked| seq - acked < super::MAX_DATA_AHEAD)
                    .instrument(info_span!("wait for ack", current = seq))
                    .await
                    .unwrap();

                // TODO: reuse buf
                let mut buf = bytes::BytesMut::with_capacity(super::DATA_BUFF_SIZE);
                let n = proxyee_read
                    .read_buf(&mut buf)
                    .instrument(info_span!("read data from proxyee", seq))
                    .await
                    .map_err(|err| ClientSessionError::ProxyeeSocks5(err.into()))?;

                info!(len = n, "bytes read from proxyee");

                if n == 0 {
                    info!("end of proxyee read stream");
                    break;
                }

                server_write
                    .send(msg::Data { seq, data: buf }.into())
                    .instrument(info_span!("send data to server", seq, len = n))
                    .await
                    .map_err(|_| ClientSessionError::ServerWriteClosed)?;

                seq += 1;
            }

            server_write
                .send(msg::Eof { seq }.into())
                .instrument(info_span!("send eof to server", seq))
                .await
                .map_err(|_| ClientSessionError::ServerWriteClosed)?;

            Ok::<_, ClientSessionError>(())
        }
    };

    let server_to_proxyee = {
        async move {
            // TODO: magic capacity
            let mut heap =
                std::collections::BinaryHeap::<std::cmp::Reverse<StreamEntry>>::with_capacity(
                    super::MAX_DATA_AHEAD as usize,
                );

            let mut next_seq = 0u16;

            while let Some(msg) = server_read
                .next()
                .instrument(info_span!("receive message from server"))
                .await
            {
                match msg {
                    msg::ServerMsg::Data(data) => {
                        info!(
                            seq = data.seq,
                            len = data.data.len(),
                            "data message from server"
                        );

                        if heap.len() == heap.capacity() {
                            return Err(ClientSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));
                    }
                    msg::ServerMsg::Ack(ack) => {
                        info!(seq = ack.seq, "ack message from server");
                        let _ = max_server_acked_tx.send(ack.seq);
                    }
                    msg::ServerMsg::Eof(eof) => {
                        info!(seq = eof.seq, "eof message from server");
                        if heap.len() == heap.capacity() {
                            return Err(ClientSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
                    }
                    msg::ServerMsg::TargetIoError(_) => {
                        info!("target io error message from server");
                        return Ok(());
                    }
                    msg => {
                        error!("unexpected server msg: {:?}", msg);
                        return Err(ClientSessionError::Protocol(format!(
                            "unexpected server msg: [{:?}], expected data/eof/err",
                            msg
                        )));
                    }
                };

                while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                    let StreamEntry(seq, entry) = heap.pop().unwrap().0;

                    match entry {
                        StreamEntryValue::Data(data) => {
                            proxyee_write
                                .write_all(data.as_ref())
                                .instrument(info_span!(
                                    "send data to proxyee",
                                    seq,
                                    len = data.len()
                                ))
                                .await
                                .map_err(|err| ClientSessionError::ProxyeeSocks5(err.into()))?;

                            let _ = server_write
                                .send(msg::Ack { seq }.into())
                                .instrument(info_span!("send ack to server", seq))
                                .await;
                        }
                        StreamEntryValue::Eof => {
                            return Ok(());
                        }
                    }

                    next_seq += 1;
                }
            }

            Ok::<_, ClientSessionError>(())
        }
    };

    tokio::try_join! {
        proxyee_to_server,
        server_to_proxyee,
    }
    .map(|_| ())
}

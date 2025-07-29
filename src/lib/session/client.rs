use std::sync::Arc;

use bytes::BytesMut;
use futures::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::msg;
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
    mut server_write: impl Sink<msg::ClientMsg, Error = futures::channel::mpsc::SendError> + Unpin,
) -> Result<(), ClientSessionError>
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let result = run_inner(proxyee, &mut server_read, &mut server_write).await;

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
    server_write: &mut (impl Sink<msg::ClientMsg, Error = futures::channel::mpsc::SendError> + Unpin),
) -> Result<(), ClientSessionError>
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (_, proxyee) = proxyee.receive_greeting_message().await?;

    let (proxyee_req, proxyee) = proxyee.send_method_selection_message(0).await?;

    server_write
        .send(msg::ClientMsg::Request(msg::Request {
            addr: proxyee_req.addr,
            port: proxyee_req.port,
        }))
        .await
        .map_err(|_| ClientSessionError::ServerWriteClosed)?;

    let reply = match server_read.next().await {
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
        .await
        .map_err(|err| ClientSessionError::ProxyeeSocks5(err.into()))?;

    let (max_server_acked_tx, mut max_server_acked_rx) = tokio::sync::watch::channel(0);

    let (mut server_write_funnel_tx, mut server_write_funnel_rx) =
        futures::channel::mpsc::channel::<msg::ClientMsg>(1);

    let funneling_client_msg = async move {
        while let Some(msg) = server_write_funnel_rx.next().await {
            server_write
                .send(msg)
                .await
                .map_err(|_| ClientSessionError::ServerWriteClosed)?;
        }

        Ok::<_, ClientSessionError>(())
    };

    let proxyee_to_server = {
        let mut server_write_funnel_tx = server_write_funnel_tx.clone();
        async move {
            let (buf, mut proxyee_read) = proxyee_read.into_parts();
            let mut seq = 0;

            server_write_funnel_tx
                .send(msg::Data { seq, data: buf }.into())
                .await
                .map_err(|_| ClientSessionError::ServerWriteClosed)?;

            seq += 1;

            loop {
                max_server_acked_rx
                    .wait_for(|acked| seq - acked < 6)
                    .await
                    .unwrap();

                // TODO: reuse buf & buf size
                let mut buf = bytes::BytesMut::with_capacity(1024 * 8);
                let n = proxyee_read
                    .read_buf(&mut buf)
                    .await
                    .map_err(|err| ClientSessionError::ProxyeeSocks5(err.into()))?;

                if n == 0 {
                    break;
                }

                server_write_funnel_tx
                    .send(msg::Data { seq, data: buf }.into())
                    .await
                    .map_err(|_| ClientSessionError::ServerWriteClosed)?;

                seq += 1;
            }

            server_write_funnel_tx
                .send(msg::Eof { seq }.into())
                .await
                .map_err(|_| ClientSessionError::ServerWriteClosed)?;

            Ok::<_, ClientSessionError>(())
        }
    };

    let server_to_proxyee = {
        async move {
            // TODO: magic capacity
            let mut heap =
                std::collections::BinaryHeap::<std::cmp::Reverse<StreamEntry>>::with_capacity(8);

            let mut next_seq = 0u16;

            while let Some(msg) = server_read.next().await {
                match msg {
                    msg::ServerMsg::Data(data) => {
                        if heap.len() == heap.capacity() {
                            return Err(ClientSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));
                    }
                    msg::ServerMsg::Ack(ack) => {
                        let _ = max_server_acked_tx.send(ack.seq);
                    }
                    msg::ServerMsg::Eof(eof) => {
                        if heap.len() == heap.capacity() {
                            return Err(ClientSessionError::Protocol(
                                "too many data cached".to_string(),
                            ));
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
                    }
                    msg::ServerMsg::TargetIoError(_) => {
                        return Ok(());
                    }
                    msg => {
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
                                .await
                                .map_err(|err| ClientSessionError::ProxyeeSocks5(err.into()))?;

                            let _ = server_write_funnel_tx.send(msg::Ack { seq }.into()).await;
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
        funneling_client_msg,
        proxyee_to_server,
        server_to_proxyee,
    }
    .map(|_| ())
}

pub enum StreamEntryValue {
    Data(BytesMut),
    Eof,
}

pub struct StreamEntry(pub u16, pub StreamEntryValue);

impl StreamEntry {
    pub fn data(seq: u16, data: BytesMut) -> Self {
        Self(seq, StreamEntryValue::Data(data))
    }

    pub fn eof(seq: u16) -> Self {
        Self(seq, StreamEntryValue::Eof)
    }
}

impl PartialEq for StreamEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for StreamEntry {}

impl std::cmp::PartialOrd for StreamEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl std::cmp::Ord for StreamEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

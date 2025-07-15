use bytes::BytesMut;
use futures::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::msg;
use crate::prelude::*;

#[derive(Error, Debug)]
pub enum ClientSessionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("socks5 error: {0}")]
    Socks5Error(#[from] socks5::Socks5Error),

    #[error("server write closed")]
    ServerWriteClosed,
}

pub async fn run_task<ProxyeeStream>(
    proxyee: socks5::agent::Init<ProxyeeStream>,
    mut server_read: impl Stream<Item = msg::ServerMsg> + Unpin,
    mut server_write: impl Sink<msg::ClientMsg, Error = futures::channel::mpsc::SendError> + Unpin,
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
            _ => {
                return Err(ClientSessionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected msg",
                )));
            }
        },
        None => {
            return Err(ClientSessionError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            )));
        }
    };

    let (proxyee_read, mut proxyee_write) = proxyee.reply(&reply.bound_addr).await?;

    let proxyee_to_server = {
        async move {
            let (buf, mut proxyee_read) = proxyee_read.into_parts();
            let mut seq = 0;

            server_write
                .send(msg::Data { seq, data: buf }.into())
                .await
                .unwrap();

            seq += 1;

            loop {
                // TODO: reuse buf & buf size
                let mut buf = bytes::BytesMut::with_capacity(1024 * 4);
                let n = proxyee_read.read_buf(&mut buf).await?;
                if n == 0 {
                    break;
                }

                server_write
                    .send(msg::Data { seq, data: buf }.into())
                    .await
                    .unwrap();

                seq += 1;
            }

            server_write.send(msg::Eof { seq }.into()).await.unwrap();

            Ok::<_, std::io::Error>(())
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
                            panic!("too many data");
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));
                    }
                    msg::ServerMsg::Ack(ack) => {
                        // TODO
                    }
                    msg::ServerMsg::Eof(eof) => {
                        if heap.len() == heap.capacity() {
                            panic!("too many eof");
                        }

                        heap.push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
                    }
                    _ => panic!("unexpected msg"),
                };

                while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                    let StreamEntry(_seq, entry) = heap.pop().unwrap().0;

                    match entry {
                        StreamEntryValue::Data(data) => {
                            proxyee_write.write_all(data.as_ref()).await?;
                        }
                        StreamEntryValue::Eof => {
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
        proxyee_to_server,
        server_to_proxyee,
    }
    .unwrap();

    Ok(())
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

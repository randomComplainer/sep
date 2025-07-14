use bytes::BytesMut;
use futures::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

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

pub async fn create_task<ProxyeeStream>(
    proxyee: socks5::agent::Init<ProxyeeStream>,
    server_read_rx: impl Stream<Item = msg::ServerMsg> + Unpin,
) -> (
    impl Stream<Item = msg::ClientMsg> + Unpin,
    impl std::future::Future<Output = Result<(), ClientSessionError>>,
)
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // TODO: magic queue size
    let (server_write_tx, server_write_rx) = futures::channel::mpsc::channel(4);

    (
        server_write_rx,
        run_task(proxyee, server_read_rx, server_write_tx),
    )
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

    let (proxyee_read, proxyee_write) = proxyee.reply(&reply.bound_addr).await?;

    // let proxyee_to_server = {
    //     async move {
    //         let (mut buf, mut proxyee_read) = proxyee_read.into_parts();
    //         let mut seq = 0;
    //
    //         server_write
    //             .send(msg::Data { seq, data: buf }.into())
    //             .await
    //             .unwrap();
    //
    //         seq += 1;
    //
    //         // TODO: buf size
    //         let mut buf = BytesMut::with_capacity(1024 * 4);
    //         loop {
    //             let n = proxyee_read.read(&mut buf).await?;
    //             if n == 0 {
    //                 break;
    //             }
    //
    //             server_write
    //                 .send(msg::Data { seq, data: buf }.into())
    //                 .await
    //                 .unwrap();
    //
    //             seq += 1;
    //         }
    //
    //         Ok::<_, std::io::Error>(())
    //     }
    // };

    Ok(())
}

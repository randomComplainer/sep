use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use chacha20::ChaCha20;
use chacha20::cipher::KeyIvInit;
use tokio::io::AsyncReadExt;
use tracing::Instrument as _;
use tracing::*;

use super::*;
use crate::codec::*;
use crate::msg;

pub struct Init<Stream>
where
    Stream: StaticStream,
{
    key: Arc<Key>,
    stream: Stream,
}

impl<Stream> Init<Stream>
where
    Stream: StaticStream,
{
    pub fn new(key: Arc<Key>, stream: Stream) -> Self {
        Self { key, stream }
    }

    pub async fn recv_greeting(
        self,
        server_timestamp: u64,
    ) -> Result<
        (
            Box<protocol::ClientId>,
            protocol::ConnId,
            impl MsgReader<msg::ClientMsg>,
            impl MsgWriter,
        ),
        InitError<Stream>,
    > {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.recv_greeting_inner(server_timestamp),
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))?
    }

    async fn recv_greeting_inner(
        mut self,
        server_timestamp: u64,
    ) -> Result<
        (
            Box<protocol::ClientId>,
            protocol::ConnId,
            impl MsgReader<msg::ClientMsg>,
            impl MsgWriter,
        ),
        InitError<Stream>,
    > {
        let mut nonce: Box<Nonce> = vec![0u8; 12].try_into().unwrap();

        self.stream.read_exact(nonce.as_mut()).await?;

        let cipher = ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into());

        let (stream_read, stream_write) = tokio::io::split(self.stream);
        let stream_read = EncryptedRead::new(stream_read, cipher);
        let mut stream_read = BufDecoder::new(stream_read);

        let client_timestamp = stream_read
            .read_next(&codec::u64_peeker(), std::time::Duration::from_secs(10))
            .await
            .and_then(|opt| {
                opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
            })?;

        async move {
            if u64::abs_diff(client_timestamp, server_timestamp) > 30 {
                return Err(InitError::InvalidGreeting(
                    "invalid timestamp",
                    stream_read
                        .into_parts()
                        .1
                        .into_parts()
                        .0
                        .unsplit(stream_write),
                ));
            }

            let rand_byte_len = cal_rand_byte_len(&self.key, &nonce, client_timestamp);

            if rand_byte_len > RAND_BYTE_LEN_MAX {
                return Err(InitError::InvalidGreeting(
                    "rand_byte_len > RAND_BYTE_LEN_MAX",
                    stream_read
                        .into_parts()
                        .1
                        .into_parts()
                        .0
                        .unsplit(stream_write),
                ));
            }

            let _rand_bytes = stream_read
                .read_next(
                    &slice_peeker_fixed_len(rand_byte_len.try_into().unwrap()),
                    std::time::Duration::from_secs(10),
                )
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?;

            let client_id: Box<[u8; 16]> = stream_read
                .read_next(
                    &slice_peeker_fixed_len(16),
                    std::time::Duration::from_secs(10),
                )
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?
                .to_vec() //TODO: no copy
                .try_into()
                .unwrap();

            let conn_id = stream_read
                .read_next(&codec::u64_peeker(), std::time::Duration::from_secs(10))
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?;

            let reader = (stream_read, msg::client_msg_peeker());

            let writer = EncryptedMsgWrite::new(
                EncryptedWrite::new(
                    stream_write,
                    ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into()),
                ),
                64,
            );

            Ok((client_id, conn_id, reader, writer))
        }
        .instrument(debug_span!("accept greeting"))
        .await
    }
}

impl<Stream> super::Init for Init<Stream>
where
    Stream: StaticStream,
{
    type Stream = Stream;

    async fn recv_greeting(
        self,
        server_timestamp: u64,
    ) -> Result<
        (
            Box<ClientId>,
            ConnId,
            impl MsgReader<msg::ClientMsg>,
            impl MsgWriter,
        ),
        InitError<Stream>,
    > {
        self.recv_greeting(server_timestamp).await
    }
}

pub struct TcpListener {
    inner: tokio::net::TcpListener,
    key: Arc<Key>,
}

impl TcpListener {
    pub async fn bind(addr: &SocketAddr, key: Arc<Key>) -> Result<Self, std::io::Error> {
        let socket = match addr.ip() {
            IpAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            IpAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        socket.set_nodelay(true)?;
        socket.set_reuseaddr(true)?;
        socket.bind(addr.clone())?;
        let inner = socket.listen(addr.port().into())?;
        Ok(Self { inner, key })
    }

    pub async fn accept(&self) -> Result<Init<tokio::net::TcpStream>, std::io::Error> {
        let (stream, _client_addr) = self.inner.accept().await?;
        Ok(Init::new(self.key.clone(), stream))
    }
}

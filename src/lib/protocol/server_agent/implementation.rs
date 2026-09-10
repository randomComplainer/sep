use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use bytes::BytesMut;
use chacha20::ChaCha20;
use chacha20::cipher::KeyIvInit;
use tokio::io::AsyncReadExt;
use tracing::Instrument as _;
use tracing::*;

use super::*;
use crate::decode::*;

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
            GreetedRead<Stream, ChaCha20>,
            GreetedWrite<Stream, ChaCha20>,
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
            GreetedRead<Stream, ChaCha20>,
            GreetedWrite<Stream, ChaCha20>,
        ),
        InitError<Stream>,
    > {
        let mut nonce: Box<Nonce> = vec![0u8; 12].try_into().unwrap();

        self.stream.read_exact(nonce.as_mut()).await?;

        let cipher = ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into());

        let (stream_read, stream_write) = tokio::io::split(self.stream);
        let stream_read = EncryptedRead::new(stream_read, cipher);
        let mut stream_read = crate::decode::BufDecoder::new(stream_read);

        let client_timestamp =
            stream_read
                .read_next(decode::u64_peeker())
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
                .read_next(slice_peeker_fixed_len(rand_byte_len.try_into().unwrap()))
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?;

            let client_id: Box<[u8; 16]> = stream_read
                .read_next(slice_peeker_fixed_len(16))
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?
                .to_vec() //TODO: no copy
                .try_into()
                .unwrap();

            let conn_id = stream_read
                .read_next(decode::u64_peeker())
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?;

            Ok((
                client_id,
                conn_id,
                GreetedRead::new(stream_read),
                GreetedWrite::new(EncryptedWrite::new(
                    stream_write,
                    ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into()),
                )),
            ))
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
    type GreetedRead = GreetedRead<Stream, ChaCha20>;
    type GreetedWrite = GreetedWrite<Stream, ChaCha20>;

    async fn recv_greeting(
        self,
        server_timestamp: u64,
    ) -> Result<(Box<ClientId>, ConnId, Self::GreetedRead, Self::GreetedWrite), InitError<Stream>>
    {
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

pub struct GreetedWrite<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    pub stream_write: WriteEncrypted<Stream, Cipher>,
    main_buf: BytesMut,
}

impl<Stream, Cipher> GreetedWrite<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    pub fn new(stream_write: WriteEncrypted<Stream, Cipher>) -> Self {
        Self {
            stream_write,
            main_buf: BytesMut::with_capacity(64),
        }
    }
}

impl<Stream, Cipher> protocol::MessageWriter for GreetedWrite<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    type Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>;

    async fn send_msg(&mut self, msg: Self::Message) -> Result<(), std::io::Error> {
        self.main_buf.clear();
        let mut side_bufs = Vec::new();

        msg.encode(&mut self.main_buf, &mut side_bufs);

        assert!(side_bufs.len() <= 1);

        self.stream_write.write_all(&mut self.main_buf).await?;

        for mut buf in side_bufs.into_iter() {
            self.stream_write.write_all(buf.as_mut()).await?;
        }

        Ok(())
    }

    fn shutdown(self) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        self.stream_write.close()
    }
}

pub struct GreetedRead<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    stream_read: FramedRead<Stream, Cipher>,
}

impl<Stream, Cipher> GreetedRead<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    pub fn new(stream_read: FramedRead<Stream, Cipher>) -> Self {
        Self { stream_read }
    }
}

impl<Stream, Cipher> protocol::MessageReader for GreetedRead<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    type Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>;

    async fn recv_msg(
        &mut self,
    ) -> Result<Option<msg::conn::ConnMsg<msg::ClientMsg>>, std::io::Error> {
        let msg = self
            .stream_read
            .read_next(msg::conn::conn_msg_peeker(msg::client_msg_peeker()))
            .await?;

        Ok(msg)
    }

    async fn recv_msg_with_timeout(
        &mut self,
        time_limit: Duration,
    ) -> Result<Option<Self::Message>, std::io::Error> {
        let msg = self
            .stream_read
            .read_next_with_timeout(
                msg::conn::conn_msg_peeker(msg::client_msg_peeker()),
                time_limit,
            )
            .await?;

        Ok(msg)
    }
}

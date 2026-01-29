use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
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

        let client_timestamp = stream_read
            .read_next(decode::u64_peeker())
            .await
            .map_err(InitError::from_decode_error)
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
                .map_err(InitError::from_decode_error)
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?;

            let client_id: Box<[u8; 16]> = stream_read
                .read_next(slice_peeker_fixed_len(16))
                .await
                .map_err(InitError::from_decode_error)
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?
                .to_vec() //TODO: no copy
                .try_into()
                .unwrap();

            let client_port = stream_read
                .read_next(decode::u16_peeker())
                .await
                .map_err(InitError::from_decode_error)
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                })?;

            let conn_id = protocol::ConnId::new(client_timestamp, client_port);

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
    // TODO: use Arc<[u8; 32]>
    key: Arc<Key>,
}

impl TcpListener {
    pub async fn bind(addr: &SocketAddr, key: Arc<Key>) -> Result<Self, std::io::Error> {
        let inner = tokio::net::TcpListener::bind(addr).await?;
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
}

impl<Stream, Cipher> GreetedWrite<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    pub fn new(stream_write: WriteEncrypted<Stream, Cipher>) -> Self {
        Self { stream_write }
    }
}

impl<Stream, Cipher> protocol::MessageWriter for GreetedWrite<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    type Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>;

    async fn send_msg(&mut self, msg: Self::Message) -> Result<(), std::io::Error> {
        // TODO: Do I need calculated size?
        let mut buf = BytesMut::with_capacity(64);

        match msg {
            msg::conn::ConnMsg::Protocol(msg) => {
                buf.put_u8(0u8);
                match msg {
                    msg::ServerMsg::SessionMsg(session_id, server_msg) => {
                        buf.put_u8(0u8);
                        buf.put_u64(session_id.timestamp);
                        buf.put_u16(session_id.proxyee_port);
                        match server_msg {
                            session::msg::ServerMsg::Reply(reply) => {
                                buf.put_u8(0u8);
                                match &reply.bound_addr {
                                    std::net::SocketAddr::V4(addr) => {
                                        buf.put_u8(0x01);
                                        buf.put_u32(addr.ip().to_bits());
                                    }
                                    std::net::SocketAddr::V6(addr) => {
                                        buf.put_u8(0x04);
                                        buf.put_slice(&addr.ip().octets());
                                    }
                                };
                                buf.put_u16(reply.bound_addr.port());
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ServerMsg::ReplyError(err) => {
                                buf.put_u8(1u8);
                                use session::msg::ConnectionError::*;
                                buf.put_u8(match err {
                                    General => 0,
                                    NetworkUnreachable => 1,
                                    HostUnreachable => 2,
                                    ConnectionRefused => 3,
                                    TtlExpired => 4,
                                });
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ServerMsg::Data(mut data) => {
                                buf.put_u8(2u8);
                                buf.put_u16(data.seq);
                                buf.put_u16(data.data.len().try_into().unwrap());
                                self.stream_write.write_all(&mut buf).await?;
                                self.stream_write.write_all(&mut data.data).await?;
                            }
                            session::msg::ServerMsg::Ack(ack) => {
                                buf.put_u8(3u8);
                                buf.put_u32(ack.bytes);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ServerMsg::Eof(eof) => {
                                buf.put_u8(4u8);
                                buf.put_u16(eof.seq);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ServerMsg::EofAck(_) => {
                                buf.put_u8(5u8);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                        };
                    }
                    msg::ServerMsg::GlobalCmd(cmd) => {
                        buf.put_u8(1u8);
                        match cmd {
                            msg::AtLeastOnce::Ack(ack) => {
                                buf.put_u8(0u8);
                                buf.put_u32(ack);
                            }
                            msg::AtLeastOnce::Msg(seq, msg) => {
                                buf.put_u8(1u8);
                                buf.put_u32(seq);
                                match msg {
                                    msg::GlobalCmd::KillSession(session_id) => {
                                        buf.put_u8(0u8);
                                        buf.put_u64(session_id.timestamp);
                                        buf.put_u16(session_id.proxyee_port);
                                    }
                                };
                            }
                        };
                        self.stream_write.write_all(&mut buf).await?;
                    }
                }
            }
            msg::conn::ConnMsg::Ping => {
                buf.put_u8(1u8);
                self.stream_write.write_all(&mut buf).await?;
            }
            msg::conn::ConnMsg::EndOfStream => {
                buf.put_u8(2u8);
                self.stream_write.write_all(&mut buf).await?;
            }
        }

        Ok(())
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
    ) -> Result<Option<msg::conn::ConnMsg<msg::ClientMsg>>, DecodeError> {
        let msg = self
            .stream_read
            .read_next(msg::conn::conn_msg_peeker(msg::client_msg_peeker()))
            .await?;

        Ok(msg)
    }
}

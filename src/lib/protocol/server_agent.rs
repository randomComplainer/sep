use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use chacha20::ChaCha20;
use chacha20::cipher::KeyIvInit;
use thiserror::Error;
use tokio::io::AsyncReadExt;

use super::*;
use crate::decode::*;

pub mod msg {
    use super::*;

    use derive_more::From;

    #[derive(Debug, From)]
    pub enum ClientMsg {
        Request(Request),
        Data(Data),
        Ack(Ack),
        Eof(Eof),
    }

    crate::peek_type! {
        pub enum ClientMsg, PeekClientMsg {
            0u8, Request(PeekRequest::peek => PeekRequest),
            1u8, Data(PeekData::peek => PeekData),
            2u8, Ack(PeekAck::peek => PeekAck),
            3u8, Eof(PeekEof::peek => PeekEof),
        }
    }

    #[derive(Debug)]
    pub struct Request {
        pub proxyee_id: u16,
        pub addr: ReadRequestAddr,
        pub port: u16,
    }

    crate::peek_type! {
        pub struct Request,PeekRequest {
            proxyee_id: PeekU16::peek => PeekU16,
            addr: PeekReadRequestAddr::peek => PeekReadRequestAddr,
            port: PeekU16::peek => PeekU16,
        }
    }

    #[derive(Debug)]
    pub struct Data {
        pub proxyee_id: u16,
        pub seq: u16,
        pub data: BytesMut,
    }

    crate::peek_type! {
        pub struct Data,PeekData {
            proxyee_id: PeekU16::peek => PeekU16,
            seq: PeekU16::peek => PeekU16,
            data: PeekSlice::peek_u16_len => PeekSlice,
        }
    }

    #[derive(Debug)]
    pub struct Ack {
        pub proxyee_id: u16,
        pub seq: u16,
    }

    crate::peek_type! {
        pub struct Ack,PeekAck {
            proxyee_id: PeekU16::peek => PeekU16,
            seq: PeekU16::peek => PeekU16,
        }
    }

    #[derive(Debug)]
    pub struct Eof {
        pub proxyee_id: u16,
        pub seq: u16,
    }

    crate::peek_type! {
        pub struct Eof,PeekEof {
            proxyee_id: PeekU16::peek => PeekU16,
            seq: PeekU16::peek => PeekU16,
        }
    }
}

pub use msg::ClientMsg;

#[derive(Error, Debug)]
pub enum InitError<Stream> {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("greeting invalid, {0}")]
    InvalidGreeting(&'static str, Stream),
}

pub struct Init<Stream>
where
    Stream: StaticStream,
{
    key: Arc<[u8; 32]>,
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
            .read_next(decode::PeekU64::peek)
            .await
            .and_then(|opt| {
                opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))
            })?;

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

        let rand_byte_len = super::cal_rand_byte_len(&self.key, &nonce, client_timestamp);

        if rand_byte_len > super::RAND_BYTE_LEN_MAX {
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

        let _ = stream_read
            .read_next(decode::PeekSlice::peek_fixed(rand_byte_len))
            .await
            .and_then(|opt| {
                opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))
            })?;

        Ok((
            GreetedRead::new(stream_read),
            GreetedWrite::new(EncryptedWrite::new(
                stream_write,
                ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into()),
            )),
        ))
    }
}

pub struct TcpListener {
    inner: tokio::net::TcpListener,
    // TODO: use Arc<[u8; 32]>
    key: Arc<Key>,
}

impl TcpListener {
    pub async fn bind(addr: SocketAddr, key: Arc<Key>) -> Result<Self, std::io::Error> {
        let inner = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self { inner, key })
    }

    pub async fn accept(
        &self,
    ) -> Result<(Init<tokio::net::TcpStream>, SocketAddr), std::io::Error> {
        let (stream, addr) = self.inner.accept().await?;
        Ok((Init::new(self.key.clone(), stream), addr))
    }
}

pub struct GreetedWrite<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    stream_write: WriteEncrypted<Stream, Cipher>,
}

impl<Stream, Cipher> GreetedWrite<Stream, Cipher>
where
    Stream: StaticStream,
    Cipher: StaticCipher,
{
    pub fn new(stream_write: WriteEncrypted<Stream, Cipher>) -> Self {
        Self { stream_write }
    }

    pub async fn send_reply(
        &mut self,
        seq: u16,
        bound_addr: std::net::SocketAddr,
    ) -> Result<(), std::io::Error> {
        let mut buf = BytesMut::with_capacity(1 + 2 + 16 + 2);
        buf.put_u8(0x01);
        buf.put_u16(seq);
        match bound_addr {
            std::net::SocketAddr::V4(addr) => {
                buf.put_u8(0x01);
                buf.put_u32(addr.ip().to_bits());
            }
            std::net::SocketAddr::V6(addr) => {
                buf.put_u8(0x04);
                buf.put_slice(&addr.ip().octets());
            }
        };
        buf.put_u16(bound_addr.port());
        self.stream_write.write_all(buf.as_mut()).await?;
        Ok(())
    }

    pub async fn send_data(
        &mut self,
        proxyee_id: u16,
        seq: u16,
        data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        let mut buf = BytesMut::with_capacity(1 + 2 + 2 + 2 + data.len());
        buf.put_u8(0x02);
        buf.put_u16(proxyee_id);
        buf.put_u16(seq);
        buf.put_u16(data.len().try_into().unwrap());
        // TODO: no copy
        buf.put_slice(data);
        self.stream_write.write_all(buf.as_mut()).await?;
        Ok(())
    }

    pub async fn send_ack(&mut self, proxyee_id: u16, seq: u16) -> Result<(), std::io::Error> {
        let mut buf = BytesMut::with_capacity(1 + 2 + 2);
        buf.put_u8(0x03);
        buf.put_u16(proxyee_id);
        buf.put_u16(seq);
        self.stream_write.write_all(buf.as_mut()).await?;
        Ok(())
    }

    pub async fn send_eof(&mut self, proxyee_id: u16, seq: u16) -> Result<(), std::io::Error> {
        let mut buf = BytesMut::with_capacity(1 + 2 + 2);
        buf.put_u8(0x04);
        buf.put_u16(proxyee_id);
        buf.put_u16(seq);
        self.stream_write.write_all(buf.as_mut()).await?;
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

    pub async fn recv_msg(&mut self) -> Result<Option<ClientMsg>, std::io::Error> {
        let msg = self.stream_read.read_next(msg::PeekClientMsg::peek).await?;

        Ok(msg)
    }
}

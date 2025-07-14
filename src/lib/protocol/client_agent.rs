use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use chacha20::ChaCha20;
use chacha20::cipher::KeyIvInit;
use derive_more::From;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::*;

pub mod msg {
    use super::*;
    use crate::decode::*;

    #[derive(Debug, From)]
    pub enum ServerMsg {
        Reply(Reply),
        Data(Data),
        Ack(Ack),
        Eof(Eof),
    }

    crate::peek_type! {
        pub enum ServerMsg, PeekServerMsg {
            1u8, Reply(PeekReply::peek => PeekReply),
            2u8, Data(PeekData::peek => PeekData),
            3u8, Ack(PeekAck::peek => PeekAck),
            4u8, Eof(PeekEof::peek => PeekEof),
        }
    }

    #[derive(Debug)]
    pub struct Reply {
        pub proxyee_id: u16,
        pub bound_addr: std::net::SocketAddr,
    }

    crate::peek_type! {
        pub struct Reply, PeekReply {
            proxyee_id: PeekU16::peek => PeekU16,
            bound_addr: PeekSocketAddr::peek => PeekSocketAddr,
        }
    }

    #[derive(Debug)]
    pub struct Data {
        pub proxyee_id: u16,
        pub seq: u16,
        pub data: BytesMut,
    }

    crate::peek_type! {
        pub struct Data, PeekData {
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
        pub struct Ack, PeekAck {
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
        pub struct Eof, PeekEof {
            proxyee_id: PeekU16::peek => PeekU16,
            seq: PeekU16::peek => PeekU16,
        }
    }
}

pub use msg::ServerMsg;

pub struct Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin,
{
    key: Arc<Key>,
    nonce: Box<Nonce>,
    stream: Stream,
}

impl<Stream> Init<Stream>
where
    Stream: StaticStream,
{
    pub fn new(key: Arc<Key>, nonce: Box<Nonce>, stream: Stream) -> Self {
        Self { key, nonce, stream }
    }

    pub async fn send_greeting(
        self,
        timestamp: u64,
    ) -> Result<
        (
            GreetedWrite<Stream, ChaCha20>,
            GreetedRead<Stream, ChaCha20>,
        ),
        std::io::Error,
    > {
        let cipher = ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into());

        let (stream_read, mut stream_write) = tokio::io::split(self.stream);

        // send nonce in plaintext
        stream_write.write_all(self.nonce.as_slice()).await?;

        let mut stream_write = EncryptedWrite::new(stream_write, cipher);

        let rand_byte_len = super::cal_rand_byte_len(&self.key, &self.nonce, timestamp);

        let mut rand_bytes = vec![0; rand_byte_len];
        rand::rng().fill_bytes(&mut rand_bytes);

        let buf_size = 8 // timestamp
            + rand_byte_len;

        let mut buf = BytesMut::with_capacity(buf_size);
        buf.put_u64(timestamp);
        buf.put_slice(&rand_bytes);

        stream_write.write_all(buf.as_mut()).await?;

        Ok((
            GreetedWrite::new(stream_write),
            GreetedRead::new(BufDecoder::new(EncryptedRead::new(
                stream_read,
                ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into()),
            ))),
        ))
    }
}

pub struct GreetedWrite<Stream, Cipher> {
    stream_write: WriteEncrypted<Stream, Cipher>,
}

impl<Stream, Cipher> GreetedWrite<Stream, Cipher>
where
    Stream: AsyncWrite + Unpin,
    Cipher: StaticCipher,
{
    pub fn new(stream_write: WriteEncrypted<Stream, Cipher>) -> Self {
        Self { stream_write }
    }

    pub async fn send_request(
        &mut self,
        proxyee_id: u16,
        mut addr: decode::ReadRequestAddr,
        port: u16,
    ) -> Result<(), std::io::Error> {
        // TODO: batch write
        self.stream_write.write_all(&mut [0u8]).await?;
        self.stream_write
            .write_all(&mut proxyee_id.to_be_bytes())
            .await?;

        match &mut addr {
            decode::ReadRequestAddr::Ipv4(addr) => {
                self.stream_write.write_all(&mut [1u8]).await?;
                self.stream_write.write_all(&mut addr.octets()).await?;
            }
            decode::ReadRequestAddr::Ipv6(addr) => {
                self.stream_write.write_all(&mut [4u8]).await?;
                self.stream_write.write_all(&mut addr.octets()).await?;
            }
            decode::ReadRequestAddr::Domain(domain) => {
                self.stream_write.write_all(&mut [3u8]).await?;
                self.stream_write
                    .write_all(&mut [domain.len() as u8])
                    .await?;
                self.stream_write.write_all(domain.as_mut()).await?;
            }
        }

        self.stream_write.write_all(&mut port.to_be_bytes()).await?;

        Ok(())
    }

    pub async fn send_data(
        &mut self,
        proxyee_id: u16,
        seq: u16,
        data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        // TODO: batch write
        self.stream_write.write_all(&mut [1u8]).await?;
        self.stream_write
            .write_all(&mut proxyee_id.to_be_bytes())
            .await?;
        self.stream_write.write_all(&mut seq.to_be_bytes()).await?;
        let len: u16 = data.len().try_into().unwrap();
        self.stream_write
            .write_all(len.to_be_bytes().as_mut())
            .await?;
        self.stream_write.write_all(data).await?;
        Ok(())
    }

    pub async fn send_ack(&mut self, proxyee_id: u16, seq: u16) -> Result<(), std::io::Error> {
        // TODO: batch write
        self.stream_write.write_all(&mut [2u8]).await?;
        self.stream_write
            .write_all(&mut proxyee_id.to_be_bytes())
            .await?;
        self.stream_write.write_all(&mut seq.to_be_bytes()).await?;
        Ok(())
    }

    pub async fn send_eof(&mut self, proxyee_id: u16, seq: u16) -> Result<(), std::io::Error> {
        // TODO: batch write
        self.stream_write.write_all(&mut [3u8]).await?;
        self.stream_write
            .write_all(&mut proxyee_id.to_be_bytes())
            .await?;
        self.stream_write.write_all(&mut seq.to_be_bytes()).await?;
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

    pub async fn recv_msg(&mut self) -> Result<Option<msg::ServerMsg>, std::io::Error> {
        let msg = self.stream_read.read_next(msg::PeekServerMsg::peek).await?;

        Ok(msg)
    }
}

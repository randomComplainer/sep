use std::net::SocketAddr;
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

    #[derive(Debug, From)]
    pub enum ServerMsg {
        Reply(#[from] Reply),
        Data(#[from] Data),
        Ack(#[from] Ack),
        Eof(#[from] Eof),
    }

    pub fn peek_server_msg(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<ServerMsg>, std::io::Error> {
        let msg_type = try_peek!(cursor.peek_u8());

        Ok(Some(match msg_type.read(cursor.get_ref()) {
            1 => try_peek!(peek_reply(cursor)?).into(),
            2 => try_peek!(peek_data(cursor)?).into(),
            3 => try_peek!(peek_ack(cursor)?).into(),
            4 => try_peek!(peek_eof(cursor)?).into(),
            x => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid msg type: {x}"),
                ));
            }
        }))
    }

    #[derive(Debug)]
    pub struct Reply {
        pub proxyee_id: u16,
        pub bound_addr: SocketAddr,
    }

    pub fn peek_reply(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<Reply>, std::io::Error> {
        let proxyee_id = try_peek!(cursor.peek_u16());
        let bound_addr = try_peek!(crate::decode::peek_socket_addr(cursor)?);
        Ok(Some(Reply {
            proxyee_id: proxyee_id.read(cursor.get_ref()),
            bound_addr,
        }))
    }

    #[derive(Debug)]
    pub struct Data {
        pub proxyee_id: u16,
        pub seq: u16,
        pub data: decode::RefSlice,
    }

    pub fn peek_data(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Data>, std::io::Error> {
        let proxyee_id = try_peek!(cursor.peek_u16());
        let seq = try_peek!(cursor.peek_u16());
        let data = try_peek!(cursor.peek_16_bit_len_slice());
        Ok(Some(Data {
            proxyee_id: proxyee_id.read(cursor.get_ref()),
            seq: seq.read(cursor.get_ref()),
            data,
        }))
    }

    #[derive(Debug)]
    pub struct Ack {
        pub proxyee_id: u16,
        pub seq: u16,
    }

    pub fn peek_ack(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Ack>, std::io::Error> {
        let proxyee_id = try_peek!(cursor.peek_u16());
        let seq = try_peek!(cursor.peek_u16());
        Ok(Some(Ack {
            proxyee_id: proxyee_id.read(cursor.get_ref()),
            seq: seq.read(cursor.get_ref()),
        }))
    }

    #[derive(Debug)]
    pub struct Eof {
        pub proxyee_id: u16,
        pub seq: u16,
    }

    pub fn peek_eof(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Eof>, std::io::Error> {
        let proxyee_id = try_peek!(cursor.peek_u16());
        let seq = try_peek!(cursor.peek_u16());
        Ok(Some(Eof {
            proxyee_id: proxyee_id.read(cursor.get_ref()),
            seq: seq.read(cursor.get_ref()),
        }))
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
            GreetedRead<Stream, ChaCha20>,
            GreetedWrite<Stream, ChaCha20>,
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
            GreetedRead::new(BufDecoder::new(EncryptedRead::new(
                stream_read,
                ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into()),
            ))),
            GreetedWrite::new(stream_write),
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
        addr_bytes: &mut [u8], // Addr + Port, in Socks5 format
    ) -> Result<(), std::io::Error> {
        // TODO: batch write
        self.stream_write.write_all(&mut [0u8]).await?;
        self.stream_write
            .write_all(&mut proxyee_id.to_be_bytes())
            .await?;
        self.stream_write.write_all(addr_bytes).await?;
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

    pub async fn recv_msg(&mut self) -> Result<(msg::ServerMsg, BytesMut), std::io::Error> {
        let (msg, buf) = self
            .stream_read
            .try_decode(msg::peek_server_msg)
            .await?
            .ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))?;

        Ok((msg, buf))
    }
}

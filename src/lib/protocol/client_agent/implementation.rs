use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use chacha20::ChaCha20;
use chacha20::cipher::KeyIvInit;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::protocol::*;

pub struct Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin,
{
    client_id: Arc<ClientId>,
    key: Arc<Key>,
    nonce: Box<Nonce>,
    stream: Stream,
    local_port: u16,
}

impl<Stream> Init<Stream>
where
    Stream: StaticStream,
{
    pub fn new(
        client_id: Arc<ClientId>,
        local_port: u16,
        key: Arc<Key>,
        nonce: Box<Nonce>,
        stream: Stream,
    ) -> Self {
        Self {
            client_id,
            key,
            nonce,
            stream,
            local_port,
        }
    }

    pub async fn send_greeting(
        self,
        timestamp: u64,
    ) -> Result<
        (
            ConnId,
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
        + rand_byte_len
        + 16; // client_id

        let mut buf = BytesMut::with_capacity(buf_size);
        buf.put_u64(timestamp);
        buf.put_slice(&rand_bytes);
        buf.put_slice(self.client_id.as_ref());

        stream_write.write_all(buf.as_mut()).await?;

        Ok((
            ConnId::new(timestamp, self.local_port),
            GreetedRead::new(BufDecoder::new(EncryptedRead::new(
                stream_read,
                ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into()),
            ))),
            GreetedWrite::new(stream_write),
        ))
    }
}

impl<Stream> super::Init for Init<Stream>
where
    Stream: StaticStream,
{
    async fn send_greeting(
        self,
        timestamp: u64,
    ) -> Result<
        (
            ConnId,
            impl protocol::MessageReader<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>,
            >,
            impl protocol::MessageWriter<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>,
            >,
        ),
        std::io::Error,
    > {
        self.send_greeting(timestamp).await
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
    type Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>;

    async fn recv_msg(&mut self) -> Result<Option<Self::Message>, decode::DecodeError> {
        let msg = self
            .stream_read
            .read_next(msg::conn::conn_msg_peeker(msg::server_msg_peeker()))
            .await?;

        Ok(msg)
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
}

impl<Stream, Cipher> protocol::MessageWriter for GreetedWrite<Stream, Cipher>
where
    Stream: AsyncWrite + Send + Unpin + 'static,
    Cipher: StaticCipher,
{
    type Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>;

    async fn send_msg(&mut self, msg: Self::Message) -> Result<(), std::io::Error> {
        // TODO: Do I need calculated size?
        let mut buf = BytesMut::with_capacity(64);

        match msg {
            protocol::msg::conn::ConnMsg::Protocol(msg) => {
                buf.put_u8(0u8);
                match msg {
                    protocol::msg::ClientMsg::SessionMsg(proxyee_id, session_msg) => {
                        buf.put_u8(0u8);
                        buf.put_u16(proxyee_id);
                        match session_msg {
                            session::msg::ClientMsg::Request(req) => {
                                buf.put_u8(0u8);
                                match &req.addr {
                                    decode::ReadRequestAddr::Ipv4(addr) => {
                                        buf.put_u8(0x01);
                                        buf.put_slice(&addr.octets());
                                    }
                                    decode::ReadRequestAddr::Ipv6(addr) => {
                                        buf.put_u8(0x04);
                                        buf.put_slice(&addr.octets());
                                    }
                                    decode::ReadRequestAddr::Domain(domain) => {
                                        buf.put_u8(0x03);
                                        buf.put_u8(domain.len() as u8);
                                        // TODO: no copy?
                                        buf.put_slice(domain.as_ref());
                                    }
                                }
                                buf.put_u16(req.port);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ClientMsg::Data(mut data) => {
                                buf.put_u8(1u8);
                                buf.put_u16(data.seq);
                                buf.put_u16(data.data.len().try_into().unwrap());
                                self.stream_write.write_all(&mut buf).await?;
                                self.stream_write.write_all(&mut data.data).await?;
                            }
                            session::msg::ClientMsg::Ack(ack) => {
                                buf.put_u8(2u8);
                                buf.put_u32(ack.bytes);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ClientMsg::Eof(eof) => {
                                buf.put_u8(3u8);
                                buf.put_u16(eof.seq);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ClientMsg::EofAck(_) => {
                                buf.put_u8(4u8);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                            session::msg::ClientMsg::ProxyeeIoError(_) => {
                                buf.put_u8(5u8);
                                self.stream_write.write_all(&mut buf).await?;
                            }
                        };
                    }
                }
            }
            protocol::msg::conn::ConnMsg::Ping => {
                buf.put_u8(1u8);
                self.stream_write.write_all(&mut buf).await?;
            }
            protocol::msg::conn::ConnMsg::EndOfStream => {
                buf.put_u8(2u8);
                self.stream_write.write_all(&mut buf).await?;
            }
        };

        Ok(())
    }
}

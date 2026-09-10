use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use chacha20::ChaCha20;
use chacha20::cipher::KeyIvInit;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::protocol::*;
use crate::*;

pub struct Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin,
{
    client_id: Arc<ClientId>,
    key: Arc<Key>,
    nonce: Box<Nonce>,
    stream: Stream,
}

impl<Stream> Init<Stream>
where
    Stream: StaticStream,
{
    pub fn new(client_id: Arc<ClientId>, key: Arc<Key>, nonce: Box<Nonce>, stream: Stream) -> Self {
        Self {
            client_id,
            key,
            nonce,
            stream,
        }
    }

    pub async fn send_greeting(
        self,
        conn_id: ConnId,
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
        + rand_byte_len
        + 16 // client_id
        + 8; // conn_id

        let mut buf = BytesMut::with_capacity(buf_size);
        buf.put_u64(timestamp);
        buf.put_slice(&rand_bytes);
        buf.put_slice(self.client_id.as_ref());
        buf.put_u64(conn_id);

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

impl<Stream> super::Init for Init<Stream>
where
    Stream: StaticStream,
{
    async fn send_greeting(
        self,
        conn_id: ConnId,
        timestamp: u64,
    ) -> Result<
        (
            impl protocol::MessageReader<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>,
            >,
            impl protocol::MessageWriter<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>,
            >,
        ),
        std::io::Error,
    > {
        self.send_greeting(conn_id, timestamp).await
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

    async fn recv_msg(&mut self) -> Result<Option<Self::Message>, std::io::Error> {
        let msg = self
            .stream_read
            .read_next(msg::conn::conn_msg_peeker(msg::server_msg_peeker()))
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
                msg::conn::conn_msg_peeker(msg::server_msg_peeker()),
                time_limit,
            )
            .await?;

        Ok(msg)
    }
}

pub struct GreetedWrite<Stream, Cipher> {
    stream_write: WriteEncrypted<Stream, Cipher>,
    main_buf: BytesMut,
}

impl<Stream, Cipher> GreetedWrite<Stream, Cipher>
where
    Stream: AsyncWrite + Unpin,
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
    Stream: AsyncWrite + Send + Unpin + 'static,
    Cipher: StaticCipher,
{
    type Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>;

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

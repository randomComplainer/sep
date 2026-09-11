use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use chacha20::ChaCha20;
use chacha20::cipher::KeyIvInit;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::codec::{EncryptedMsgWrite, MsgReader, MsgWriter};
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
    ) -> Result<(impl MsgReader<msg::ServerMsg>, impl MsgWriter), std::io::Error> {
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

        let reader = (
            BufDecoder::new(EncryptedRead::new(
                stream_read,
                ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into()),
            )),
            msg::server_msg_peeker(),
        );

        let writer = EncryptedMsgWrite::new(stream_write, 64);

        Ok((reader, writer))
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
    ) -> Result<(impl MsgReader<msg::ServerMsg>, impl MsgWriter), std::io::Error> {
        self.send_greeting(conn_id, timestamp).await
    }
}

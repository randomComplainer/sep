// Client -> Server: Greeting
//
// Server dose not send anything back,
// close connection after random delay if Greeting is invalid
//
// Client -> Server: Request
// Server -> Client: Response
// Client <-> Server: Data
// Client <-> Server: EOF (for proxied connection)

const RAND_BYTE_LEN_MAX: usize = 1024;

pub mod msg {
    pub struct Greeting {
        // plaintext
        pub nonce: Box<[u8; 12]>,
        // encrypted
        pub timestamp: u64,
        // and some random bytes, size is derived from key, nonce and timestamp
    }
}

fn get_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn cal_rand_byte_len(key: &[u8; 32], nonce: &[u8; 12], timestamp: u64) -> usize {
    let key_head = u64::from_be_bytes(key[0..8].try_into().unwrap());
    let nonce_head = u64::from_be_bytes(nonce[0..8].try_into().unwrap());
    let len_max: u64 = RAND_BYTE_LEN_MAX.try_into().unwrap();

    let len: usize = ((key_head ^ nonce_head ^ timestamp) % len_max)
        .try_into()
        .unwrap();
    len
}

pub mod client_agent {
    use bytes::{BufMut, BytesMut};
    use chacha20::ChaCha20;
    use chacha20::cipher::KeyIvInit;
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

    use crate::prelude::*;

    pub struct Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        key: Box<[u8; 32]>,
        nonce: Box<[u8; 12]>,
        stream: Stream,
    }

    impl<Stream> Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(key: Box<[u8; 32]>, nonce: Box<[u8; 12]>, stream: Stream) -> Self {
            Self { key, nonce, stream }
        }

        pub async fn send_greeting(self, timestamp: u64) -> Result<(), std::io::Error> {
            let cipher = ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into());

            let (stream_read, mut stream_write) = tokio::io::split(self.stream);

            // send nonce in plaintext
            stream_write.write_all(self.nonce.as_slice()).await?;

            let mut stream_write = EncryptedWrite::new(stream_write, cipher);

            let rand_byte_len = super::cal_rand_byte_len(&self.key, &self.nonce, timestamp);

            // TODO: actually random generating
            let rand_bytes = vec![0; rand_byte_len];

            let buf_size = 8 // timestamp
                + rand_byte_len;

            let mut buf = BytesMut::with_capacity(buf_size);
            buf.put_u64(timestamp);
            buf.put_slice(&rand_bytes);

            stream_write.write_all(buf.as_mut()).await?;

            Ok(())
        }
    }
}

pub mod server_agent {
    use chacha20::ChaCha20;
    use chacha20::cipher::KeyIvInit;
    use thiserror::Error;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

    use crate::prelude::*;

    #[derive(Error, Debug)]
    pub enum InitError {
        #[error("io error")]
        Io(#[from] std::io::Error),
        #[error("greeting invalid, {0}")]
        InvalidGreeting(&'static str),
    }

    pub struct Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        key: Box<[u8; 32]>,
        stream: Stream,
    }

    impl<Stream> Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(key: Box<[u8; 32]>, stream: Stream) -> Self {
            Self { key, stream }
        }

        pub async fn recv_greeting(self, server_timestamp: u64) -> Result<(), InitError> {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                self.recv_greeting_inner(server_timestamp),
            )
            .await
            .map_err(|_| InitError::InvalidGreeting("timeout"))?
        }

        async fn recv_greeting_inner(mut self, server_timestamp: u64) -> Result<(), InitError> {
            let mut nonce: Box<[u8; 12]> = vec![0u8; 12].try_into().unwrap();

            self.stream.read_exact(nonce.as_mut()).await?;

            let cipher = ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into());

            let (stream_read, stream_write) = tokio::io::split(self.stream);
            let stream_read = EncryptedRead::new(stream_read, cipher);
            let mut stream_read = crate::decode::BufDecoder::new(stream_read);

            let (peek_client_timestamp, timestamp_buf) = stream_read
                .try_decode(|cursor| Ok::<_, std::io::Error>(cursor.peek_u64()))
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))
                })?;

            let client_timestamp = peek_client_timestamp.read(&timestamp_buf);

            if u64::abs_diff(client_timestamp, server_timestamp) > 30 {
                return Err(InitError::InvalidGreeting("invalid timestamp"));
            }

            let rand_byte_len = super::cal_rand_byte_len(&self.key, &nonce, client_timestamp);

            if rand_byte_len > super::RAND_BYTE_LEN_MAX {
                return Err(InitError::InvalidGreeting(
                    "rand_byte_len > RAND_BYTE_LEN_MAX",
                ));
            }

            Ok(())
        }
    }
}

// Client -> Server: Greeting (nounce, timestamp, random bytes)
//
// Server dose not send anything back,
// close connection after random delay if Greeting is invalid
//
// Client -> Server: Request (addr, port)
// Server -> Client: Response
// Client <-> Server: Data
// Client <-> Server: EOF (for proxied connection)

const RAND_BYTE_LEN_MAX: usize = 1024;

fn cal_rand_byte_len(key: &[u8; 32], nonce: &[u8; 12], timestamp: u64) -> usize {
    let key_head = u64::from_be_bytes(key[0..8].try_into().unwrap());
    let nonce_head = u64::from_be_bytes(nonce[0..8].try_into().unwrap());
    let len_max: u64 = RAND_BYTE_LEN_MAX.try_into().unwrap();

    let len: usize = ((key_head ^ nonce_head ^ timestamp) % len_max)
        .try_into()
        .unwrap();
    len
}

mod msg {
    use bytes::BytesMut;
    use derive_more::From;

    use crate::decode::*;

    // nonce comes unencrypted so it's not here
    pub struct RefGreeting {
        timestamp: RefU64,
    }

    pub fn peek_greeting(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<RefGreeting>, std::io::Error> {
        let timestamp = try_peek!(cursor.peek_u64());
        Ok(Some(RefGreeting { timestamp }))
    }

    pub struct ViewGreeting(RefGreeting, BytesMut);
    impl ViewGreeting {
        pub fn new(tup: (RefGreeting, BytesMut)) -> Self {
            Self(tup.0, tup.1)
        }

        pub fn timestamp(&self) -> u64 {
            self.0.timestamp.read(self.1.as_ref())
        }
    }

    pub struct RefRequest {
        addr: RefAddr,
        port: RefU16,
    }

    pub fn peek_request(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<RefRequest>, std::io::Error> {
        let addr = try_peek!(peek_addr(cursor)?);
        let port = try_peek!(cursor.peek_u16());
        Ok(Some(RefRequest { addr, port }))
    }

    #[derive(From)]
    pub struct ViewRequest(RefRequest, BytesMut);
    impl ViewRequest {
        pub fn new(tup: (RefRequest, BytesMut)) -> Self {
            Self(tup.0, tup.1)
        }

        pub fn addr(&self) -> ViewAddr {
            self.0.addr.read(self.1.as_ref())
        }

        pub fn port(&self) -> u16 {
            self.0.port.read(self.1.as_ref())
        }
    }
}

pub mod client_agent {
    use bytes::{BufMut, BytesMut};
    use chacha20::ChaCha20;
    use chacha20::cipher::{KeyIvInit, StreamCipher};
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
    use tokio::io::{ReadHalf, WriteHalf};

    // use super::AddrRef;
    use crate::decode::*;
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

        pub async fn send_greeting(
            self,
            timestamp: u64,
        ) -> Result<Greeted<Stream, ChaCha20>, std::io::Error> {
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

            Ok(Greeted::new(
                stream_write,
                BufDecoder::new(EncryptedRead::new(
                    stream_read,
                    ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into()),
                )),
            ))
        }
    }

    pub struct Greeted<Stream, Cipher>
    where
        Cipher: StreamCipher + Unpin + 'static,
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_write: EncryptedWrite<WriteHalf<Stream>, Cipher>,
        stream_read: BufDecoder<EncryptedRead<ReadHalf<Stream>, Cipher>>,
    }

    impl<Stream, Cipher> Greeted<Stream, Cipher>
    where
        Cipher: StreamCipher + Unpin + 'static,
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(
            stream_write: EncryptedWrite<WriteHalf<Stream>, Cipher>,
            stream_read: BufDecoder<EncryptedRead<ReadHalf<Stream>, Cipher>>,
        ) -> Self {
            Self {
                stream_write,
                stream_read,
            }
        }

        pub async fn send_request(
            mut self,
            addr_bytes: &mut [u8], // Addr + Port, in Socks5 format
        ) -> Result<(), std::io::Error> {
            self.stream_write.write_all(addr_bytes).await?;
            Ok(())
        }
    }
}

pub mod server_agent {
    use chacha20::cipher::KeyIvInit;
    use chacha20::{ChaCha20, cipher::StreamCipher};
    use thiserror::Error;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadHalf, WriteHalf};

    use super::msg;
    use crate::decode::*;
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

        pub async fn recv_greeting(
            self,
            server_timestamp: u64,
        ) -> Result<Greeted<Stream, ChaCha20>, InitError> {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                self.recv_greeting_inner(server_timestamp),
            )
            .await
            .map_err(|_| InitError::InvalidGreeting("timeout"))?
        }

        async fn recv_greeting_inner(
            mut self,
            server_timestamp: u64,
        ) -> Result<Greeted<Stream, ChaCha20>, InitError> {
            let mut nonce: Box<[u8; 12]> = vec![0u8; 12].try_into().unwrap();

            self.stream.read_exact(nonce.as_mut()).await?;

            let cipher = ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into());

            let (stream_read, stream_write) = tokio::io::split(self.stream);
            let stream_read = EncryptedRead::new(stream_read, cipher);
            let mut stream_read = crate::decode::BufDecoder::new(stream_read);

            let msg =
                msg::ViewGreeting::new(stream_read.try_decode(msg::peek_greeting).await.and_then(
                    |opt| opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "")),
                )?);

            let client_timestamp = msg.timestamp();

            if u64::abs_diff(client_timestamp, server_timestamp) > 30 {
                return Err(InitError::InvalidGreeting("invalid timestamp"));
            }

            let rand_byte_len = super::cal_rand_byte_len(&self.key, &nonce, client_timestamp);

            if rand_byte_len > super::RAND_BYTE_LEN_MAX {
                return Err(InitError::InvalidGreeting(
                    "rand_byte_len > RAND_BYTE_LEN_MAX",
                ));
            }

            stream_read
                .try_decode(|cursor| Ok::<_, std::io::Error>(cursor.peek_slice(rand_byte_len)))
                .await
                .and_then(|opt| {
                    opt.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))
                })?;

            Ok(Greeted::new(
                EncryptedWrite::new(
                    stream_write,
                    ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into()),
                ),
                stream_read,
            ))
        }
    }

    pub struct Greeted<Stream, Cipher>
    where
        Cipher: StreamCipher + Unpin + 'static,
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_write: EncryptedWrite<WriteHalf<Stream>, Cipher>,
        stream_read: BufDecoder<EncryptedRead<ReadHalf<Stream>, Cipher>>,
    }

    impl<Stream, Cipher> Greeted<Stream, Cipher>
    where
        Cipher: StreamCipher + Unpin + 'static,
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(
            stream_write: EncryptedWrite<WriteHalf<Stream>, Cipher>,
            stream_read: BufDecoder<EncryptedRead<ReadHalf<Stream>, Cipher>>,
        ) -> Self {
            Self {
                stream_write,
                stream_read,
            }
        }

        pub async fn recv_request(mut self) -> Result<msg::ViewRequest, std::io::Error> {
            let view = msg::ViewRequest::new(
                self.stream_read
                    .try_decode(msg::peek_request)
                    .await
                    .and_then(|msg_opt| match msg_opt {
                        Some(msg) => Ok(msg),
                        None => {
                            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into())
                        }
                    })?,
            );

            Ok(view)
        }
    }
}

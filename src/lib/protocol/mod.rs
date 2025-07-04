use std::sync::Arc;
use std::time::SystemTime;

use chacha20::cipher::StreamCipher;
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::decode::BufDecoder;
use crate::prelude::*;

// Client -> Server: Greeting (nounce, timestamp, random bytes) Server dose not send anything back, close connection after random delay if Greeting is invalid Client -> Server: Request (addr, port)
// Server -> Client: Response
// Client <-> Server: Data
// Client <-> Server: EOF (for proxied connection)

const RAND_BYTE_LEN_MAX: usize = 1024;

pub type Key = [u8; 32];
pub type Nonce = [u8; 12];

pub fn key_from_string(s: &str) -> Box<protocol::Key> {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    let result = hasher.finalize();
    Box::new(result.into())
}

pub fn rand_nonce() -> Box<protocol::Nonce> {
    let mut nonce: Box<protocol::Nonce> = vec![0u8; 12].try_into().unwrap();
    rand::rng().fill_bytes(nonce.as_mut());
    nonce
}

pub fn get_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
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

type ReadEncrypted<S, C> = EncryptedRead<ReadHalf<S>, C>;
type WriteEncrypted<S, C> = EncryptedWrite<WriteHalf<S>, C>;
type FramedRead<S, C> = BufDecoder<ReadEncrypted<S, C>>;

pub trait StaticStream: AsyncRead + AsyncWrite + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + 'static> StaticStream for T {}

pub trait StaticCipher: StreamCipher + Unpin + 'static {}
impl<T: StreamCipher + Unpin + 'static> StaticCipher for T {}

pub mod msg {

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

        pub fn addr(&self) -> ViewAddr<'_> {
            self.0.addr.read(self.1.as_ref())
        }

        pub fn port(&self) -> u16 {
            self.0.port.read(self.1.as_ref())
        }
    }
}

pub mod client_agent {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use bytes::{BufMut, BytesMut};
    use chacha20::ChaCha20;
    use chacha20::cipher::KeyIvInit;
    use rand::RngCore;
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

    use super::*;

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
        ) -> Result<Greeted<Stream, ChaCha20>, std::io::Error> {
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
        Stream: StaticStream,
        Cipher: StaticCipher,
    {
        stream_write: WriteEncrypted<Stream, Cipher>,
        stream_read: FramedRead<Stream, Cipher>,
    }

    impl<Stream, Cipher> Greeted<Stream, Cipher>
    where
        Stream: StaticStream,
        Cipher: StaticCipher,
    {
        pub fn new(
            stream_write: WriteEncrypted<Stream, Cipher>,
            stream_read: FramedRead<Stream, Cipher>,
        ) -> Self {
            Self {
                stream_write,
                stream_read,
            }
        }

        pub async fn send_request(
            mut self,
            addr_bytes: &mut [u8], // Addr + Port, in Socks5 format
        ) -> Result<
            (
                SocketAddr,
                (FramedRead<Stream, Cipher>, WriteEncrypted<Stream, Cipher>),
            ),
            std::io::Error,
        > {
            self.stream_write.write_all(addr_bytes).await?;
            let (addr, _) = self
                .stream_read
                .try_decode(crate::decode::peek_socket_addr)
                .await
                .and_then(|msg_opt| match msg_opt {
                    Some(msg) => Ok(msg),
                    None => Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into()),
                })?;

            Ok((addr, (self.stream_read, self.stream_write)))
        }
    }
}

pub mod server_agent {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use bytes::{BufMut, BytesMut};
    use chacha20::ChaCha20;
    use chacha20::cipher::KeyIvInit;
    use thiserror::Error;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::decode::*;

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
        ) -> Result<Greeted<Stream, ChaCha20>, InitError<Stream>> {
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
        ) -> Result<Greeted<Stream, ChaCha20>, InitError<Stream>> {
            let mut nonce: Box<Nonce> = vec![0u8; 12].try_into().unwrap();

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

            let (_ref_rand_byte, _rand_byte) = stream_read
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

    pub struct Greeted<Stream, Cipher>
    where
        Stream: StaticStream,
        Cipher: StaticCipher,
    {
        stream_write: WriteEncrypted<Stream, Cipher>,
        stream_read: FramedRead<Stream, Cipher>,
    }

    impl<Stream, Cipher> Greeted<Stream, Cipher>
    where
        Stream: StaticStream,
        Cipher: StaticCipher,
    {
        pub fn new(
            stream_write: WriteEncrypted<Stream, Cipher>,
            stream_read: FramedRead<Stream, Cipher>,
        ) -> Self {
            Self {
                stream_write,
                stream_read,
            }
        }

        pub async fn recv_request(
            mut self,
        ) -> Result<(msg::ViewRequest, RequestAccepted<Stream, Cipher>), std::io::Error> {
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

            Ok((
                view,
                RequestAccepted::new(self.stream_write, self.stream_read),
            ))
        }
    }

    pub struct RequestAccepted<Stream, Cipher>
    where
        Stream: StaticStream,
        Cipher: StaticCipher,
    {
        stream_write: WriteEncrypted<Stream, Cipher>,
        stream_read: FramedRead<Stream, Cipher>,
    }

    impl<Stream, Cipher> RequestAccepted<Stream, Cipher>
    where
        Stream: StaticStream,
        Cipher: StaticCipher,
    {
        pub fn new(
            stream_write: WriteEncrypted<Stream, Cipher>,
            stream_read: FramedRead<Stream, Cipher>,
        ) -> Self {
            Self {
                stream_write,
                stream_read,
            }
        }

        pub async fn reply(
            mut self,
            bound_addr: std::net::SocketAddr,
        ) -> Result<(FramedRead<Stream, Cipher>, WriteEncrypted<Stream, Cipher>), std::io::Error>
        {
            let mut buf = match bound_addr {
                std::net::SocketAddr::V4(addr) => {
                    let mut buf = BytesMut::with_capacity(1 + 4 + 2);
                    buf.put_u8(0x01);
                    buf.put_u32(addr.ip().to_bits());
                    buf.put_u16(addr.port());
                    buf
                }
                std::net::SocketAddr::V6(addr) => {
                    let mut buf = BytesMut::with_capacity(1 + 16 + 2);
                    buf.put_u8(0x04);
                    buf.put_slice(&addr.ip().octets());
                    buf.put_u16(addr.port());
                    buf
                }
            };

            self.stream_write.write_all(buf.as_mut()).await?;

            Ok((self.stream_read, self.stream_write))
        }
    }
}

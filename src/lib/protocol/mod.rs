use std::sync::Arc;
use std::time::SystemTime;

use chacha20::cipher::StreamCipher;
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::decode::BufDecoder;
use crate::prelude::*;

pub mod client_agent;
pub mod server_agent;

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


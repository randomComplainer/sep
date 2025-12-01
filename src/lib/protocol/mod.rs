use std::time::SystemTime;

use chacha20::cipher::StreamCipher;
use rand::RngCore;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::decode::BufDecoder;
use crate::prelude::*;

pub mod client_agent;
pub mod msg;
pub mod server_agent;

// Client -> Server: Greeting (nounce, timestamp, random bytes) Server dose not send anything back, close connection after random delay if Greeting is invalid Client -> Server: Request (addr, port)
// Server -> Client: Response
// Client <-> Server: Data
// Client <-> Server: EOF (for proxied connection)

const RAND_BYTE_LEN_MAX: usize = 1024;

pub type Key = [u8; 32];
pub type Nonce = [u8; 12];
pub type ClientId = [u8; 16];

#[derive(Error, Debug)]
#[error("protocol error: {0}")]
pub struct ProtocolError(String);
impl From<String> for ProtocolError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

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

pub trait StaticStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> StaticStream for T {}

pub trait StaticCipher: StreamCipher + Send + Unpin + 'static {}
impl<T: StreamCipher + Unpin + Send + 'static> StaticCipher for T {}

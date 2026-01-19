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

pub trait MessageWriter
where
    Self: Unpin + Send + 'static,
{
    type Message: Send;

    fn send_msg(
        &mut self,
        msg: Self::Message,
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

pub trait MessageReader
where
    Self: Unpin + Send + 'static,
{
    type Message: Send;

    fn recv_msg(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Message>, DecodeError>> + Send;
}

#[derive(PartialEq, Eq, Copy, Clone, Hash)]
pub struct SessionId {
    timestamp: u64,
    proxyee_port: u16,
}

impl std::fmt::Debug for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.timestamp)
            .field(&self.proxyee_port)
            .finish()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.timestamp)
            .field(&self.proxyee_port)
            .finish()
    }
}

impl SessionId {
    pub fn new(timestamp: u64, proxyee_port: u16) -> Self {
        Self {
            timestamp,
            proxyee_port,
        }
    }

    pub fn from_port(proxyee_port: u16) -> Self {
        Self {
            timestamp: protocol::get_timestamp(),
            proxyee_port,
        }
    }
}
#[derive(Clone, Copy)]
pub struct ConnId {
    pub timestamp: u64,
    pub client_port: u16,
}

impl std::fmt::Debug for ConnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.timestamp)
            .field(&self.client_port)
            .finish()
    }
}

impl ConnId {
    pub fn new(timestamp: u64, client_port: u16) -> Self {
        Self {
            timestamp,
            client_port,
        }
    }
}

type ReadEncrypted<S, C> = EncryptedRead<ReadHalf<S>, C>;
type WriteEncrypted<S, C> = EncryptedWrite<WriteHalf<S>, C>;
type FramedRead<S, C> = BufDecoder<ReadEncrypted<S, C>>;

pub trait StaticStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> StaticStream for T {}

pub trait StaticCipher: StreamCipher + Send + Unpin + 'static {}
impl<T: StreamCipher + Unpin + Send + 'static> StaticCipher for T {}

pub mod test_utils {
    use std::sync::Arc;

    use chacha20::ChaCha20;
    use tokio::io::{DuplexStream, duplex};

    use crate::prelude::*;
    use protocol::{client_agent, server_agent};

    pub fn create_init_pair() -> (
        protocol::client_agent::implementation::Init<DuplexStream>,
        protocol::server_agent::implementation::Init<DuplexStream>,
    ) {
        let key: Arc<protocol::Key> = protocol::key_from_string("000").into();
        let nonce: Box<protocol::Nonce> = vec![1u8; 12].try_into().unwrap();
        let client_id: Arc<protocol::ClientId> = [1u8; 16].into();

        let (client_steam, server_stream) = duplex(8 * 1024);

        let client_agent = protocol::client_agent::implementation::Init::new(
            client_id,
            0,
            key.clone(),
            nonce,
            client_steam,
        );
        let server_agent =
            protocol::server_agent::implementation::Init::new(key, server_stream, 10);

        (client_agent, server_agent)
    }

    pub async fn create_greeted_pair() -> (
        (
            client_agent::implementation::GreetedRead<DuplexStream, ChaCha20>,
            client_agent::implementation::GreetedWrite<DuplexStream, ChaCha20>,
        ),
        (
            Box<protocol::ClientId>,
            server_agent::implementation::GreetedRead<DuplexStream, ChaCha20>,
            server_agent::implementation::GreetedWrite<DuplexStream, ChaCha20>,
        ),
    ) {
        let (client_agent, server_agent) = create_init_pair();
        let client_agent = client_agent.send_greeting(12).await.unwrap();
        let server_agent = server_agent.recv_greeting(12).await.unwrap();

        (
            (client_agent.1, client_agent.2),
            (server_agent.0, server_agent.2, server_agent.3),
        )
    }
}

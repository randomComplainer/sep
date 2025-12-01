use thiserror::Error;

use crate::prelude::*;
use protocol::*;

pub mod implementation;

#[derive(Error, Debug)]
pub enum InitError<Stream> {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("greeting invalid, {0}")]
    InvalidGreeting(&'static str, Stream),
    #[error("protocol error: {0}")]
    Protocol(#[from] protocol::ProtocolError),
}

impl<Stream> InitError<Stream> {
    pub fn from_decode_error(err: DecodeError) -> Self {
        match err {
            DecodeError::InvalidStream(str) => InitError::Protocol(str.into()),
            DecodeError::Io(err) => InitError::Io(err),
        }
    }
}

pub trait Init {
    type Stream;
    type GreetedRead;
    type GreetedWrite;

    async fn recv_greeting(
        self,
        server_timestamp: u64,
    ) -> Result<(Box<ClientId>, Self::GreetedRead, Self::GreetedWrite), InitError<Self::Stream>>;
}

pub trait GreetedRead
where
    Self: Send + 'static,
{
    fn recv_msg(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<protocol::msg::ClientMsg>, DecodeError>> + Send;
}

pub trait GreetedWrite
where
    Self: Send + 'static,
{
    fn send_msg(
        &mut self,
        msg: protocol::msg::ServerMsg,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}

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
    ) -> Result<
        (Box<ClientId>, ConnId, Self::GreetedRead, Self::GreetedWrite),
        InitError<Self::Stream>,
    >;
}

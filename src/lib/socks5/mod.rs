use thiserror::Error;

use crate::prelude::*;

pub mod msg;
pub mod server_agent;

#[derive(Error, Debug)]
pub enum Socks5Error {
    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("Error at '{context}': {source}")]
    Contextualized {
        context: &'static str,
        #[source]
        source: Box<Socks5Error>,
    },
}

impl Into<std::io::Error> for Socks5Error {
    fn into(self) -> std::io::Error {
        match self {
            Socks5Error::Io(err) => err,
            Socks5Error::Protocol(err) => std::io::Error::new(std::io::ErrorKind::Other, err),
            Socks5Error::Contextualized { source, .. } => Into::<std::io::Error>::into(*source),
        }
    }
}

impl From<String> for Socks5Error {
    fn from(s: String) -> Self {
        Self::Protocol(s)
    }
}

impl Socks5Error {
    pub fn from_decode_error(err: DecodeError) -> Self {
        match err {
            DecodeError::InvalidStream(str) => Socks5Error::Protocol(str),
            DecodeError::Io(err) => Socks5Error::Io(err),
        }
    }
}

pub trait Contextualize<Ok> {
    fn contextualize_err(self, context: &'static str) -> Result<Ok, Socks5Error>;
}

impl<Ok, Err> Contextualize<Ok> for Result<Ok, Err>
where
    Err: Into<Socks5Error>,
{
    fn contextualize_err(self, context: &'static str) -> Result<Ok, Socks5Error> {
        self.map_err(|err| Socks5Error::Contextualized {
            context,
            source: Box::new(err.into()),
        })
    }
}

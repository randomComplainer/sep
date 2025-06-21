use std::net::SocketAddr;

use bytes::{Buf, BytesMut};
use futures::StreamExt;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, FramedRead};

pub mod msg {
    use super::*;
    #[derive(Debug)]
    pub struct ClientGreeding {
        pub ver: u8,
        pub methods: Vec<u8>,
    }

    pub struct ClientGreedingDecoder;

    impl Decoder for ClientGreedingDecoder {
        type Item = ClientGreeding;
        type Error = Socks5Error;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.len() < 2 {
                return Ok(None); // not enough data yet
            }

            let mut buf = &src[..];
            let ver = buf.get_u8();
            let nmethods = buf.get_u8() as usize;

            if src.len() < 2 + nmethods {
                return Ok(None); // wait for more data
            }

            let methods = src[2..2 + nmethods].to_vec();

            // Advance the buffer
            src.advance(2 + nmethods);

            Ok(Some(ClientGreeding { ver, methods }))
        }
    }
}

#[derive(Error, Debug)]
pub enum Socks5Error {
    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("Error at '{context}': {source}")]
    Contextualized {
        context: &'static str,
        #[source]
        source: Box<Socks5Error>,
    },
}

impl Socks5Error {
    pub fn contextualize(context: &'static str) -> impl Fn(Self) -> Self {
        move |source| Self::Contextualized {
            context,
            source: Box::new(source),
        }
    }
}

pub mod agent {
    use super::*;

    pub struct Init {
        stream: TcpStream,
    }

    impl Init {
        pub fn new(stream: TcpStream) -> Self {
            Self { stream }
        }

        pub async fn receive_greeting_message(
            mut self,
        ) -> Result<(msg::ClientGreeding, ClientMethodSent), Socks5Error> {
            let mut framed = FramedRead::new(&mut self.stream, msg::ClientGreedingDecoder);

            let greeting_msg = framed
                .next()
                .await
                .ok_or(std::io::Error::other("unexpected eof"))?
                .map_err(Socks5Error::contextualize(
                    "receving client greeting message",
                ))?;

            Ok((greeting_msg, ClientMethodSent {}))
        }
    }

    pub struct ClientMethodSent {}

    pub struct Socks5Listener {
        inner: tokio::net::TcpListener,
    }

    impl Socks5Listener {
        pub async fn bind(addr: SocketAddr) -> Result<Self, Socks5Error> {
            let inner = tokio::net::TcpListener::bind(addr).await?;
            Ok(Self { inner })
        }

        pub async fn accept(&self) -> Result<Init, Socks5Error> {
            let (stream, _) = self.inner.accept().await?;
            Ok(Init::new(stream))
        }
    }
}

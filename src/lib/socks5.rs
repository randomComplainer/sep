use std::net::SocketAddr;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct ClientMethodSelectionMessage {
    pub ver: u8,
    pub methods: Vec<u8>,
}

#[derive(Error, Debug)]
pub enum Socks5Error {
    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("Step '{step}' failed: {source}")]
    StepError {
        step: &'static str,
        #[source]
        source: Box<Socks5Error>,
    },
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

        pub async fn receive_method_selection_message(
            mut self,
        ) -> Result<(ClientMethodSelectionMessage, ClientMethodSent), Socks5Error> {
            let ver = self.stream.read_u8().await?;
            let n_methods = self.stream.read_u8().await?;
            let mut methods = Vec::with_capacity(n_methods as usize);
            self.stream.read_exact(&mut methods).await?;

            Ok((
                ClientMethodSelectionMessage { ver, methods },
                ClientMethodSent {},
            ))
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

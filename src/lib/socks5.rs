use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::prelude::*;
use decode::BufDecoder;

pub mod msg {
    use super::*;
    use crate::decode::*;

    crate::peek_type! {
        #[derive(Debug)]
        pub struct ClientGreeting, PeekClientGreeting {
            ver: PeekU8::peek => PeekU8,
            methods: PeekSlice::peek_u8_len => PeekSlice,
        }
    }

    pub struct MethodSelection {
        pub ver: u8,
        pub method: u8,
    }

    pub fn encode_method_selection(item: MethodSelection) -> Result<BytesMut, Socks5Error> {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u8(item.ver);
        buf.put_u8(item.method);
        return Ok(buf);
    }

    crate::peek_type! {
        pub struct ClientRequest, PeekClientRequest {
            ver: PeekU8::peek => PeekU8,
            cmd: PeekU8::peek => PeekU8,
            rsv: PeekU8::peek => PeekU8,
            addr: PeekReadRequestAddr::peek => PeekReadRequestAddr,
            port: PeekU16::peek => PeekU16,
        }
    }

    pub struct Reply<'a> {
        pub ver: u8,
        pub rep: u8,
        pub rsv: u8,
        pub addr: &'a SocketAddr,
    }

    pub fn encode_reply(item: &Reply) -> Result<BytesMut, std::io::Error> {
        let mut buf = BytesMut::with_capacity(4 + 16 + 2);
        buf.put_u8(item.ver);
        buf.put_u8(item.rep);
        buf.put_u8(item.rsv);
        match item.addr {
            SocketAddr::V4(addr) => {
                buf.put_u8(0x01);
                buf.put_u32(addr.ip().to_bits());
                buf.put_u16(addr.port());
            }
            SocketAddr::V6(addr) => {
                buf.put_u8(0x04);
                buf.put_slice(&addr.ip().octets());
                buf.put_u16(addr.port());
            }
        };

        return Ok(buf);
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

pub mod agent {
    use std::net::SocketAddr;

    use tokio::io::AsyncRead;
    use tokio::io::AsyncWrite;
    use tokio::io::ReadHalf;
    use tokio::io::WriteHalf;

    use super::msg::*;
    use super::*;

    pub struct Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_read: BufDecoder<ReadHalf<Stream>>,
        stream_write: WriteHalf<Stream>,
    }

    impl<Stream> Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(stream: Stream) -> Self {
            let (stream_read, stream_write) = tokio::io::split(stream);

            Self {
                stream_read: BufDecoder::new(stream_read),
                stream_write,
            }
        }

        pub async fn receive_greeting_message(
            mut self,
        ) -> Result<(msg::ClientGreeting, Greeted<Stream>), Socks5Error> {
            let greeting_msg = self
                .stream_read
                .read_next(PeekClientGreeting::peek)
                .await
                .and_then(|msg_opt| match msg_opt {
                    Some(msg) => Ok(msg),
                    None => Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into()),
                })
                .contextualize_err("receving client greeting message")?;

            Ok((
                greeting_msg,
                Greeted::new(self.stream_read, self.stream_write),
            ))
        }
    }

    pub struct Greeted<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_read: BufDecoder<ReadHalf<Stream>>,
        stream_write: WriteHalf<Stream>,
    }

    impl<Stream> Greeted<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(
            stream_read: BufDecoder<ReadHalf<Stream>>,
            stream_write: WriteHalf<Stream>,
        ) -> Self {
            Self {
                stream_read,
                stream_write,
            }
        }

        pub async fn send_method_selection_message(
            mut self,
            method: u8,
        ) -> Result<(ClientRequest, Requested<Stream>), Socks5Error> {
            let buf = msg::encode_method_selection(msg::MethodSelection { ver: 5, method })?;
            self.stream_write
                .write_all(buf.as_ref())
                .await
                .contextualize_err("sending method selection message")?;

            let req_msg = self
                .stream_read
                .read_next(PeekClientRequest::peek)
                .await
                .and_then(|msg_opt| match msg_opt {
                    Some(msg) => Ok(msg),
                    None => Err(std::io::Error::other("unexpected eof").into()),
                })
                .contextualize_err("receving request message")?;

            Ok((req_msg, Requested::new(self.stream_read, self.stream_write)))
        }
    }

    pub struct Requested<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_read: BufDecoder<ReadHalf<Stream>>,
        stream_write: WriteHalf<Stream>,
    }

    impl<Stream> Requested<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(
            stream_read: BufDecoder<ReadHalf<Stream>>,
            stream_write: WriteHalf<Stream>,
        ) -> Self {
            Self {
                stream_read,
                stream_write,
            }
        }

        pub async fn reply(
            mut self,
            bound_addr: &SocketAddr,
        ) -> Result<(BufDecoder<ReadHalf<Stream>>, WriteHalf<Stream>), std::io::Error> {
            let buf = msg::encode_reply(&msg::Reply {
                ver: 5,
                rep: 0,
                rsv: 0,
                addr: bound_addr,
            })?;

            self.stream_write.write_all(buf.as_ref()).await?;

            Ok((self.stream_read, self.stream_write))
        }
    }

    pub struct Socks5Listener {
        inner: tokio::net::TcpListener,
    }

    impl Socks5Listener {
        pub async fn bind(addr: SocketAddr) -> Result<Self, Socks5Error> {
            let inner = tokio::net::TcpListener::bind(addr).await?;
            Ok(Self { inner })
        }

        pub async fn accept(&self) -> Result<(Init<TcpStream>, SocketAddr), Socks5Error> {
            let (stream, addr) = self.inner.accept().await?;
            Ok((Init::new(stream), addr))
        }
    }
}

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

    #[derive(Debug)]
    pub struct ClientGreeting {
        pub ver: u8,
        pub methods: BytesMut,
    }

    pub struct ClientGreetingReader {
        pub ver: U8Reader,
        pub methods: SliceReader,
    }

    impl Reader for ClientGreetingReader {
        type Value = ClientGreeting;
        fn read(&self, buf: &mut BytesMut) -> ClientGreeting {
            ClientGreeting {
                ver: self.ver.read(buf),
                methods: self.methods.read(buf),
            }
        }
    }

    pub fn client_greeting_peeker() -> impl Peeker<ClientGreeting, Reader = ClientGreetingReader> {
        peek::wrap(|cursor| {
            Ok(Some(ClientGreetingReader {
                ver: crate::peek!(u8_peeker().peek(cursor)),
                methods: crate::peek!(slice_peeker_u8_len().peek(cursor)),
            }))
        })
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

    #[derive(Debug)]
    pub struct ClientRequest {
        pub ver: u8,
        pub cmd: u8,
        pub rsv: u8,
        pub addr: ReadRequestAddr,
        pub port: u16,
    }

    pub struct ClientRequestReader {
        pub ver: U8Reader,
        pub cmd: U8Reader,
        pub rsv: U8Reader,
        pub addr: ReadRequestAddrReader,
        pub port: U16Reader,
    }

    impl Reader for ClientRequestReader {
        type Value = ClientRequest;
        fn read(&self, buf: &mut BytesMut) -> ClientRequest {
            ClientRequest {
                ver: self.ver.read(buf),
                cmd: self.cmd.read(buf),
                rsv: self.rsv.read(buf),
                addr: self.addr.read(buf),
                port: self.port.read(buf),
            }
        }
    }

    pub fn client_request_peeker() -> impl Peeker<ClientRequest, Reader = ClientRequestReader> {
        peek::wrap(|cursor| {
            Ok(Some(ClientRequestReader {
                ver: crate::peek!(u8_peeker().peek(cursor)),
                cmd: crate::peek!(u8_peeker().peek(cursor)),
                rsv: crate::peek!(u8_peeker().peek(cursor)),
                addr: crate::peek!(request_addr_peeker().peek(cursor)),
                port: crate::peek!(u16_peeker().peek(cursor)),
            }))
        })
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

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("Error at '{context}': {source}")]
    Contextualized {
        context: &'static str,
        #[source]
        source: Box<Socks5Error>,
    },
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
                .read_next(msg::client_greeting_peeker())
                .await
                .map_err(Socks5Error::from_decode_error)
                .and_then(|msg_opt| match msg_opt {
                    Some(msg) => Ok(msg),
                    None => Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into()),
                })
                .contextualize_err("receving proxyee greeting message")?;

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
                .read_next(msg::client_request_peeker())
                .await
                .map_err(Socks5Error::from_decode_error)
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

        pub async fn reply_error(mut self, err_code: u8) -> Result<(), std::io::Error> {
            let buf = msg::encode_reply(&msg::Reply {
                ver: 5,
                rep: err_code,
                rsv: 0,
                addr: &SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                    0,
                ),
            })?;

            self.stream_write.write_all(buf.as_ref()).await?;

            Ok(())
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

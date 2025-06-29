use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::decode::*;

pub mod msg {
    use super::*;

    pub struct RefClientGreeding {
        pub ver: RefU8,
        pub methods: RefSlice,
    }

    pub fn peek_client_greeding_message(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<RefClientGreeding>, Socks5Error> {
        let ver = try_peek!(cursor.peek_u8());
        let methods = try_peek!(cursor.peek_oct_len_slice());

        Ok(Some(RefClientGreeding { ver, methods }))
    }

    pub struct ViewClientGreeding(RefClientGreeding, BytesMut);

    impl ViewClientGreeding {
        pub fn new(greeting: RefClientGreeding, msg_bytes: BytesMut) -> Self {
            Self(greeting, msg_bytes)
        }

        pub fn ver(&self) -> u8 {
            self.0.ver.read(self.1.as_ref())
        }

        pub fn methods(&self) -> &[u8] {
            self.0.methods.read(self.1.as_ref())
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

    pub struct RefRequest {
        pub ver: RefU8,
        pub cmd: RefU8,
        pub rsv: RefU8,
        pub addr: RefAddr,
        pub port: RefU16,
    }

    pub fn peek_request(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<RefRequest>, Socks5Error> {
        let ver = try_peek!(cursor.peek_u8());
        let cmd = try_peek!(cursor.peek_u8());
        let rsv = try_peek!(cursor.peek_u8());
        let addr = try_peek!(peek_addr(cursor)?);
        let port = try_peek!(cursor.peek_u16());

        Ok(Some(RefRequest {
            ver,
            cmd,
            rsv,
            addr,
            port,
        }))
    }

    pub struct ViewRequest(RefRequest, BytesMut);
    impl ViewRequest {
        pub fn new(request: RefRequest, msg_bytes: BytesMut) -> Self {
            Self(request, msg_bytes)
        }

        pub fn ver(&self) -> u8 {
            self.0.ver.read(self.1.as_ref())
        }

        pub fn cmd(&self) -> u8 {
            self.0.cmd.read(self.1.as_ref())
        }

        pub fn rsv(&self) -> u8 {
            self.0.rsv.read(self.1.as_ref())
        }

        pub fn addr(&self) -> ViewAddr {
            self.0.addr.read(self.1.as_ref())
        }

        pub fn port(&self) -> u16 {
            self.0.port.read(self.1.as_ref())
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
        ) -> Result<(msg::ViewClientGreeding, Greeted<Stream>), Socks5Error> {
            let (greeting_msg, msg_bytes) = self
                .stream_read
                .try_decode(peek_client_greeding_message)
                .await
                .and_then(|msg_opt| match msg_opt {
                    Some(msg) => Ok(msg),
                    None => Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into()),
                })
                .contextualize_err("receving client greeting message")?;

            Ok((
                ViewClientGreeding::new(greeting_msg, msg_bytes),
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
        ) -> Result<(ViewRequest, Requested<Stream>), Socks5Error> {
            let buf = msg::encode_method_selection(msg::MethodSelection { ver: 5, method })?;
            self.stream_write
                .write_all(buf.as_ref())
                .await
                .contextualize_err("sending method selection message")?;

            self.stream_write
                .write_all(&buf)
                .await
                .contextualize_err("sending method selection message")?;

            let (req_msg, msg_bytes) = self
                .stream_read
                .try_decode(peek_request)
                .await
                .and_then(|msg_opt| match msg_opt {
                    Some(msg) => Ok(msg),
                    None => Err(std::io::Error::other("unexpected eof").into()),
                })
                .contextualize_err("receving request message")?;

            Ok((
                ViewRequest::new(req_msg, msg_bytes),
                Requested::new(self.stream_read, self.stream_write),
            ))
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
    }

    pub struct Socks5Listener {
        inner: tokio::net::TcpListener,
    }

    impl Socks5Listener {
        pub async fn bind(addr: SocketAddr) -> Result<Self, Socks5Error> {
            let inner = tokio::net::TcpListener::bind(addr).await?;
            Ok(Self { inner })
        }

        pub async fn accept(&self) -> Result<Init<TcpStream>, Socks5Error> {
            let (stream, _) = self.inner.accept().await?;
            Ok(Init::new(stream))
        }
    }
}

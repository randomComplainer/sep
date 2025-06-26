use std::net::SocketAddr;

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::Encoder;

#[macro_use]
pub mod try_decode {
    use super::*;
    use std::io::Cursor;

    pub struct BufReader<Stream>
    where
        Stream: tokio::io::AsyncRead + 'static + Unpin,
    {
        inner: Stream,
        buf: BytesMut,
    }

    impl<Stream> BufReader<Stream>
    where
        Stream: tokio::io::AsyncRead + Unpin,
    {
        const BUF_SIZE: usize = 1024 * 4;

        pub fn new(inner: Stream) -> Self {
            Self {
                inner,
                buf: BytesMut::with_capacity(Self::BUF_SIZE),
            }
        }

        pub async fn read_next<T, E>(
            &mut self,
            decode: impl Fn(&mut BytesMut) -> Result<Option<T>, E>,
        ) -> Result<Option<T>, E>
        where
            E: From<std::io::Error>,
        {
            match (decode)(&mut self.buf) {
                Ok(Some(item)) => return Ok(Some(item)),
                Ok(None) => {}
                Err(err) => return Err(err),
            }

            loop {
                self.buf.reserve(1);
                let n = self.inner.read_buf(&mut self.buf).await?;

                if n == 0 {
                    return Ok(None);
                }

                match (decode)(&mut self.buf) {
                    Ok(Some(item)) => return Ok(Some(item)),
                    Ok(None) => {}
                    Err(err) => return Err(err),
                }
            }
        }

        pub async fn try_decode<T, E>(
            &mut self,
            peek_fn: impl Fn(&mut Cursor<&[u8]>) -> Result<Option<T>, E>,
        ) -> Result<Option<(T, BytesMut)>, E>
        where
            E: From<std::io::Error>,
        {
            self.read_next(move |bytes| {
                let mut cursor = Cursor::new(bytes.as_ref());
                let peeked = peek_fn(&mut cursor);
                let pos = cursor.position();
                peeked.map(|peeked| {
                    peeked.map(|peeked| {
                        let size: usize = pos.try_into().unwrap();
                        (peeked, bytes.split_to(size))
                    })
                })
            })
            .await
        }
    }

    #[macro_export]
    macro_rules! peek {
        ($value:expr) => {
            match $value {
                Some(result) => result,
                None => return None,
            }
        };
    }

    #[macro_export]
    macro_rules! try_peek {
        ($value:expr) => {
            match $value {
                Some(result) => result,
                None => return Ok(None),
            }
        };
    }

    pub enum DecodeError {
        NotEnoughData,
    }

    pub trait Peek {
        fn peek_u8(&mut self) -> Option<PeekU8>;
        fn peek_u16(&mut self) -> Option<PeekU16>;
        fn peek_slice(&mut self, len: usize) -> Option<PeekSlice>;
        fn peek_oct_len_slice(&mut self) -> Option<PeekSlice>;
    }

    impl Peek for Cursor<&[u8]> {
        fn peek_u8(&mut self) -> Option<PeekU8> {
            if self.remaining() < 1 {
                None
            } else {
                let result = Some(PeekU8 {
                    offset: self.position().try_into().unwrap(),
                });
                self.advance(1);
                result
            }
        }

        fn peek_u16(&mut self) -> Option<PeekU16> {
            if self.remaining() < 2 {
                None
            } else {
                let result = Some(PeekU16 {
                    offset: self.position().try_into().unwrap(),
                });
                self.advance(2);
                result
            }
        }

        fn peek_slice(&mut self, len: usize) -> Option<PeekSlice> {
            if self.remaining() < len {
                None
            } else {
                let result = Some(PeekSlice {
                    offset: self.position().try_into().unwrap(),
                    len,
                });
                self.advance(len);
                result
            }
        }

        fn peek_oct_len_slice(&mut self) -> Option<PeekSlice> {
            let len = peek!(self.peek_u8());
            self.peek_slice(len.read(self.get_ref()) as usize)
        }
    }

    pub struct PeekU8 {
        pub offset: usize,
    }
    impl PeekU8 {
        pub fn read(&self, bytes: &[u8]) -> u8 {
            bytes[self.offset]
        }
    }

    pub struct PeekU16 {
        pub offset: usize,
    }

    pub struct PeekSlice {
        pub offset: usize,
        pub len: usize,
    }

    impl PeekSlice {
        pub fn read<'bytes>(&self, bytes: &'bytes [u8]) -> &'bytes [u8] {
            &bytes[self.offset..self.offset + self.len]
        }

        pub fn read_as_str<'bytes>(&self, bytes: &'bytes [u8]) -> &'bytes str {
            std::str::from_utf8(&bytes[self.offset..self.offset + self.len]).unwrap()
        }
    }
}

pub mod msg {
    use super::try_decode::*;
    use super::*;

    pub struct PeekClientGreedingMessage {
        pub ver: PeekU8,
        pub methods: PeekSlice,
    }

    pub fn peek_client_greeding_message(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<PeekClientGreedingMessage>, Socks5Error> {
        let ver = try_peek!(cursor.peek_u8());
        let methods = try_peek!(cursor.peek_oct_len_slice());

        Ok(Some(PeekClientGreedingMessage { ver, methods }))
    }

    pub struct MethodSelection {
        pub ver: u8,
        pub method: u8,
    }

    pub struct MethodSelectionEncoder;

    impl Encoder<MethodSelection> for MethodSelectionEncoder {
        type Error = Socks5Error;

        fn encode(&mut self, item: MethodSelection, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.put_u8(item.ver);
            dst.put_u8(item.method);

            return Ok(());
        }
    }

    pub struct PeekIpv4Addr {
        pub offset: usize,
    }

    impl PeekIpv4Addr {
        pub fn read(&self, bytes: &[u8]) -> std::net::Ipv4Addr {
            let raw = &bytes[self.offset..self.offset + 4];
            std::net::Ipv4Addr::from(<[u8; 4]>::try_from(raw).unwrap())
        }
    }

    pub struct PeekIpv6Addr {
        pub offset: usize,
    }

    impl PeekIpv6Addr {
        pub fn read(&self, bytes: &[u8]) -> std::net::Ipv6Addr {
            let raw = &bytes[self.offset..self.offset + 16];
            std::net::Ipv6Addr::from(<[u8; 16]>::try_from(raw).unwrap())
        }
    }

    pub enum PeekAddr {
        Ipv4(PeekIpv4Addr),
        Ipv6(PeekIpv6Addr),
        Domain(PeekSlice),
    }

    impl PeekAddr {
        pub fn format<'bytes>(&self, bytes: &'bytes [u8]) -> String {
            match self {
                PeekAddr::Ipv4(addr) => addr.read(bytes).to_string(),
                PeekAddr::Ipv6(addr) => addr.read(bytes).to_string(),
                PeekAddr::Domain(addr) => String::from(addr.read_as_str(bytes)),
            }
        }
    }

    pub fn peek_addr(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<PeekAddr>, Socks5Error> {
        let atyp = try_peek!(cursor.peek_u8()).read(cursor.get_ref());
        Ok(Some(match atyp {
            1 => PeekAddr::Ipv4(PeekIpv4Addr {
                offset: cursor.position().try_into().unwrap(),
            }),

            4 => PeekAddr::Ipv6(PeekIpv6Addr {
                offset: cursor.position().try_into().unwrap(),
            }),
            3 => PeekAddr::Domain(try_peek!(cursor.peek_oct_len_slice())),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid address type",
                )
                .into());
            }
        }))
    }

    pub struct PeekRequest {
        pub ver: PeekU8,
        pub cmd: PeekU8,
        pub rsv: PeekU8,
        pub addr: PeekAddr,
        pub port: PeekU16,
    }

    pub fn peek_request(
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<PeekRequest>, Socks5Error> {
        let ver = try_peek!(cursor.peek_u8());
        let cmd = try_peek!(cursor.peek_u8());
        let rsv = try_peek!(cursor.peek_u8());
        let addr = try_peek!(peek_addr(cursor)?);
        let port = try_peek!(cursor.peek_u16());

        Ok(Some(PeekRequest {
            ver,
            cmd,
            rsv,
            addr,
            port,
        }))
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
    use super::try_decode::*;
    use super::*;

    pub struct Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_read: BufReader<ReadHalf<Stream>>,
        stream_write: WriteHalf<Stream>,
    }

    impl<Stream> Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(stream: Stream) -> Self {
            let (stream_read, stream_write) = tokio::io::split(stream);

            Self {
                stream_read: BufReader::new(stream_read),
                stream_write,
            }
        }

        pub async fn receive_greeting_message(
            mut self,
        ) -> Result<(msg::PeekClientGreedingMessage, BytesMut, Greeted<Stream>), Socks5Error>
        {
            let (greeting_msg, msg_bytes) = self
                .stream_read
                .try_decode(peek_client_greeding_message)
                .await
                .and_then(|msg_opt| match msg_opt {
                    Some(msg) => Ok(msg),
                    None => Err(std::io::Error::other("unexpected eof").into()),
                })
                .contextualize_err("receving client greeting message")?;

            Ok((
                greeting_msg,
                msg_bytes,
                Greeted::new(self.stream_read, self.stream_write),
            ))
        }
    }

    pub struct Greeted<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_read: BufReader<ReadHalf<Stream>>,
        stream_write: WriteHalf<Stream>,
    }

    impl<Stream> Greeted<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(
            stream_read: BufReader<ReadHalf<Stream>>,
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
        ) -> Result<(msg::PeekRequest, BytesMut, Requested<Stream>), Socks5Error> {
            let mut buf = BytesMut::new();
            msg::MethodSelectionEncoder
                .encode(msg::MethodSelection { ver: 5, method }, &mut buf)
                .contextualize_err("encoding method selection message")?;

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
                req_msg,
                msg_bytes,
                Requested::new(self.stream_read, self.stream_write),
            ))
        }
    }

    pub struct Requested<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        stream_read: BufReader<ReadHalf<Stream>>,
        stream_write: WriteHalf<Stream>,
    }

    impl<Stream> Requested<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(
            stream_read: BufReader<ReadHalf<Stream>>,
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

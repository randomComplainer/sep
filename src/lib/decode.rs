use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct BufDecoder<Stream>
where
    Stream: AsyncRead + 'static + Unpin,
{
    inner: Stream,
    buf: BytesMut,
}

impl<Stream> BufDecoder<Stream>
where
    Stream: tokio::io::AsyncRead + Unpin,
{
    // TODO: test different buffer size
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
    fn peek_u8(&mut self) -> Option<RefU8>;
    fn peek_u16(&mut self) -> Option<RefU16>;
    fn peek_u64(&mut self) -> Option<RefU64>;
    fn peek_slice(&mut self, len: usize) -> Option<RefSlice>;
    fn peek_oct_len_slice(&mut self) -> Option<RefSlice>;
}

impl Peek for Cursor<&[u8]> {
    fn peek_u8(&mut self) -> Option<RefU8> {
        if self.remaining() < 1 {
            None
        } else {
            let result = Some(RefU8 {
                offset: self.position().try_into().unwrap(),
            });
            self.advance(1);
            result
        }
    }

    fn peek_u16(&mut self) -> Option<RefU16> {
        if self.remaining() < 2 {
            None
        } else {
            let result = Some(RefU16 {
                offset: self.position().try_into().unwrap(),
            });
            self.advance(2);
            result
        }
    }

    fn peek_u64(&mut self) -> Option<RefU64> {
        if self.remaining() < 8 {
            None
        } else {
            let result = Some(RefU64 {
                offset: self.position().try_into().unwrap(),
            });
            self.advance(8);
            result
        }
    }

    fn peek_slice(&mut self, len: usize) -> Option<RefSlice> {
        if self.remaining() < len {
            None
        } else {
            let result = Some(RefSlice {
                offset: self.position().try_into().unwrap(),
                len,
            });
            self.advance(len);
            result
        }
    }

    fn peek_oct_len_slice(&mut self) -> Option<RefSlice> {
        let len = peek!(self.peek_u8());
        self.peek_slice(len.read(self.get_ref()) as usize)
    }
}

pub struct RefU8 {
    pub offset: usize,
}
impl RefU8 {
    pub fn read(&self, bytes: &[u8]) -> u8 {
        bytes[self.offset]
    }
}

pub struct RefU16 {
    pub offset: usize,
}
impl RefU16 {
    pub fn read(&self, bytes: &[u8]) -> u16 {
        u16::from_be_bytes([bytes[self.offset], bytes[self.offset + 1]])
    }
}

pub struct RefU64 {
    pub offset: usize,
}

impl RefU64 {
    pub fn read(&self, bytes: &[u8]) -> u64 {
        u64::from_be_bytes(bytes[self.offset..self.offset + 8].try_into().unwrap())
    }
}

pub struct RefSlice {
    pub offset: usize,
    pub len: usize,
}

impl RefSlice {
    pub fn read<'bytes>(&self, bytes: &'bytes [u8]) -> &'bytes [u8] {
        &bytes[self.offset..self.offset + self.len]
    }

    pub fn read_as_str<'bytes>(&self, bytes: &'bytes [u8]) -> &'bytes str {
        std::str::from_utf8(&bytes[self.offset..self.offset + self.len]).unwrap()
    }
}

pub struct RefIpv4Addr {
    pub offset: usize,
}

impl RefIpv4Addr {
    pub fn read(&self, bytes: &[u8]) -> std::net::Ipv4Addr {
        let raw = &bytes[self.offset..self.offset + 4];
        std::net::Ipv4Addr::from(<[u8; 4]>::try_from(raw).unwrap())
    }
}

pub struct RefIpv6Addr {
    pub offset: usize,
}

impl RefIpv6Addr {
    pub fn read(&self, bytes: &[u8]) -> std::net::Ipv6Addr {
        let raw = &bytes[self.offset..self.offset + 16];
        std::net::Ipv6Addr::from(<[u8; 16]>::try_from(raw).unwrap())
    }
}

pub enum RefAddr {
    Ipv4(RefIpv4Addr),
    Ipv6(RefIpv6Addr),
    Domain(RefSlice),
}

#[derive(Debug)]
pub enum ViewAddr<'a> {
    Ipv4(std::net::Ipv4Addr),
    Ipv6(std::net::Ipv6Addr),
    Domain(&'a str),
}

impl RefAddr {
    pub fn format<'bytes>(&self, bytes: &'bytes [u8]) -> String {
        match self {
            RefAddr::Ipv4(addr) => addr.read(bytes).to_string(),
            RefAddr::Ipv6(addr) => addr.read(bytes).to_string(),
            RefAddr::Domain(addr) => String::from(addr.read_as_str(bytes)),
        }
    }

    pub fn read<'bytes>(&self, bytes: &'bytes [u8]) -> ViewAddr<'bytes> {
        match self {
            RefAddr::Ipv4(addr) => ViewAddr::Ipv4(addr.read(bytes)),
            RefAddr::Ipv6(addr) => ViewAddr::Ipv6(addr.read(bytes)),
            RefAddr::Domain(addr) => ViewAddr::Domain(addr.read_as_str(bytes)),
        }
    }
}

pub fn peek_addr(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<RefAddr>, std::io::Error> {
    let atyp = try_peek!(cursor.peek_u8()).read(cursor.get_ref());
    Ok(Some(match atyp {
        1 => {
            let addr = RefAddr::Ipv4(RefIpv4Addr {
                offset: cursor.position().try_into().unwrap(),
            });
            cursor.advance(4);
            addr
        }

        4 => {
            let addr = RefAddr::Ipv6(RefIpv6Addr {
                offset: cursor.position().try_into().unwrap(),
            });
            cursor.advance(16);
            addr
        }
        3 => RefAddr::Domain(try_peek!(cursor.peek_oct_len_slice())),
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid address type",
            )
            .into());
        }
    }))
}

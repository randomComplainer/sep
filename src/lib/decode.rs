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
    fn peek_u8(&mut self) -> Option<PeekU8>;
    fn peek_u16(&mut self) -> Option<PeekU16>;
    fn peek_u64(&mut self) -> Option<PeekU64>;
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

    fn peek_u64(&mut self) -> Option<PeekU64> {
        if self.remaining() < 8 {
            None
        } else {
            let result = Some(PeekU64 {
                offset: self.position().try_into().unwrap(),
            });
            self.advance(8);
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
impl PeekU16 {
    pub fn read(&self, bytes: &[u8]) -> u16 {
        u16::from_be_bytes([bytes[self.offset], bytes[self.offset + 1]])
    }
}

pub struct PeekU64 {
    pub offset: usize,
}

impl PeekU64 {
    pub fn read(&self, bytes: &[u8]) -> u64 {
        u64::from_be_bytes(bytes[self.offset..self.offset + 8].try_into().unwrap())
    }
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

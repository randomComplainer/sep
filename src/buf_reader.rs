use std::io::Cursor;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt as _};

pub use peek::Peeker;
pub use read::Reader;

pub struct BufReader<Inner> {
    buf: BytesMut,
    inner: Inner,
}

impl<Inner> BufReader<Inner>
where
    Inner: 'static + Unpin + AsyncRead,
{
    // TODO: test different buffer size
    const BUF_SIZE: usize = 1024 * 8;

    pub fn new(inner: Inner) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(Self::BUF_SIZE),
        }
    }

    pub async fn read_ahead(&mut self) -> Result<usize, std::io::Error> {
        self.buf.reserve(1);
        let n = self.inner.read_buf(&mut self.buf).await?;
        Ok(n)
    }

    pub fn get_buf(&self) -> &BytesMut {
        &self.buf
    }

    pub async fn read_framed<T>(&mut self, peeker: impl Peeker<T>) -> Result<T, std::io::Error> {
        loop {
            let mut cursor = Cursor::new(self.buf.as_ref());
            match peeker.peek(&mut cursor) {
                Ok(Some(reader)) => return Ok(reader.read(&mut self.buf)),
                Ok(None) => {}
                Err(e) => return Err(e),
            };

            let n = self.read_ahead().await?;

            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "unexpected end of stream",
                ));
            }
        }
    }

    pub async fn skip(&mut self, bytes_count: usize) -> Result<(), std::io::Error> {
        let mut skipped = 0;
        loop {
            let bytes_to_skip_current_iter = std::cmp::min(bytes_count - skipped, self.buf.len());
            let _ = self.buf.split_to(bytes_to_skip_current_iter);
            skipped += bytes_to_skip_current_iter;

            if skipped == bytes_count {
                return Ok(());
            }

            assert!(skipped < bytes_count);
            let n = self.read_ahead().await?;

            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "unexpected end of stream",
                ));
            }
        }
    }

    pub fn unpack(self) -> (Inner, BytesMut) {
        (self.inner, self.buf)
    }
}

pub fn unknown_enum_code(enum_name: &'static str, code: u8) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("unkown enum code: {code}, enum name: {enum_name}"),
    )
}

pub mod read {
    use bytes::BytesMut;

    pub trait Reader {
        type Value;
        fn read(&self, buf: &mut BytesMut) -> Self::Value;
    }

    impl<T, F> Reader for F
    where
        F: Fn(&mut BytesMut) -> T,
    {
        type Value = T;
        fn read(&self, buf: &mut BytesMut) -> T {
            (self)(buf)
        }
    }

    pub const fn wrap<T, TFn>(f: TFn) -> impl Reader<Value = T>
    where
        TFn: Fn(&mut BytesMut) -> T,
    {
        f
    }
}

pub mod peek {
    use std::io::Cursor;

    use bytes::Buf;

    use super::Reader;

    pub trait Peeker<T> {
        type Reader: Reader<Value = T>;

        fn peek(&self, cursor: &mut Cursor<&[u8]>) -> Result<Option<Self::Reader>, std::io::Error>;
    }

    impl<T, TReader, F> Peeker<T> for F
    where
        F: Fn(&mut Cursor<&[u8]>) -> Result<Option<TReader>, std::io::Error>,
        TReader: Reader<Value = T>,
    {
        type Reader = TReader;
        fn peek(&self, cursor: &mut Cursor<&[u8]>) -> Result<Option<Self::Reader>, std::io::Error> {
            (self)(cursor)
        }
    }

    pub const fn wrap<T, TReader>(
        f: impl Fn(&mut Cursor<&[u8]>) -> Result<Option<TReader>, std::io::Error>,
    ) -> impl Peeker<T, Reader = TReader>
    where
        TReader: Reader<Value = T>,
    {
        f
    }

    pub const fn peek_enum<T, TReader>(
        f: impl Fn(&mut Cursor<&[u8]>, u8) -> Result<Option<TReader>, std::io::Error>,
    ) -> impl Peeker<T, Reader = TReader>
    where
        TReader: Reader<Value = T>,
    {
        wrap(move |cursor| {
            if cursor.remaining() < 1 {
                return Ok(None);
            }
            let enum_code = cursor.get_u8();
            f(cursor, enum_code)
        })
    }

    #[macro_export]
    macro_rules! peek {
        ($peek_result:expr) => {
            match $peek_result {
                Ok(Some(peeked)) => peeked,
                Ok(None) => return Ok(None),
                Err(err) => return Err(err),
            }
        };
    }
}

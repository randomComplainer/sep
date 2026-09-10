use std::{
    io::Cursor,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    time::Duration,
};

use bytes::{Buf as _, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::protocol::msg::session::Buf;

pub use peek::Peeker;
pub use read::Reader;

pub trait Encode
where
    Self: Sized,
{
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>);
}

pub trait Encoder<T> {
    fn encode(&self, item: T, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>);
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

pub struct U8Reader;
impl Reader for U8Reader {
    type Value = u8;
    fn read(&self, buf: &mut BytesMut) -> u8 {
        buf.split_to(1)[0]
    }
}

pub const fn u8_peeker() -> impl Peeker<u8, Reader = U8Reader> {
    peek::wrap(|cursor| {
        Ok(if cursor.remaining() < 1 {
            None
        } else {
            cursor.advance(1);
            Some(U8Reader)
        })
    })
}

pub struct U16Reader;
impl Reader for U16Reader {
    type Value = u16;
    fn read(&self, buf: &mut BytesMut) -> u16 {
        u16::from_be_bytes(buf.split_to(2).as_ref().try_into().unwrap())
    }
}

pub const fn u16_peeker() -> impl Peeker<u16, Reader = U16Reader> {
    peek::wrap(|cursor| {
        Ok(if cursor.remaining() < 2 {
            None
        } else {
            cursor.advance(2);
            Some(U16Reader)
        })
    })
}

pub struct U32Reader;
impl Reader for U32Reader {
    type Value = u32;
    fn read(&self, buf: &mut BytesMut) -> u32 {
        u32::from_be_bytes(buf.split_to(4).as_ref().try_into().unwrap())
    }
}

pub const fn u32_peeker() -> impl Peeker<u32, Reader = U32Reader> {
    peek::wrap(|cursor| {
        Ok(if cursor.remaining() < 4 {
            None
        } else {
            cursor.advance(4);
            Some(U32Reader)
        })
    })
}

pub struct U64Reader;
impl Reader for U64Reader {
    type Value = u64;
    fn read(&self, buf: &mut BytesMut) -> u64 {
        u64::from_be_bytes(buf.split_to(8).as_ref().try_into().unwrap())
    }
}

pub const fn u64_peeker() -> impl Peeker<u64, Reader = U64Reader> {
    peek::wrap(|cursor| {
        Ok(if cursor.remaining() < 8 {
            None
        } else {
            cursor.advance(8);
            Some(U64Reader)
        })
    })
}

pub struct SliceReader {
    pub head_len: u8,
    pub body_len: u16,
}
impl Reader for SliceReader {
    type Value = BytesMut;
    fn read(&self, buf: &mut BytesMut) -> BytesMut {
        let _ = buf.split_to(self.head_len as usize);
        buf.split_to(self.body_len as usize)
    }
}

impl SliceReader {
    pub fn new(head_len: u8, body_len: u16) -> Self {
        Self { head_len, body_len }
    }
}

pub const fn slice_peeker_u8_len() -> impl Peeker<BytesMut, Reader = SliceReader> {
    peek::wrap(|cursor| {
        if cursor.remaining() < 1 {
            return Ok(None);
        }
        let len = cursor.get_u8();
        if cursor.remaining() < len as usize {
            return Ok(None);
        }
        cursor.advance(len as usize);
        Ok(Some(SliceReader::new(1, len as u16)))
    })
}

pub const fn slice_peeker_u16_len() -> impl Peeker<BytesMut, Reader = SliceReader> {
    peek::wrap(|cursor| {
        if cursor.remaining() < 2 {
            return Ok(None);
        }
        let len = cursor.get_u16();
        if cursor.remaining() < len as usize {
            return Ok(None);
        }
        cursor.advance(len as usize);
        Ok(Some(SliceReader::new(2, len as u16)))
    })
}

pub const fn slice_peeker_fixed_len(len: u16) -> impl Peeker<BytesMut> {
    peek::wrap(move |cursor| {
        if cursor.remaining() < len as usize {
            return Ok(None);
        }
        cursor.advance(len as usize);
        Ok(Some(SliceReader::new(0, len as u16)))
    })
}

pub struct StringReader(SliceReader);
impl Reader for StringReader {
    type Value = String;
    fn read(&self, buf: &mut BytesMut) -> String {
        let _ = buf.split_to(self.0.head_len as usize);
        let str_buf = buf.split_to(self.0.body_len as usize);
        // SAFTY: validation happened in peeking
        unsafe { String::from_utf8_unchecked(str_buf.to_vec()) }
    }
}

pub const fn string_peeker() -> impl Peeker<String, Reader = StringReader> {
    peek::wrap(move |cursor| {
        if cursor.remaining() < 1 {
            return Ok(None);
        }
        let len = cursor.get_u8();
        if cursor.remaining() < len as usize {
            return Ok(None);
        }

        let pos = cursor.position() as usize;
        let slice = &cursor.get_ref()[pos..pos + len as usize];
        str::from_utf8(slice).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid UTF-8 string")
        })?;

        cursor.advance(len as usize);
        Ok(Some(StringReader(SliceReader::new(1, len as u16))))
    })
}

pub struct Ipv4AddrReader;
impl Reader for Ipv4AddrReader {
    type Value = Ipv4Addr;
    fn read(&self, buf: &mut BytesMut) -> std::net::Ipv4Addr {
        let raw = &buf.split_to(4);
        std::net::Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3])
    }
}

pub const fn ipv4_peeker() -> impl Peeker<std::net::Ipv4Addr, Reader = Ipv4AddrReader> {
    peek::wrap(|cursor| {
        if cursor.remaining() < 4 {
            return Ok(None);
        } else {
            cursor.advance(4);
            return Ok(Some(Ipv4AddrReader));
        }
    })
}

pub struct Ipv6AddrReader;
impl Reader for Ipv6AddrReader {
    type Value = Ipv6Addr;
    fn read(&self, buf: &mut BytesMut) -> std::net::Ipv6Addr {
        let raw = &buf.split_to(16);
        std::net::Ipv6Addr::from_octets(raw.as_ref().try_into().unwrap())
    }
}

pub const fn ipv6_peeker() -> impl Peeker<std::net::Ipv6Addr, Reader = Ipv6AddrReader> {
    peek::wrap(|cursor| {
        if cursor.remaining() < 16 {
            return Ok(None);
        } else {
            cursor.advance(16);
            return Ok(Some(Ipv6AddrReader));
        }
    })
}

#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
#[derive(Hash)]
pub enum RequestAddr {
    Ipv4(std::net::Ipv4Addr),
    Ipv6(std::net::Ipv6Addr),
    Domain(String),
}

impl std::fmt::Debug for RequestAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ipv4(addr) => write!(f, "{}", addr),
            Self::Ipv6(addr) => write!(f, "{}", addr),
            Self::Domain(domain) => write!(f, "{}", &domain),
        }
    }
}

impl Encode for RequestAddr {
    fn encode(
        self,
        main_buf: &mut BytesMut,
        side_bufs: &mut Vec<crate::protocol::msg::session::Buf>,
    ) {
        match self {
            RequestAddr::Ipv4(addr) => {
                main_buf.put_u8(0);
                main_buf.put_slice(&addr.octets());
            }
            RequestAddr::Ipv6(addr) => {
                main_buf.put_u8(0x04);
                main_buf.put_slice(&addr.octets());
            }
            RequestAddr::Domain(domain) => {
                main_buf.put_u8(0x03);
                main_buf.put_u8(domain.as_bytes().len() as u8);
                side_bufs.push(domain.into_bytes().into());
            }
        }
    }
}

pub enum RequestAddrReader {
    IpV4(Ipv4AddrReader),
    IpV6(Ipv6AddrReader),
    Domain(StringReader),
}

impl Reader for RequestAddrReader {
    type Value = RequestAddr;
    fn read(&self, buf: &mut BytesMut) -> RequestAddr {
        buf.split_to(1)[0];
        match self {
            Self::IpV4(reader) => RequestAddr::Ipv4(reader.read(buf)),
            Self::IpV6(reader) => RequestAddr::Ipv6(reader.read(buf)),
            Self::Domain(reader) => RequestAddr::Domain(reader.read(buf)),
        }
    }
}

pub fn request_addr_peeker() -> impl Peeker<RequestAddr, Reader = RequestAddrReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            1 => RequestAddrReader::IpV4(crate::peek!(ipv4_peeker().peek(cursor))),

            4 => RequestAddrReader::IpV6(crate::peek!(ipv6_peeker().peek(cursor))),
            3 => RequestAddrReader::Domain(crate::peek!(string_peeker().peek(cursor))),
            x => {
                return Err(unknown_enum_code("reuqest addr", x));
            }
        }))
    })
}

pub enum SockerAddrReader {
    IpV4(Ipv4AddrReader, U16Reader),
    IpV6(Ipv6AddrReader, U16Reader),
}

impl Reader for SockerAddrReader {
    type Value = SocketAddr;
    fn read(&self, buf: &mut BytesMut) -> SocketAddr {
        let _ = buf.split_to(1);
        match self {
            Self::IpV4(reader, port_reader) => {
                SocketAddr::V4(SocketAddrV4::new(reader.read(buf), port_reader.read(buf)))
            }
            Self::IpV6(reader, port_reader) => SocketAddr::V6(SocketAddrV6::new(
                reader.read(buf),
                port_reader.read(buf),
                0,
                0,
            )),
        }
    }
}

pub fn socket_addr_peeker() -> impl Peeker<SocketAddr, Reader = SockerAddrReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            1 => SockerAddrReader::IpV4(
                crate::peek!(ipv4_peeker().peek(cursor)),
                crate::peek!(u16_peeker().peek(cursor)),
            ),
            4 => SockerAddrReader::IpV6(
                crate::peek!(ipv6_peeker().peek(cursor)),
                crate::peek!(u16_peeker().peek(cursor)),
            ),
            x => {
                return Err(unknown_enum_code("socket addr", x));
            }
        }))
    })
}

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
    const BUF_SIZE: usize = 1024 * 32;

    pub fn new(inner: Stream) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(Self::BUF_SIZE),
        }
    }

    pub async fn read_next<T, P: Peeker<T>>(
        &mut self,
        peeker: P,
    ) -> Result<Option<T>, std::io::Error> {
        loop {
            let mut cursor = Cursor::new(self.buf.as_ref());
            match peeker.peek(&mut cursor) {
                Ok(Some(reader)) => return Ok(Some(reader.read(&mut self.buf))),
                Ok(None) => {}
                Err(err) => return Err(err),
            };

            self.buf.reserve(Self::BUF_SIZE);
            let n = self.inner.read_buf(&mut self.buf).await?;

            if n == 0 {
                return Ok(None);
            }
        }
    }

    pub async fn read_next_with_timeout<T, P: Peeker<T>>(
        &mut self,
        peeker: P,
        time_limit: Duration,
    ) -> Result<Option<T>, std::io::Error> {
        loop {
            let mut cursor = Cursor::new(self.buf.as_ref());
            match peeker.peek(&mut cursor) {
                Ok(Some(reader)) => return Ok(Some(reader.read(&mut self.buf))),
                Ok(None) => {}
                Err(err) => return Err(err),
            };

            self.buf.reserve(1);

            let n = tokio::select! {
                n = self.inner.read_buf(&mut self.buf) => n?,
                _ = tokio::time::sleep(time_limit) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout reading stream",
                    ).into());
                }
            };

            if n == 0 {
                return Ok(None);
            }
        }
    }

    pub fn from_parts(buf: BytesMut, inner: Stream) -> Self {
        Self { buf, inner }
    }

    pub fn into_parts(self) -> (BytesMut, Stream) {
        (self.buf, self.inner)
    }
}

#[cfg(test)]
pub use tests::test_codec;

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, io::Cursor};

    use bytes::BufMut as _;

    use super::*;

    struct Aggregated {
        pub value_1: u8,
        pub value_2: u8,
    }

    impl Aggregated {
        pub const fn decoder() -> impl Reader<Value = Self> {
            read::wrap(|buf| Aggregated {
                value_1: U8Reader.read(buf),
                value_2: U8Reader.read(buf),
            })
        }

        pub fn peeker() -> impl Peeker<Self> {
            peek::wrap(|cursor| {
                crate::peek!(u8_peeker().peek(cursor));
                crate::peek!(u8_peeker().peek(cursor));

                Ok(Some(Aggregated::decoder()))
            })
        }
    }

    #[test]
    fn test_aggregate() {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u8(0x01);
        buf.put_u8(0x02);
        let resolved = Aggregated::peeker()
            .peek(&mut Cursor::new(&buf))
            .unwrap()
            .map(|decoder| decoder.read(&mut buf))
            .unwrap();

        assert_eq!(resolved.value_1, 0x01);
        assert_eq!(resolved.value_2, 0x02);
    }

    enum Enum {
        Branch1(u8),
        Branch2(Aggregated),
    }

    enum DecodeEnum {
        Branch1,
        Branch2,
    }

    impl Reader for DecodeEnum {
        type Value = Enum;
        fn read(&self, buf: &mut BytesMut) -> Enum {
            buf.split_to(1)[0];
            match self {
                DecodeEnum::Branch1 => Enum::Branch1(U8Reader.read(buf)),
                DecodeEnum::Branch2 => Enum::Branch2(Aggregated::decoder().read(buf)),
            }
        }
    }

    const fn enum_peeker() -> impl Peeker<Enum> {
        peek::peek_enum(|cursor, enum_code| {
            Ok(Some(match enum_code {
                0 => {
                    crate::peek!(u8_peeker().peek(cursor));
                    DecodeEnum::Branch1
                }
                1 => {
                    crate::peek!(Aggregated::peeker().peek(cursor));
                    DecodeEnum::Branch2
                }
                x => {
                    return Err(unknown_enum_code("test enum", x));
                }
            }))
        })
    }

    #[test]
    fn test_enum_1() {
        let mut buf = BytesMut::from([0, 8].as_ref());
        let resolved = enum_peeker()
            .peek(&mut Cursor::new(&buf))
            .unwrap()
            .map(|decoder| decoder.read(&mut buf))
            .unwrap();

        match resolved {
            Enum::Branch1(value) => assert_eq!(value, 8),
            _ => panic!("wrong branch"),
        };
    }

    #[test]
    fn test_enum_2() {
        let mut buf = BytesMut::from([1, 0x01, 0x02].as_ref());
        let resolved = enum_peeker()
            .peek(&mut Cursor::new(&buf))
            .unwrap()
            .map(|decoder| decoder.read(&mut buf))
            .unwrap();

        match resolved {
            Enum::Branch2(value) => {
                assert_eq!(value.value_1, 0x01);
                assert_eq!(value.value_2, 0x02);
            }
            _ => panic!("wrong branch"),
        };
    }

    #[test]
    fn test_enum_err() {
        let buf = BytesMut::from([2, 0x01, 0x02].as_ref());
        let resolved = enum_peeker().peek(&mut Cursor::new(&buf));

        assert!(resolved.is_err());
    }

    pub fn test_codec<T>(msg: T, peeker: impl Peeker<T>)
    where
        T: Encode + Eq + Debug + Clone,
    {
        let mut buf = BytesMut::new();
        let mut side_bufs = Vec::new();
        msg.clone().encode(&mut buf, &mut side_bufs);

        assert!(side_bufs.len() <= 1);
        if let Some(side_buf) = side_bufs.pop() {
            buf.extend_from_slice(side_buf.as_ref());
        }

        let mut cursor = Cursor::new(buf.as_ref());
        let reader = peeker.peek(&mut cursor).unwrap().unwrap();
        let read = reader.read(&mut buf);

        assert_eq!(msg, read);
        assert_eq!(0, buf.len());
    }

    #[test]
    fn reqest_addr_domain() {
        let addr = RequestAddr::Domain("www.test.com".to_string());

        test_codec(addr, request_addr_peeker());
    }
}

use std::{
    io::Cursor,
    net::{Ipv4Addr, Ipv6Addr},
};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

pub trait Peek: Sized {
    type Value;
    // fn peek(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error>;
    fn split_from(self, buf: &mut BytesMut) -> Self::Value;
}

pub trait PeekFn {
    type PeekType: Peek;
    fn peek(&self, cursor: &mut Cursor<&[u8]>) -> Result<Option<Self::PeekType>, std::io::Error>;
}

impl<T, TPeek> PeekFn for T
where
    TPeek: Peek,
    T: Fn(&mut Cursor<&[u8]>) -> Result<Option<TPeek>, std::io::Error>,
{
    type PeekType = TPeek;
    fn peek(&self, cursor: &mut Cursor<&[u8]>) -> Result<Option<Self::PeekType>, std::io::Error> {
        (self)(cursor)
    }
}

pub struct PeekU8;
impl PeekU8 {
    pub fn peek(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        Ok(if cursor.remaining() < 1 {
            None
        } else {
            cursor.advance(1);
            Some(Self)
        })
    }
}

impl Peek for PeekU8 {
    type Value = u8;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0]
    }
}

pub struct PeekU16;
impl PeekU16 {
    pub fn peek(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        Ok(if cursor.remaining() < 2 {
            None
        } else {
            cursor.advance(2);
            Some(Self)
        })
    }
}
impl Peek for PeekU16 {
    type Value = u16;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        let splited = buf.split_to(2);
        u16::from_be_bytes(splited.as_ref().try_into().unwrap())
    }
}

pub struct PeekU64;
impl PeekU64 {
    pub fn peek(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        Ok(if cursor.remaining() < 8 {
            None
        } else {
            cursor.advance(8);
            Some(Self)
        })
    }
}
impl Peek for PeekU64 {
    type Value = u64;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        u64::from_be_bytes(buf.split_to(8).as_ref().try_into().unwrap())
    }
}

pub struct PeekSlice {
    pub head_len: u8,
    pub body_len: u16,
}
impl PeekSlice {
    pub fn peek_fixed(len: usize) -> impl PeekFn {
        move |cursor: &mut Cursor<&[u8]>| -> Result<Option<Self>, std::io::Error> {
            if cursor.remaining() < len {
                return Ok(None);
            }

            Ok(Some(Self {
                head_len: 0,
                body_len: len as u16,
            }))
        }
    }

    pub fn peek_u8_len(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        if cursor.remaining() < 1 {
            return Ok(None);
        }

        let len = cursor.get_u8();
        if cursor.remaining() < len as usize {
            return Ok(None);
        }

        Ok(Some(Self {
            head_len: 1,
            body_len: len as u16,
        }))
    }

    pub fn peek_u16_len(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        if cursor.remaining() < 2 {
            return Ok(None);
        }

        let len = cursor.get_u16();
        if cursor.remaining() < len as usize {
            return Ok(None);
        }

        Ok(Some(Self {
            head_len: 2,
            body_len: len as u16,
        }))
    }
}
impl Peek for PeekSlice {
    type Value = BytesMut;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        let _ = buf.split_to(self.head_len as usize);
        buf.split_to(self.body_len as usize)
    }
}

pub struct PeekIpv4Addr;
impl PeekIpv4Addr {
    pub fn peek(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        if cursor.remaining() < 4 {
            return Ok(None);
        } else {
            return Ok(Some(Self));
        }
    }
}

impl Peek for PeekIpv4Addr {
    type Value = std::net::Ipv4Addr;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        let raw = &buf.split_to(4);
        std::net::Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3])
    }
}

pub struct PeekIpv6Addr;
impl PeekIpv6Addr {
    pub fn peek(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        if cursor.remaining() < 16 {
            return Ok(None);
        } else {
            return Ok(Some(Self));
        }
    }
}

impl Peek for PeekIpv6Addr {
    type Value = std::net::Ipv6Addr;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        let raw = &buf.split_to(16);
        std::net::Ipv6Addr::from_octets(raw.as_ref().try_into().unwrap())
    }
}

pub enum ReadRequestAddr {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
    Domain(BytesMut),
}

crate::peek_type! {
    pub enum ReadRequestAddr, PeekReadRequestAddr {
        1u8, Ipv4(PeekIpv4Addr::peek => PeekIpv4Addr),
        4u8, Ipv6(PeekIpv6Addr::peek => PeekIpv6Addr),
        3u8, Domain(PeekSlice::peek_u8_len => PeekSlice),
    }
}

impl std::fmt::Debug for ReadRequestAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ipv4(addr) => write!(f, "{}", addr),
            Self::Ipv6(addr) => write!(f, "{}", addr),
            Self::Domain(bytes) => write!(f, "{}", std::str::from_utf8(bytes.as_ref()).unwrap()),
        }
    }
}

impl ReadRequestAddr {
    pub fn read(&self) -> String {
        match self {
            Self::Ipv4(addr) => addr.to_string(),
            Self::Ipv6(addr) => addr.to_string(),
            Self::Domain(bytes) => std::str::from_utf8(bytes.as_ref()).unwrap().to_string(),
        }
    }
}

pub struct PeekSocketAddrV4(PeekIpv4Addr, PeekU16);
impl PeekSocketAddrV4 {
    pub fn peek(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        match (PeekIpv4Addr::peek(cursor)?, PeekU16::peek(cursor)?) {
            (Some(addr), Some(port)) => Ok(Some(Self(addr, port))),
            _ => Ok(None),
        }
    }
}

impl Peek for PeekSocketAddrV4 {
    type Value = std::net::SocketAddr;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
            self.0.split_from(buf),
            self.1.split_from(buf),
        ))
    }
}

pub struct PeekSocketAddrV6(PeekIpv6Addr, PeekU16);
impl PeekSocketAddrV6 {
    pub fn peek(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        match (PeekIpv6Addr::peek(cursor)?, PeekU16::peek(cursor)?) {
            (Some(addr), Some(port)) => Ok(Some(Self(addr, port))),
            _ => Ok(None),
        }
    }
}

impl Peek for PeekSocketAddrV6 {
    type Value = std::net::SocketAddr;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
            self.0.split_from(buf),
            self.1.split_from(buf),
            0,
            0,
        ))
    }
}

pub enum PeekSocketAddr {
    Ipv4(PeekSocketAddrV4),
    Ipv6(PeekSocketAddrV6),
}

impl PeekSocketAddr {
    pub fn peek(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
        if cursor.remaining() < 1 {
            return Ok(None);
        }

        let addr_type = cursor.get_u8();
        Ok(match addr_type {
            1 => PeekSocketAddrV4::peek(cursor)?.map(Self::Ipv4),
            4 => PeekSocketAddrV6::peek(cursor)?.map(Self::Ipv6),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid addr type",
                )
                .into());
            }
        })
    }
}

impl Peek for PeekSocketAddr {
    type Value = std::net::SocketAddr;

    fn split_from(self, buf: &mut BytesMut) -> Self::Value {
        let _ = buf.split_to(1);
        match self {
            Self::Ipv4(addr) => addr.split_from(buf),
            Self::Ipv6(addr) => addr.split_from(buf),
        }
    }
}

#[macro_export]
macro_rules! peek_type {
    (
        $(#[$meta:meta])*
        $vis:vis struct $struct_name:ident, $peek_type_name:ident {
        $(
            $field:ident: $field_peek_fn:path => $field_peek_type:ty,
        )*
    }) => {
        $(#[$meta])*
        $vis struct $peek_type_name {
            $(
                pub $field: $field_peek_type,
            )*
        }

        impl $peek_type_name {
            pub fn peek(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
                $(
                    let $field = match ($field_peek_fn)(cursor) {
                        Ok(Some(peeked)) => peeked,
                        Ok(None) => return Ok(None),
                        Err(err) => return Err(err),
                    };
                )*

                    Ok(Some($peek_type_name {
                        $(
                            $field,
                        )*
                    }))
            }
        }

        impl Peek for $peek_type_name {
            type Value = $struct_name;

            fn split_from(self, buf: &mut BytesMut) -> Self::Value {
                $struct_name {
                    $(
                        $field: self.$field.split_from(buf),
                    )*
                }
            }

        }
    };

    // Only support one field
    ($vis:vis struct $struct_name:ident, $peek_type_name:ident (
            $field_peek_fn:path => $field_peek_type:ty,
    )) => {
        $vis struct $peek_type_name (
            pub $field_peek_type,
        );

        impl $peek_type_name {
            pub fn peek(cursor: &mut Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
                Ok(Some($peek_type_name (
                            match ($field_peek_fn)(cursor) {
                                Ok(Some(peeked)) => peeked,
                                Ok(None) => return Ok(None),
                                Err(err) => return Err(err),
                            },
                )))
            }
        }

        impl Peek for $peek_type_name {
            type Value = $struct_name;

            fn split_from(self, buf: &mut BytesMut) -> Self::Value {
                $struct_name (
                    self.0.split_from(buf),
                )
            }
        }
    };

    (
        $(#[$meta:meta])*
        $vis:vis enum $enum_name:ident, $peek_type_name:ident {
        $(
            $branch_code:expr, $branch_name:ident(
                $(#[$branch_meta:meta])* $branch_peek_fn:path => $branch_peek_type:ty
            ),
        )*
    }) => {
        $(#[$meta])*
        $vis enum $peek_type_name {
            $(
                $branch_name($branch_peek_type),
            )*
        }

        impl $peek_type_name {
            pub fn peek(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<Self>, std::io::Error> {
                use bytes::Buf;

                if cursor.remaining() < 1 {
                    return Ok(None);
                }

                let value = match cursor.get_u8() {
                    $(
                        $branch_code => Self::$branch_name(
                            match ($branch_peek_fn)(cursor) {
                                Ok(Some(peeked)) => peeked,
                                Ok(None) => return Ok(None),
                                Err(err) => return Err(err),
                            }
                        ),
                    )*
                        _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid enum code").into()),
                };

                Ok(Some(value))
            }
        }

        impl Peek for $peek_type_name {
            type Value = $enum_name;


            fn split_from(self, buf: &mut BytesMut) -> Self::Value {
                let _ = buf.split_to(1);
                match self {
                    $(
                        $peek_type_name::$branch_name(value) => $enum_name::$branch_name(value.split_from(buf)),
                    )*
                }
            }
        }
    };
}

#[allow(dead_code)]
#[cfg(test)]
mod test {
    use super::*;

    use bytes::{BufMut, BytesMut};

    struct Aggregated {
        pub value_1: u8,
        pub value_2: u8,
    }

    peek_type! {
        struct Aggregated, PeekAggregated {
            value_1: PeekU8::peek => PeekU8,
            value_2: PeekU8::peek => PeekU8,
        }
    }

    #[test]
    fn test_aggregate() {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u8(0x01);
        buf.put_u8(0x02);

        let resolved = PeekAggregated::peek(&mut Cursor::new(&buf))
            .unwrap()
            .unwrap()
            .split_from(&mut buf);

        assert_eq!(resolved.value_1, 0x01);
        assert_eq!(resolved.value_2, 0x02);
    }

    struct Nested {
        pub value_1: u8,
        pub value_2: Aggregated,
    }

    peek_type! {
        struct Nested,PeekNested {
            value_1: PeekU8::peek => PeekU8,
            value_2: PeekAggregated::peek => PeekAggregated,
        }
    }

    #[test]
    fn test_nested() {
        let mut buf = BytesMut::with_capacity(3);
        buf.put_u8(0x01);
        buf.put_u8(0x02);
        buf.put_u8(0x03);

        let resolved = PeekNested::peek(&mut Cursor::new(&buf))
            .unwrap()
            .unwrap()
            .split_from(&mut buf);

        assert_eq!(resolved.value_1, 0x01);
        assert_eq!(resolved.value_2.value_1, 0x02);
        assert_eq!(resolved.value_2.value_2, 0x03);
    }

    struct Tuple(Aggregated);

    peek_type! {
        struct Tuple,PeekTuple(
            PeekAggregated::peek => PeekAggregated,
        )
    }

    #[test]
    fn test_tuple() {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u8(0x01);
        buf.put_u8(0x02);

        let resolved = PeekTuple::peek(&mut Cursor::new(&buf))
            .unwrap()
            .unwrap()
            .split_from(&mut buf);

        assert_eq!(resolved.0.value_1, 0x01);
        assert_eq!(resolved.0.value_2, 0x02);
    }

    enum Enum {
        Branch1(u8),
        Branch2(Aggregated),
    }

    peek_type! {
        enum Enum,PeekEnum {
            1u8, Branch1(PeekU8::peek => PeekU8),
            2u8, Branch2(PeekAggregated::peek => PeekAggregated),
        }
    }

    #[test]
    fn test_enum_1() {
        let mut buf = BytesMut::from([1, 8].as_ref());

        let resolved = PeekEnum::peek(&mut Cursor::new(&buf))
            .unwrap()
            .unwrap()
            .split_from(&mut buf);

        match resolved {
            Enum::Branch1(value) => assert_eq!(value, 8),
            _ => panic!("wrong branch"),
        };
    }

    #[test]
    fn test_enum_2() {
        let mut buf = BytesMut::from([2, 0x01, 0x02].as_ref());

        let resolved = PeekEnum::peek(&mut Cursor::new(&buf))
            .unwrap()
            .unwrap()
            .split_from(&mut buf);

        match resolved {
            Enum::Branch2(value) => {
                assert_eq!(value.value_1, 0x01);
                assert_eq!(value.value_2, 0x02);
            }
            _ => panic!("wrong branch"),
        };
    }
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
    const BUF_SIZE: usize = 1024 * 4;

    pub fn new(inner: Stream) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(Self::BUF_SIZE),
        }
    }

    pub async fn read_next<P: PeekFn>(
        &mut self,
        peek_fn: P,
    ) -> Result<Option<<P::PeekType as Peek>::Value>, std::io::Error> {
        loop {
            let mut cursor = Cursor::new(self.buf.as_ref());
            match peek_fn.peek(&mut cursor) {
                Ok(Some(peeked)) => return Ok(Some(peeked.split_from(&mut self.buf))),
                Ok(None) => {}
                Err(err) => return Err(err),
            };

            self.buf.reserve(1);
            let n = self.inner.read_buf(&mut self.buf).await?;

            if n == 0 {
                return Ok(None);
            }
        }
    }

    pub fn into_parts(self) -> (BytesMut, Stream) {
        (self.buf, self.inner)
    }
}

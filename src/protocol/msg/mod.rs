use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use bytes::{Buf as _, BytesMut};

use crate::buf_reader::{Peeker, Reader, peek, unknown_enum_code};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct Request {
    pub addr: RequestAddr,
    pub port: u16,
}

pub struct RequestReader {
    addr: RequestAddrReader,
    port: U16Reader,
}

impl Reader for RequestReader {
    type Value = Request;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        Request {
            addr: self.addr.read(buf),
            port: self.port.read(buf),
        }
    }
}

pub fn request_peeker() -> impl Peeker<Request, Reader = RequestReader> {
    peek::wrap(|cursor| {
        Ok(Some(RequestReader {
            addr: crate::peek!(request_addr_peeker().peek(cursor)),
            port: crate::peek!(u16_peeker().peek(cursor)),
        }))
    })
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct Reply {
    pub bound_addr: std::net::SocketAddr,
}

pub struct ReplyReader {
    pub bound_addr: SockerAddrReader,
}
impl Reader for ReplyReader {
    type Value = Reply;
    fn read(&self, buf: &mut BytesMut) -> Reply {
        Reply {
            bound_addr: self.bound_addr.read(buf),
        }
    }
}

pub fn reply_peeker() -> impl Peeker<Reply, Reader = ReplyReader> {
    peek::wrap(|cursor| {
        Ok(Some(ReplyReader {
            bound_addr: crate::peek!(socket_addr_peeker().peek(cursor)),
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

pub struct Ipv4AddrReader;
impl Reader for Ipv4AddrReader {
    type Value = Ipv4Addr;
    fn read(&self, buf: &mut BytesMut) -> Ipv4Addr {
        let raw = &buf.split_to(4);
        Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3])
    }
}

pub const fn ipv4_peeker() -> impl Peeker<Ipv4Addr, Reader = Ipv4AddrReader> {
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
    fn read(&self, buf: &mut BytesMut) -> Ipv6Addr {
        let raw = &buf.split_to(16);
        Ipv6Addr::from_octets(raw.as_ref().try_into().unwrap())
    }
}

pub const fn ipv6_peeker() -> impl Peeker<Ipv6Addr, Reader = Ipv6AddrReader> {
    peek::wrap(|cursor| {
        if cursor.remaining() < 16 {
            return Ok(None);
        } else {
            cursor.advance(16);
            return Ok(Some(Ipv6AddrReader));
        }
    })
}

#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum RequestAddr {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
    Domain(BytesMut),
}

impl std::fmt::Debug for RequestAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ipv4(addr) => write!(f, "{}", addr),
            Self::Ipv6(addr) => write!(f, "{}", addr),
            Self::Domain(bytes) => write!(
                f,
                "{}",
                match std::str::from_utf8(bytes.as_ref()) {
                    Ok(s) => s,
                    Err(_) => "invalid utf8",
                }
            ),
        }
    }
}

pub enum RequestAddrReader {
    IpV4(Ipv4AddrReader),
    IpV6(Ipv6AddrReader),
    Domain(SliceReader),
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
            3 => RequestAddrReader::Domain(crate::peek!(slice_peeker_u8_len().peek(cursor))),
            x => {
                return Err(unknown_enum_code("reuqest addr", x));
            }
        }))
    })
}

use std::net::SocketAddr;

use bytes::{BufMut as _, BytesMut};

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

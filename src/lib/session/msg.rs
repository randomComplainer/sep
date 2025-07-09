use bytes::BytesMut;

use crate::prelude::*;

pub struct Request {
    pub addr: decode::ReadRequestAddr,
    pub port: u16,
}

pub struct Reply {
    pub bound_addr: std::net::SocketAddr,
}

pub struct Data {
    pub seq: u16,
    pub data: BytesMut,
}

pub struct Ack {
    pub seq: u16,
}

pub struct Eof {
    pub seq: u16,
}

pub enum ClientMsg {
    Request(Request),
    Data(Data),
    Ack(Ack),
    Eof(Eof),
}

pub enum ServerMsg {
    Reply(Reply),
    Data(Data),
    Ack(Ack),
    Eof(Eof),
}

use bytes::BytesMut;
use derive_more::From;

use crate::prelude::*;

#[derive(Debug)]
pub struct Request {
    pub addr: decode::ReadRequestAddr,
    pub port: u16,
}

#[derive(Debug)]
pub struct Reply {
    pub bound_addr: std::net::SocketAddr,
}

pub struct Data {
    pub seq: u16,
    pub data: BytesMut,
}

impl std::fmt::Debug for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Data")
            .field("seq", &self.seq)
            .field("data", &self.data.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct Ack {
    pub seq: u16,
}

#[derive(Debug)]
pub struct Eof {
    pub seq: u16,
}

#[derive(Debug, From)]
pub enum ClientMsg {
    Request(#[from] Request),
    Data(#[from] Data),
    Ack(#[from] Ack),
    Eof(#[from] Eof),
}

#[derive(Debug, From)]
pub enum ServerMsg {
    Reply(#[from] Reply),
    Data(#[from] Data),
    Ack(#[from] Ack),
    Eof(#[from] Eof),
}

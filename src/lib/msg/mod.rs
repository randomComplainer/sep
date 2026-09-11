use bytes::BytesMut;

use crate::codec::*;
use crate::protocol::SessionId;

pub mod conn;
pub mod group;
pub mod protocol;
pub mod session;

pub type ClientMsg = conn::ConnMsg<protocol::ClientMsg>;
pub fn client_msg_peeker() -> impl Peeker<ClientMsg> {
    conn::conn_msg_peeker(protocol::client_msg_peeker())
}

pub type ServerMsg = conn::ConnMsg<protocol::ServerMsg>;
pub fn server_msg_peeker() -> impl Peeker<ServerMsg> {
    conn::conn_msg_peeker(protocol::server_msg_peeker())
}

pub struct SessionIdReader(U64Reader);
impl Reader for SessionIdReader {
    type Value = SessionId;
    fn read(&self, buf: &mut BytesMut) -> SessionId {
        self.0.read(buf)
    }
}

pub fn session_id_peeker() -> impl Peeker<SessionId, Reader = SessionIdReader> {
    peek::wrap(|cursor| {
        Ok(Some(SessionIdReader(crate::peek!(
            u64_peeker().peek(cursor)
        ))))
    })
}

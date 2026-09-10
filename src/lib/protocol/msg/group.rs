use bytes::BufMut as _;
use bytes::BytesMut;

use crate::decode::*;
use crate::prelude::*;
use crate::protocol::SessionId;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ClientCmd {
    KillSession(SessionId),
}

impl Encode for ClientCmd {
    fn encode(self, main_buf: &mut BytesMut, _side_bufs: &mut Vec<super::session::Buf>) {
        match self {
            ClientCmd::KillSession(session_id) => {
                main_buf.put_u8(0);
                main_buf.put_u64(session_id);
            }
        }
    }
}

pub enum ClientCmdReader {
    KillSession(super::SessionIdReader),
}

impl Reader for ClientCmdReader {
    type Value = ClientCmd;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::KillSession(session_id) => ClientCmd::KillSession(session_id.read(buf)),
        }
    }
}

pub fn client_cmd_peeker() -> impl Peeker<ClientCmd, Reader = ClientCmdReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => {
                ClientCmdReader::KillSession(crate::peek!(super::session_id_peeker().peek(cursor)))
            }
            x => {
                return Err(decode::unknown_enum_code("client command", x).into());
            }
        }))
    })
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ServerCmd {
    KillSession(SessionId),
    ConnectMore { expected: u8 },
}

impl Encode for ServerCmd {
    fn encode(self, main_buf: &mut BytesMut, _side_bufs: &mut Vec<super::session::Buf>) {
        match self {
            ServerCmd::KillSession(session_id) => {
                main_buf.put_u8(0);
                main_buf.put_u64(session_id);
            }
            ServerCmd::ConnectMore { expected } => {
                main_buf.put_u8(1);
                main_buf.put_u8(expected);
            }
        }
    }
}

pub enum ServerCmdReader {
    KillSession(super::SessionIdReader),
    ConnectMore { expected: U8Reader },
}

impl Reader for ServerCmdReader {
    type Value = ServerCmd;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::KillSession(session_id) => ServerCmd::KillSession(session_id.read(buf)),
            Self::ConnectMore { expected } => ServerCmd::ConnectMore {
                expected: expected.read(buf),
            },
        }
    }
}

pub fn server_cmd_peeker() -> impl Peeker<ServerCmd, Reader = ServerCmdReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => {
                ServerCmdReader::KillSession(crate::peek!(super::session_id_peeker().peek(cursor)))
            }
            1 => ServerCmdReader::ConnectMore {
                expected: crate::peek!(u8_peeker().peek(cursor)),
            },
            x => {
                return Err(decode::unknown_enum_code("server command", x).into());
            }
        }))
    })
}

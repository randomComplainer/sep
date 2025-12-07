use bytes::BytesMut;
use derive_more::From;

use crate::decode::*;
use crate::prelude::*;

// connection level messages
pub mod conn {
    use bytes::BytesMut;
    use derive_more::From;

    use crate::decode::*;
    use crate::prelude::*;

    #[derive(Debug, From, PartialEq, Eq)]
    pub enum ServerMsg {
        Protocol(super::ServerMsg),
        EndOfStream,
        Pong,
    }

    pub enum ServerMsgReader {
        Protocol(super::ServerMsgReader),
        EndOfStream,
        Pong,
    }

    impl Reader for ServerMsgReader {
        type Value = ServerMsg;

        fn read(&self, buf: &mut BytesMut) -> Self::Value {
            buf.split_to(1)[0];
            match self {
                Self::Protocol(protocol) => ServerMsg::Protocol(protocol.read(buf)),
                Self::EndOfStream => ServerMsg::EndOfStream,
                Self::Pong => ServerMsg::Pong,
            }
        }
    }

    pub fn server_msg_peeker() -> impl Peeker<ServerMsg, Reader = ServerMsgReader> {
        peek::peek_enum(|cursor, enum_code| {
            Ok(Some(match enum_code {
                0 => {
                    ServerMsgReader::Protocol(crate::peek!(super::server_msg_peeker().peek(cursor)))
                }
                1 => ServerMsgReader::EndOfStream,
                2 => ServerMsgReader::Pong,
                x => {
                    return Err(
                        decode::unknown_enum_code("connection level server message", x).into(),
                    );
                }
            }))
        })
    }

    #[derive(Debug, From, PartialEq, Eq)]
    pub enum ClientMsg {
        Protocol(super::ClientMsg),
        Ping,
    }

    pub enum ClientMsgReader {
        Protocol(super::ClientMsgReader),
        Ping,
    }

    impl Reader for ClientMsgReader {
        type Value = ClientMsg;

        fn read(&self, buf: &mut BytesMut) -> Self::Value {
            buf.split_to(1)[0];
            match self {
                Self::Protocol(protocol) => ClientMsg::Protocol(protocol.read(buf)),
                Self::Ping => ClientMsg::Ping,
            }
        }
    }

    pub fn client_msg_peeker() -> impl Peeker<ClientMsg, Reader = ClientMsgReader> {
        peek::peek_enum(|cursor, enum_code| {
            Ok(Some(match enum_code {
                0 => {
                    ClientMsgReader::Protocol(crate::peek!(super::client_msg_peeker().peek(cursor)))
                }
                1 => ClientMsgReader::Ping,
                x => {
                    return Err(
                        decode::unknown_enum_code("connection level client message", x).into(),
                    );
                }
            }))
        })
    }
}

#[derive(Debug, From, PartialEq, Eq)]
pub enum ServerMsg {
    SessionMsg(u16, session::msg::ServerMsg),
}

impl session::msg::ServerMsg {
    pub fn with_session_id(self, session_id: u16) -> ServerMsg {
        ServerMsg::SessionMsg(session_id, self)
    }
}

pub enum ServerMsgReader {
    SessionMsg(U16Reader, session::msg::ServerMsgReader),
}

impl Reader for ServerMsgReader {
    type Value = ServerMsg;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::SessionMsg(u16, session_msg) => {
                ServerMsg::SessionMsg(u16.read(buf), session_msg.read(buf))
            }
        }
    }
}

pub fn server_msg_peeker() -> impl Peeker<ServerMsg, Reader = ServerMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ServerMsgReader::SessionMsg(
                crate::peek!(u16_peeker().peek(cursor)),
                crate::peek!(session::msg::server_msg_peeker().peek(cursor)),
            ),
            x => {
                return Err(decode::unknown_enum_code("server message", x).into());
            }
        }))
    })
}

#[derive(Debug, From, PartialEq, Eq)]
pub enum ClientMsg {
    SessionMsg(u16, session::msg::ClientMsg),
}

impl session::msg::ClientMsg {
    pub fn with_session_id(self, session_id: u16) -> ClientMsg {
        ClientMsg::SessionMsg(session_id, self)
    }
}

pub enum ClientMsgReader {
    SessionMsg(U16Reader, session::msg::ClientMsgReader),
}

impl Reader for ClientMsgReader {
    type Value = ClientMsg;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::SessionMsg(u16, session_msg) => {
                ClientMsg::SessionMsg(u16.read(buf), session_msg.read(buf))
            }
        }
    }
}

pub fn client_msg_peeker() -> impl Peeker<ClientMsg, Reader = ClientMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ClientMsgReader::SessionMsg(
                crate::peek!(u16_peeker().peek(cursor)),
                crate::peek!(session::msg::client_msg_peeker().peek(cursor)),
            ),
            x => {
                return Err(decode::unknown_enum_code("client message", x).into());
            }
        }))
    })
}

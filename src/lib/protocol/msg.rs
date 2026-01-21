use bytes::BytesMut;
use derive_more::From;

use crate::decode::*;
use crate::prelude::*;
use crate::protocol::SessionId;

// connection level messages
pub mod conn {
    use bytes::BytesMut;
    use derive_more::From;

    use crate::decode::*;
    use crate::prelude::*;

    #[derive(Debug, From, PartialEq, Eq)]
    pub enum ConnMsg<TMessage> {
        Protocol(TMessage),
        Ping,
        EndOfStream,
    }

    pub enum ConnMsgReader<TMessage> {
        Protocol(TMessage),
        Ping,
        EndOfStream,
    }

    impl<TMessageReader> Reader for ConnMsgReader<TMessageReader>
    where
        TMessageReader: Reader,
    {
        type Value = ConnMsg<TMessageReader::Value>;

        fn read(&self, buf: &mut BytesMut) -> Self::Value {
            buf.split_to(1)[0];
            match self {
                Self::Protocol(protocol) => ConnMsg::Protocol(protocol.read(buf)),
                Self::Ping => ConnMsg::Ping,
                Self::EndOfStream => ConnMsg::EndOfStream,
            }
        }
    }

    pub fn conn_msg_peeker<TMessage, TMessageReader>(
        protocol_msg_peeker: impl Peeker<TMessage, Reader = TMessageReader>,
    ) -> impl Peeker<ConnMsg<TMessage>, Reader = ConnMsgReader<TMessageReader>>
    where
        TMessageReader: Reader<Value = TMessage>,
    {
        peek::peek_enum(move |cursor, enum_code| {
            Ok(Some(match enum_code {
                0 => ConnMsgReader::Protocol(crate::peek!(protocol_msg_peeker.peek(cursor))),
                1 => ConnMsgReader::Ping,
                2 => ConnMsgReader::EndOfStream,
                x => {
                    return Err(decode::unknown_enum_code("connection level message", x).into());
                }
            }))
        })
    }
}

pub struct SessionIdReader(U64Reader, U16Reader);
impl Reader for SessionIdReader {
    type Value = SessionId;
    fn read(&self, buf: &mut BytesMut) -> SessionId {
        SessionId::new(self.0.read(buf), self.1.read(buf))
    }
}

pub fn session_id_peeker() -> impl Peeker<SessionId, Reader = SessionIdReader> {
    peek::wrap(|cursor| {
        Ok(Some(SessionIdReader(
            crate::peek!(u64_peeker().peek(cursor)),
            crate::peek!(u16_peeker().peek(cursor)),
        )))
    })
}

#[derive(Debug, From, PartialEq, Eq)]
pub enum ServerMsg {
    SessionMsg(SessionId, session::msg::ServerMsg),
    // When message loss or other errors
    KillSession(SessionId),
}

impl session::msg::ServerMsg {
    pub fn with_session_id(self, session_id: SessionId) -> ServerMsg {
        ServerMsg::SessionMsg(session_id, self)
    }
}

pub enum ServerMsgReader {
    SessionMsg(SessionIdReader, session::msg::ServerMsgReader),
    KillSession(SessionIdReader),
}

impl Reader for ServerMsgReader {
    type Value = ServerMsg;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::SessionMsg(session_id, session_msg) => {
                ServerMsg::SessionMsg(session_id.read(buf), session_msg.read(buf))
            }
            Self::KillSession(session_id) => ServerMsg::KillSession(session_id.read(buf)),
        }
    }
}

pub fn server_msg_peeker() -> impl Peeker<ServerMsg, Reader = ServerMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ServerMsgReader::SessionMsg(
                crate::peek!(session_id_peeker().peek(cursor)),
                crate::peek!(session::msg::server_msg_peeker().peek(cursor)),
            ),
            1 => ServerMsgReader::KillSession(crate::peek!(session_id_peeker().peek(cursor))),
            x => {
                return Err(decode::unknown_enum_code("server message", x).into());
            }
        }))
    })
}

#[derive(Debug, From, PartialEq, Eq)]
pub enum ClientMsg {
    SessionMsg(SessionId, session::msg::ClientMsg),
    KillSession(SessionId),
}

impl session::msg::ClientMsg {
    pub fn with_session_id(self, session_id: SessionId) -> ClientMsg {
        ClientMsg::SessionMsg(session_id, self)
    }
}

pub enum ClientMsgReader {
    SessionMsg(SessionIdReader, session::msg::ClientMsgReader),
    KillSession(SessionIdReader),
}

impl Reader for ClientMsgReader {
    type Value = ClientMsg;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::SessionMsg(session_id, session_msg) => {
                ClientMsg::SessionMsg(session_id.read(buf), session_msg.read(buf))
            }
            Self::KillSession(session_id) => ClientMsg::KillSession(session_id.read(buf)),
        }
    }
}

pub fn client_msg_peeker() -> impl Peeker<ClientMsg, Reader = ClientMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ClientMsgReader::SessionMsg(
                crate::peek!(session_id_peeker().peek(cursor)),
                crate::peek!(session::msg::client_msg_peeker().peek(cursor)),
            ),
            1 => ClientMsgReader::KillSession(crate::peek!(session_id_peeker().peek(cursor))),
            x => {
                return Err(decode::unknown_enum_code("client message", x).into());
            }
        }))
    })
}

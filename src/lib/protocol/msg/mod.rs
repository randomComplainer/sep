use bytes::BufMut;
use bytes::BytesMut;
use derive_more::From;

use crate::decode::*;
use crate::prelude::*;
use crate::protocol::SessionId;

pub mod group;
pub mod session;

// connection level messages
pub mod conn {
    use bytes::BufMut;
    use bytes::BytesMut;
    use derive_more::From;

    use crate::decode::*;
    use crate::prelude::*;

    #[cfg_attr(test, derive(PartialEq, Eq))]
    #[derive(Debug, From)]
    pub enum ConnMsg<TMessage> {
        Protocol(#[from] TMessage),
        Ping,
        EndOfStream,
    }

    impl<TMessage> Encode for ConnMsg<TMessage>
    where
        TMessage: Encode,
    {
        fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<super::session::Buf>) {
            match self {
                ConnMsg::Protocol(msg) => {
                    main_buf.put_u8(0);
                    msg.encode(main_buf, side_bufs);
                }
                ConnMsg::Ping => main_buf.put_u8(1),
                ConnMsg::EndOfStream => main_buf.put_u8(2),
            }
        }
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

#[derive(Debug, From)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum ProtocolMsg<Session, Global>
where
    Global: Clone + PartialEq + Eq,
{
    SessionMsg(SessionId, Session),
    GlobalCmd(#[from] AtLeastOnce<Global>),
}

impl<Session, Global> Encode for ProtocolMsg<Session, Global>
where
    Session: Encode,
    Global: Encode + Clone + PartialEq + Eq,
{
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<session::Buf>) {
        match self {
            ProtocolMsg::SessionMsg(session_id, msg) => {
                main_buf.put_u8(0);
                main_buf.put_u64(session_id);
                msg.encode(main_buf, side_bufs);
            }
            ProtocolMsg::GlobalCmd(cmd) => {
                main_buf.put_u8(1);
                cmd.encode(main_buf, side_bufs);
            }
        }
    }
}

pub type ServerMsg = ProtocolMsg<session::ServerMsg, group::ServerCmd>;

impl session::ServerMsg {
    pub fn with_session_id(self, session_id: SessionId) -> ServerMsg {
        ServerMsg::SessionMsg(session_id, self)
    }
}

pub enum ServerMsgReader {
    SessionMsg(SessionIdReader, session::ServerMsgReader),
    GlobalCmd(AtLeastOnceReader<group::ServerCmd, group::ServerCmdReader>),
}

impl Reader for ServerMsgReader {
    type Value = ServerMsg;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::SessionMsg(session_id, session_msg) => {
                ServerMsg::SessionMsg(session_id.read(buf), session_msg.read(buf))
            }
            Self::GlobalCmd(global_cmd) => ServerMsg::GlobalCmd(global_cmd.read(buf)),
        }
    }
}

pub fn server_msg_peeker() -> impl Peeker<ServerMsg, Reader = ServerMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ServerMsgReader::SessionMsg(
                crate::peek!(session_id_peeker().peek(cursor)),
                crate::peek!(session::server_msg_peeker().peek(cursor)),
            ),
            1 => ServerMsgReader::GlobalCmd(crate::peek!(
                at_least_once_peeker(group::server_cmd_peeker()).peek(cursor)
            )),
            x => {
                return Err(decode::unknown_enum_code("server message", x).into());
            }
        }))
    })
}

pub type ClientMsg = ProtocolMsg<session::ClientMsg, group::ClientCmd>;

impl session::ClientMsg {
    pub fn with_session_id(self, session_id: SessionId) -> ClientMsg {
        ClientMsg::SessionMsg(session_id, self)
    }
}

pub enum ClientMsgReader {
    SessionMsg(SessionIdReader, session::ClientMsgReader),
    GlobalCmd(AtLeastOnceReader<group::ClientCmd, group::ClientCmdReader>),
}

impl Reader for ClientMsgReader {
    type Value = ClientMsg;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::SessionMsg(session_id, session_msg) => {
                ClientMsg::SessionMsg(session_id.read(buf), session_msg.read(buf))
            }
            Self::GlobalCmd(global_cmd) => ClientMsg::GlobalCmd(global_cmd.read(buf)),
        }
    }
}

pub fn client_msg_peeker() -> impl Peeker<ClientMsg, Reader = ClientMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ClientMsgReader::SessionMsg(
                crate::peek!(session_id_peeker().peek(cursor)),
                crate::peek!(session::client_msg_peeker().peek(cursor)),
            ),
            1 => ClientMsgReader::GlobalCmd(crate::peek!(
                at_least_once_peeker(group::client_cmd_peeker()).peek(cursor)
            )),
            x => {
                return Err(decode::unknown_enum_code("client message", x).into());
            }
        }))
    })
}

#[derive(Debug, From, PartialEq, Eq, Clone)]
pub enum AtLeastOnce<T>
where
    T: Clone + PartialEq + Eq,
{
    Ack(u32),
    Msg(u32, T),
}

impl<T> Encode for AtLeastOnce<T>
where
    T: Encode + Clone + PartialEq + Eq,
{
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<session::Buf>) {
        match self {
            AtLeastOnce::Ack(seq) => {
                main_buf.put_u8(0);
                main_buf.put_u32(seq)
            }
            AtLeastOnce::Msg(seq, msg) => {
                main_buf.put_u8(1);
                main_buf.put_u32(seq);
                msg.encode(main_buf, side_bufs);
            }
        }
    }
}

pub enum AtLeastOnceReader<T, TReader>
where
    T: Clone + PartialEq + Eq,
    TReader: Reader<Value = T>,
{
    Ack(U32Reader),
    Msg(U32Reader, TReader),
}

impl<T, TReader> Reader for AtLeastOnceReader<T, TReader>
where
    T: Clone + PartialEq + Eq,
    TReader: Reader<Value = T>,
{
    type Value = AtLeastOnce<T>;

    fn read(&self, buf: &mut BytesMut) -> Self::Value {
        buf.split_to(1)[0];
        match self {
            Self::Ack(ack) => AtLeastOnce::Ack(ack.read(buf)),
            Self::Msg(msg, msg_reader) => AtLeastOnce::Msg(msg.read(buf), msg_reader.read(buf)),
        }
    }
}

pub fn at_least_once_peeker<T, TReader>(
    inner_peeker: impl Peeker<T, Reader = TReader>,
) -> impl Peeker<AtLeastOnce<T>, Reader = AtLeastOnceReader<T, TReader>>
where
    T: Clone + PartialEq + Eq,
    TReader: Reader<Value = T>,
{
    peek::peek_enum(move |cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => AtLeastOnceReader::Ack(crate::peek!(u32_peeker().peek(cursor))),
            1 => AtLeastOnceReader::Msg(
                crate::peek!(u32_peeker().peek(cursor)),
                crate::peek!(inner_peeker.peek(cursor)),
            ),
            x => {
                return Err(decode::unknown_enum_code("at least one message", x).into());
            }
        }))
    })
}

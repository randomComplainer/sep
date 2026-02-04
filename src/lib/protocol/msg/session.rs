use bytes::BytesMut;
use derive_more::From;

use crate::decode::*;
use crate::prelude::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Request {
    pub addr: decode::ReadRequestAddr,
    pub port: u16,
}

pub struct RequestReader {
    pub addr: ReadRequestAddrReader,
    pub port: U16Reader,
}

impl Reader for RequestReader {
    type Value = Request;
    fn read(&self, buf: &mut BytesMut) -> Request {
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

#[derive(Debug, PartialEq, Eq)]
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

#[derive(PartialEq, Eq)]
pub struct Data {
    pub seq: u16,
    pub data: BytesMut,
}

pub struct DataReader {
    pub seq: U16Reader,
    pub data: SliceReader,
}

impl Reader for DataReader {
    type Value = Data;
    fn read(&self, buf: &mut BytesMut) -> Data {
        Data {
            seq: self.seq.read(buf),
            data: self.data.read(buf),
        }
    }
}

pub fn data_peeker() -> impl Peeker<Data, Reader = DataReader> {
    peek::wrap(|cursor| {
        Ok(Some(DataReader {
            seq: crate::peek!(u16_peeker().peek(cursor)),
            data: crate::peek!(slice_peeker_u16_len().peek(cursor)),
        }))
    })
}

impl std::fmt::Debug for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Data")
            .field("seq", &self.seq)
            .field("data", &self.data.len())
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Ack {
    // pub seq: u16,
    pub bytes: u32,
}

pub struct AckReader {
    pub bytes: U32Reader,
}

impl Reader for AckReader {
    type Value = Ack;
    fn read(&self, buf: &mut BytesMut) -> Ack {
        Ack {
            bytes: self.bytes.read(buf),
        }
    }
}

pub fn ack_peeker() -> impl Peeker<Ack, Reader = AckReader> {
    peek::wrap(|cursor| {
        Ok(Some(AckReader {
            bytes: crate::peek!(u32_peeker().peek(cursor)),
        }))
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct Eof {
    pub seq: u16,
}

pub struct EofReader {
    pub seq: U16Reader,
}

impl Reader for EofReader {
    type Value = Eof;
    fn read(&self, buf: &mut BytesMut) -> Eof {
        Eof {
            seq: self.seq.read(buf),
        }
    }
}

pub fn eof_peeker() -> impl Peeker<Eof, Reader = EofReader> {
    peek::wrap(|cursor| {
        Ok(Some(EofReader {
            seq: crate::peek!(u16_peeker().peek(cursor)),
        }))
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct EofAck;

pub struct EOFAckReader;

impl Reader for EOFAckReader {
    type Value = EofAck;
    fn read(&self, _buf: &mut BytesMut) -> EofAck {
        EofAck
    }
}

pub fn eof_ack_peeker() -> impl Peeker<EofAck, Reader = EOFAckReader> {
    peek::wrap(|_cursor| Ok(Some(EOFAckReader)))
}

#[derive(Debug, PartialEq, Eq)]
pub struct IoError;
pub struct IoErrorReader;

impl Reader for IoErrorReader {
    type Value = IoError;
    fn read(&self, _buf: &mut BytesMut) -> IoError {
        IoError
    }
}

pub fn error_peeker() -> impl Peeker<IoError, Reader = IoErrorReader> {
    peek::wrap(|_cursor| Ok(Some(IoErrorReader)))
}

#[derive(Debug, From, PartialEq, Eq)]
pub enum ClientMsg {
    Request(#[from] Request),
    Data(#[from] Data),
    Ack(#[from] Ack),
    Eof(#[from] Eof),
    EofAck(#[from] EofAck),
}

pub enum ClientMsgReader {
    Request(RequestReader),
    Data(DataReader),
    Ack(AckReader),
    Eof(EofReader),
    EofAck(EOFAckReader),
}

impl Reader for ClientMsgReader {
    type Value = ClientMsg;
    fn read(&self, buf: &mut BytesMut) -> ClientMsg {
        buf.split_to(1)[0];
        match self {
            Self::Request(reader) => ClientMsg::Request(reader.read(buf)),
            Self::Data(reader) => ClientMsg::Data(reader.read(buf)),
            Self::Ack(reader) => ClientMsg::Ack(reader.read(buf)),
            Self::Eof(reader) => ClientMsg::Eof(reader.read(buf)),
            Self::EofAck(reader) => ClientMsg::EofAck(reader.read(buf)),
        }
    }
}

pub fn client_msg_peeker() -> impl Peeker<ClientMsg, Reader = ClientMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ClientMsgReader::Request(crate::peek!(request_peeker().peek(cursor))),
            1 => ClientMsgReader::Data(crate::peek!(data_peeker().peek(cursor))),
            2 => ClientMsgReader::Ack(crate::peek!(ack_peeker().peek(cursor))),
            3 => ClientMsgReader::Eof(crate::peek!(eof_peeker().peek(cursor))),
            4 => ClientMsgReader::EofAck(crate::peek!(eof_ack_peeker().peek(cursor))),
            x => {
                return Err(decode::unknown_enum_code("client session message", x).into());
            }
        }))
    })
}

#[derive(Debug, Eq, PartialEq)]
pub enum ConnectionError {
    General,
    NetworkUnreachable,
    HostUnreachable,
    ConnectionRefused,
    TtlExpired,
}

impl From<std::io::Error> for ConnectionError {
    fn from(err: std::io::Error) -> Self {
        use ConnectionError::*;
        use std::io::ErrorKind;

        match err.kind() {
            ErrorKind::NetworkUnreachable => NetworkUnreachable,
            ErrorKind::HostUnreachable => HostUnreachable,
            ErrorKind::ConnectionRefused => ConnectionRefused,
            _ => General,
        }
    }
}

pub enum ConnectionErrorReader {
    General,
    NetworkUnreachable,
    HostUnreachable,
    ConnectionRefused,
    TtlExpired,
}

impl Reader for ConnectionErrorReader {
    type Value = ConnectionError;
    fn read(&self, buf: &mut BytesMut) -> ConnectionError {
        buf.split_to(1)[0];
        match self {
            Self::General => ConnectionError::General,
            Self::NetworkUnreachable => ConnectionError::NetworkUnreachable,
            Self::HostUnreachable => ConnectionError::HostUnreachable,
            Self::ConnectionRefused => ConnectionError::ConnectionRefused,
            Self::TtlExpired => ConnectionError::TtlExpired,
        }
    }
}

pub fn connection_error_peeker() -> impl Peeker<ConnectionError, Reader = ConnectionErrorReader> {
    peek::peek_enum(|_cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ConnectionErrorReader::General,
            1 => ConnectionErrorReader::NetworkUnreachable,
            2 => ConnectionErrorReader::HostUnreachable,
            3 => ConnectionErrorReader::ConnectionRefused,
            4 => ConnectionErrorReader::TtlExpired,
            x => {
                return Err(decode::unknown_enum_code("connection error", x).into());
            }
        }))
    })
}

#[derive(Debug, From, Eq, PartialEq)]
pub enum ServerMsg {
    Reply(#[from] Reply),
    ReplyError(#[from] ConnectionError),
    Data(#[from] Data),
    Ack(#[from] Ack),
    Eof(#[from] Eof),
    EofAck(#[from] EofAck),
}

pub enum ServerMsgReader {
    Reply(ReplyReader),
    ReplyError(ConnectionErrorReader),
    Data(DataReader),
    Ack(AckReader),
    Eof(EofReader),
    EofAck(EOFAckReader),
}

impl Reader for ServerMsgReader {
    type Value = ServerMsg;
    fn read(&self, buf: &mut BytesMut) -> ServerMsg {
        buf.split_to(1)[0];
        match self {
            Self::Reply(reader) => ServerMsg::Reply(reader.read(buf)),
            Self::ReplyError(reader) => ServerMsg::ReplyError(reader.read(buf)),
            Self::Data(reader) => ServerMsg::Data(reader.read(buf)),
            Self::Ack(reader) => ServerMsg::Ack(reader.read(buf)),
            Self::Eof(reader) => ServerMsg::Eof(reader.read(buf)),
            Self::EofAck(reader) => ServerMsg::EofAck(reader.read(buf)),
        }
    }
}

pub fn server_msg_peeker() -> impl Peeker<ServerMsg, Reader = ServerMsgReader> {
    peek::peek_enum(|cursor, enum_code| {
        Ok(Some(match enum_code {
            0 => ServerMsgReader::Reply(crate::peek!(reply_peeker().peek(cursor))),
            1 => ServerMsgReader::ReplyError(crate::peek!(connection_error_peeker().peek(cursor))),
            2 => ServerMsgReader::Data(crate::peek!(data_peeker().peek(cursor))),
            3 => ServerMsgReader::Ack(crate::peek!(ack_peeker().peek(cursor))),
            4 => ServerMsgReader::Eof(crate::peek!(eof_peeker().peek(cursor))),
            5 => ServerMsgReader::EofAck(crate::peek!(eof_ack_peeker().peek(cursor))),
            x => {
                return Err(decode::unknown_enum_code("server session message", x).into());
            }
        }))
    })
}

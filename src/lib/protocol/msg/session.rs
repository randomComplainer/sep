use bytes::BufMut;
use bytes::BytesMut;
use derive_more::From;

use crate::buffer_pool::Recycle;
use crate::decode::*;
use crate::prelude::*;

#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
#[derive(Debug)]
pub struct Request {
    pub addr: decode::RequestAddr,
    pub port: u16,
}

impl Encode for Request {
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>) {
        main_buf.put_u16(self.port);
        self.addr.encode(main_buf, side_bufs);
    }
}

pub struct RequestReader {
    pub addr: RequestAddrReader,
    pub port: U16Reader,
}

impl Reader for RequestReader {
    type Value = Request;
    fn read(&self, buf: &mut BytesMut) -> Request {
        Request {
            port: self.port.read(buf),
            addr: self.addr.read(buf),
        }
    }
}

pub fn request_peeker() -> impl Peeker<Request, Reader = RequestReader> {
    peek::wrap(|cursor| {
        Ok(Some(RequestReader {
            port: crate::peek!(u16_peeker().peek(cursor)),
            addr: crate::peek!(request_addr_peeker().peek(cursor)),
        }))
    })
}

#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
#[derive(Debug)]
pub struct Reply {
    pub bound_addr: std::net::SocketAddr,
}

impl Encode for Reply {
    fn encode(self, main_buf: &mut BytesMut, _side_bufs: &mut Vec<Buf>) {
        match self.bound_addr {
            std::net::SocketAddr::V4(addr) => {
                main_buf.put_u8(0x01);
                main_buf.put_u32(addr.ip().to_bits());
                main_buf.put_u16(addr.port());
            }
            std::net::SocketAddr::V6(addr) => {
                main_buf.put_u8(0x04);
                main_buf.put_slice(&addr.ip().octets());
                main_buf.put_u16(addr.port());
            }
        };
    }
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

#[derive(From)]
#[cfg_attr(test, derive(Clone))]
pub enum Buf {
    Bytes(#[from] BytesMut),
    Vec(#[from] Vec<u8>),
    Recyle(#[from] Recycle<BytesMut>),
}

impl std::fmt::Debug for Buf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bytes(buf) => f.debug_tuple("Bytes").field(&buf.len()).finish(),
            Self::Vec(vec) => f.debug_tuple("Vec").field(&vec.len()).finish(),
            Self::Recyle(recyle) => f
                .debug_tuple("Recyle")
                .field(&recyle.ref_inner().len())
                .finish(),
        }
    }
}

#[cfg(test)]
impl PartialEq for Buf {
    fn eq(&self, other: &Self) -> bool {
        let l = self.as_ref();
        let r = other.as_ref();
        l.as_ref().eq(r.as_ref())
    }
}

#[cfg(test)]
impl Eq for Buf {}

impl AsRef<[u8]> for Buf {
    fn as_ref(&self) -> &[u8] {
        match self {
            Buf::Bytes(inner) => inner.as_ref(),
            Buf::Vec(inner) => inner.as_slice(),
            Buf::Recyle(recyle) => recyle.as_ref(),
        }
    }
}

impl AsMut<[u8]> for Buf {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Buf::Bytes(inner) => inner.as_mut(),
            Buf::Vec(inner) => inner.as_mut_slice(),
            Buf::Recyle(recyle) => recyle.as_mut(),
        }
    }
}

#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
pub struct Data {
    pub seq: u16,
    pub data: Buf,
}

impl Encode for Data {
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>) {
        main_buf.put_u16(self.seq);
        main_buf.put_u16(self.data.as_ref().len().try_into().unwrap());
        side_bufs.push(self.data);
    }
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
            data: Buf::Bytes(self.data.read(buf)),
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
            .field("data", &self.data.as_ref().len())
            .finish()
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
pub struct Ack {
    pub bytes: u32,
}

impl Encode for Ack {
    fn encode(self, main_buf: &mut BytesMut, _side_bufs: &mut Vec<Buf>) {
        main_buf.put_u32(self.bytes);
    }
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

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
pub struct Eof {
    pub seq: u16,
}

impl Encode for Eof {
    fn encode(self, main_buf: &mut BytesMut, _side_bufs: &mut Vec<Buf>) {
        main_buf.put_u16(self.seq);
    }
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

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
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

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
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

#[derive(Debug, From)]
#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
pub enum ClientMsg {
    Request(#[from] Request),
    Data(#[from] Data),
    Ack(#[from] Ack),
    Eof(#[from] Eof),
    EofAck(#[from] EofAck),
}

impl Encode for ClientMsg {
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>) {
        match self {
            ClientMsg::Request(request) => {
                main_buf.put_u8(0);
                request.encode(main_buf, side_bufs);
            }
            ClientMsg::Data(data) => {
                main_buf.put_u8(1);
                data.encode(main_buf, side_bufs);
            }
            ClientMsg::Ack(ack) => {
                main_buf.put_u8(2);
                ack.encode(main_buf, side_bufs);
            }
            ClientMsg::Eof(eof) => {
                main_buf.put_u8(3);
                eof.encode(main_buf, side_bufs);
            }
            ClientMsg::EofAck(_eof_ack) => {
                main_buf.put_u8(4);
            }
        }
    }
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

#[derive(Debug, From)]
#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
pub enum ConnectionError {
    General,
    NetworkUnreachable,
    HostUnreachable,
    ConnectionRefused,
    TtlExpired,
}

impl Encode for ConnectionError {
    fn encode(self, main_buf: &mut BytesMut, _side_bufs: &mut Vec<Buf>) {
        match self {
            ConnectionError::General => {
                main_buf.put_u8(0);
            }
            ConnectionError::NetworkUnreachable => {
                main_buf.put_u8(1);
            }
            ConnectionError::HostUnreachable => {
                main_buf.put_u8(2);
            }
            ConnectionError::ConnectionRefused => {
                main_buf.put_u8(3);
            }
            ConnectionError::TtlExpired => {
                main_buf.put_u8(4);
            }
        }
    }
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

#[derive(Debug, From)]
#[cfg_attr(test, derive(PartialEq, Eq, Clone))]
pub enum ServerMsg {
    Reply(#[from] Reply),
    ReplyError(#[from] ConnectionError),
    Data(#[from] Data),
    Ack(#[from] Ack),
    Eof(#[from] Eof),
    EofAck(#[from] EofAck),
}

impl Encode for ServerMsg {
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>) {
        match self {
            ServerMsg::Reply(reply) => {
                main_buf.put_u8(0);
                reply.encode(main_buf, side_bufs);
            }
            ServerMsg::ReplyError(connection_error) => {
                main_buf.put_u8(1);
                connection_error.encode(main_buf, side_bufs);
            }
            ServerMsg::Data(data) => {
                main_buf.put_u8(2);
                data.encode(main_buf, side_bufs);
            }
            ServerMsg::Ack(ack) => {
                main_buf.put_u8(3);
                ack.encode(main_buf, side_bufs);
            }
            ServerMsg::Eof(eof) => {
                main_buf.put_u8(4);
                eof.encode(main_buf, side_bufs);
            }
            ServerMsg::EofAck(_eof_ack) => {
                main_buf.put_u8(5);
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, str::FromStr};

    use super::*;

    #[test]
    fn request_msg() {
        let req = Request {
            addr: RequestAddr::Domain("www.test.com".to_string()),
            port: 2008,
        };

        decode::test_codec(req, request_peeker());
    }

    #[test]
    fn client_msg_request() {
        let req: ClientMsg = Request {
            addr: RequestAddr::Domain("www.test.com".to_string()),
            port: 2008,
        }
        .into();

        decode::test_codec(req, client_msg_peeker());
    }

    #[test]
    fn reply_msg() {
        let rep = Reply {
            bound_addr: std::net::SocketAddr::new(
                std::net::IpAddr::V4(Ipv4Addr::from_str("192.168.0.1").unwrap()),
                6090,
            ),
        };

        decode::test_codec(rep, reply_peeker());
    }

    #[test]
    fn data_msg() {
        let data = Data {
            seq: 1345,
            data: vec![0u8, 4u8, 3u8, 2u8].into(),
        };

        decode::test_codec(data, data_peeker());
    }

    #[test]
    fn client_msg_data() {
        let msg: ClientMsg = Data {
            seq: 1345,
            data: vec![0u8, 4u8, 3u8, 2u8].into(),
        }
        .into();

        decode::test_codec(msg, client_msg_peeker());
    }

    #[test]
    fn client_msg_eof() {
        let msg: ClientMsg = Eof {
            seq: 1543
        }.into();

        decode::test_codec(msg, client_msg_peeker());
    }
}

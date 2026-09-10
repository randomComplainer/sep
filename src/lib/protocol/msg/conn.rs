use bytes::BufMut;
use bytes::BytesMut;
use derive_more::From;

use crate::codec::*;

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
                return Err(unknown_enum_code("connection level message", x).into());
            }
        }))
    })
}


use thiserror::Error;

use crate::{
    codec::{MsgReader, MsgWriter},
    msg,
    prelude::*,
};
use protocol::*;

pub mod implementation;

#[derive(Error, Debug)]
pub enum InitError<Stream> {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("greeting invalid, {0}")]
    InvalidGreeting(&'static str, Stream),
}

pub trait Init {
    type Stream;
    // type GreetedRead;
    // type GreetedWrite;

    fn recv_greeting(
        self,
        server_timestamp: u64,
    ) -> impl Future<
        Output = Result<
            (
                Box<ClientId>,
                ConnId,
                impl MsgReader<msg::ClientMsg>,
                impl MsgWriter,
            ),
            InitError<Self::Stream>,
        >,
    > + Send;
}

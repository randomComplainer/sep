use std::future::Future;

use crate::{
    codec::{MsgReader, MsgWriter},
    protocol::*,
};

pub mod implementation;

pub trait Init {
    fn send_greeting(
        self,
        conn_id: ConnId,
        timestamp: u64,
    ) -> impl Future<
        Output = Result<
            (
                impl MsgReader<msg::conn::ConnMsg<msg::ServerMsg>>,
                impl MsgWriter,
            ),
            std::io::Error,
        >,
    > + Send;
}

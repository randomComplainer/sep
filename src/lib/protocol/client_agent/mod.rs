use std::future::Future;

use crate::protocol::*;

pub mod implementation;

pub trait Init {
    fn send_greeting(
        self,
        conn_id: ConnId,
        timestamp: u64,
    ) -> impl Future<
        Output = Result<
            (
                impl MessageReader<Message = protocol::msg::conn::ConnMsg<msg::ServerMsg>>,
                impl MessageWriter<Message = protocol::msg::conn::ConnMsg<msg::ClientMsg>>,
            ),
            std::io::Error,
        >,
    > + Send;
}

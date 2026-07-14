use crate::prelude::*;

pub trait ServerConnector
where
    Self: Clone + Sync + Send + Unpin + 'static,
{
    type GreetedWrite: protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>;
    type GreetedRead: protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>;

    type Fut: std::future::Future<
            Output = Result<(Self::GreetedRead, Self::GreetedWrite), std::io::Error>,
        > + Send
        + Sync
        + Unpin
        + 'static;

    fn connect(&self, conn_id: u64) -> Self::Fut;
}

impl<TFn, TGreetedRead, TGreetedWrite, TFuture> ServerConnector for TFn
where
    TFn: (Fn(u64) -> TFuture) + Clone + Sync + Send + Unpin + 'static,
    TGreetedRead:
        protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>,
    TGreetedWrite:
        protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>,
    TFuture: std::future::Future<Output = Result<(TGreetedRead, TGreetedWrite), std::io::Error>>
        + Send
        + Sync
        + Unpin
        + 'static,
{
    type GreetedWrite = TGreetedWrite;
    type GreetedRead = TGreetedRead;
    type Fut = TFuture;
    fn connect(&self, conn_id: u64) -> Self::Fut {
        self(conn_id)
    }
}

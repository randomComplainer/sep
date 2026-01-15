use crate::prelude::*;

pub trait ServerConnector
where
    Self: Clone + Sync + Send + Unpin + 'static,
{
    type GreetedWrite: protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>;
    type GreetedRead: protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>;

    type Fut: std::future::Future<
            Output = Result<
                (protocol::ConnId, Self::GreetedRead, Self::GreetedWrite),
                std::io::Error,
            >,
        > + Send
        + Unpin
        + 'static;

    fn connect(&self) -> Self::Fut;
}

impl<TFn, TGreetedRead, TGreetedWrite, TFuture> ServerConnector for TFn
where
    TFn: (Fn() -> TFuture) + Clone + Sync + Send + Unpin + 'static,
    TGreetedRead:
        protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>,
    TGreetedWrite:
        protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>,
    TFuture: std::future::Future<
            Output = Result<(protocol::ConnId, TGreetedRead, TGreetedWrite), std::io::Error>,
        > + Send
        + Unpin
        + 'static,
{
    type GreetedWrite = TGreetedWrite;
    type GreetedRead = TGreetedRead;
    type Fut = TFuture;
    fn connect(&self) -> Self::Fut {
        self()
    }
}

use crate::{
    codec::{MsgReader, MsgWriter},
    protocol::msg,
};

pub trait ServerConnector
where
    Self: Clone + Sync + Send + Unpin + 'static,
{
    type Reader: MsgReader<msg::conn::ConnMsg<msg::ServerMsg>>;
    type Writer: MsgWriter;

    type Fut: std::future::Future<Output = Result<(Self::Reader, Self::Writer), std::io::Error>>
        + Send
        + Sync
        + Unpin
        + 'static;

    fn connect(&self, conn_id: u64) -> Self::Fut;
}

impl<TFn, TGreetedRead, TGreetedWrite, TFuture> ServerConnector for TFn
where
    TFn: (Fn(u64) -> TFuture) + Clone + Sync + Send + Unpin + 'static,
    TGreetedRead: MsgReader<msg::conn::ConnMsg<msg::ServerMsg>>,
    TGreetedWrite: MsgWriter,
    TFuture: std::future::Future<Output = Result<(TGreetedRead, TGreetedWrite), std::io::Error>>
        + Send
        + Sync
        + Unpin
        + 'static,
{
    type Reader = TGreetedRead;
    type Writer = TGreetedWrite;
    type Fut = TFuture;

    fn connect(&self, conn_id: u64) -> Self::Fut {
        self(conn_id)
    }
}

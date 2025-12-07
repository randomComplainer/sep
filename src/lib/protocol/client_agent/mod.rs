use std::future::Future;

use crate::protocol::*;

pub mod implementation;

pub trait Init {
    fn send_greeting(
        self,
        timestamp: u64,
    ) -> impl Future<
        Output = Result<(Box<str>, impl GreetedRead, impl GreetedWrite), std::io::Error>,
    > + Send;
}

pub trait GreetedRead
where
    Self: Unpin + Send + 'static,
{
    fn recv_msg(
        &mut self,
    ) -> impl Future<Output = Result<Option<msg::conn::ServerMsg>, decode::DecodeError>> + Send;
}

pub trait GreetedWrite
where
    Self: Unpin + Send + 'static,
{
    fn send_msg(
        &mut self,
        msg: protocol::msg::conn::ClientMsg,
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

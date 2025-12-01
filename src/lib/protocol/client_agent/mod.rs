use std::future::Future;

use crate::protocol::*;

pub mod implementation;

pub trait Init {
    fn send_greeting(
        self,
        timestamp: u64,
    ) -> impl Future<Output = Result<(impl GreetedRead, impl GreetedWrite), std::io::Error>> + Send;
}

pub trait GreetedRead
where
    Self: Unpin + Send + 'static,
{
    fn recv_msg(
        &mut self,
    ) -> impl Future<Output = Result<Option<msg::ServerMsg>, decode::DecodeError>> + Send;
}

pub trait GreetedWrite
where
    Self: Unpin + Send + 'static,
{
    fn send_msg(
        &mut self,
        msg: protocol::msg::ClientMsg,
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite};

use super::*;

pub mod stream;

pub trait Init
where
    Self: Send + Unpin + 'static,
{
    fn receive_greeting_message(
        self,
    ) -> impl Future<Output = Result<(msg::ClientGreeting, impl Greeted), Socks5Error>> + Send;
}

pub trait Greeted
where
    Self: Send + Unpin + 'static,
{
    fn send_method_selection_message(
        self,
        method: u8,
    ) -> impl Future<Output = Result<(msg::ClientRequest, impl Requested), Socks5Error>> + Send;
}

pub trait Requested
where
    Self: Send + Unpin + 'static,
{
    fn reply(
        self,
        bound_addr: &SocketAddr,
    ) -> impl Future<
        Output = Result<
            (
                BufDecoder<impl AsyncRead + Unpin + Send + 'static>,
                impl AsyncWrite + Unpin + Send + 'static,
            ),
            std::io::Error,
        >,
    > + Send;

    fn reply_error(self, err_code: u8) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

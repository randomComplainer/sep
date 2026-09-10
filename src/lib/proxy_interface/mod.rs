use std::net::{IpAddr, SocketAddr};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf},
    net::TcpStream,
};

use crate::codec::{BufDecoder, RequestAddr};

pub mod socks5;

pub enum Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send + Sync,
{
    Socks5(socks5::Init<Stream>),
}

impl<Stream> Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send + Sync,
{
    pub async fn receive_request(
        self,
    ) -> Result<(RequestAddr, u16, Requested<Stream>), std::io::Error> {
        Ok(match self {
            Init::Socks5(init) => {
                let (req_addr, port, requested) = init.receive_request().await?;
                (req_addr, port, Requested::Socks5(requested))
            }
        })
    }
}

pub enum Requested<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send + Sync,
{
    Socks5(socks5::Requested<Stream>),
}

impl<Stream> Requested<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send + Sync,
{
    pub async fn reply(
        self,
        bound_addr: SocketAddr,
    ) -> Result<(BufDecoder<ReadHalf<Stream>>, WriteHalf<Stream>), std::io::Error> {
        Ok(match self {
            Requested::Socks5(requested) => requested.reply(bound_addr).await?,
        })
    }

    pub async fn reply_error(self, err_code: u8) -> Result<(), std::io::Error> {
        Ok(match self {
            Requested::Socks5(requested) => requested.reply_error(err_code).await?,
        })
    }
}

pub struct Listener {
    inner: tokio::net::TcpListener,
}

impl Listener {
    pub async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let socket = match addr.ip() {
            IpAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            IpAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        socket.set_nodelay(true)?;
        socket.set_reuseaddr(true)?;
        socket.bind(addr.clone())?;
        let inner = socket.listen(addr.port().into())?;
        Ok(Self { inner })
    }

    pub async fn accept(&self) -> std::io::Result<(Init<TcpStream>, SocketAddr)> {
        loop {
            let (stream, remote_addr) = self.inner.accept().await?;
            let mut buf = [0u8];
            stream.peek(buf.as_mut_slice()).await?;

            return Ok((
                match buf[0] {
                    5 => Init::Socks5(socks5::Init::new(stream)),
                    b'C' => {
                        tracing::warn!("http protocol is not supported yet");
                        continue;
                    }
                    x => {
                        tracing::warn!(x, "unexpected byte");
                        continue;
                    }
                },
                remote_addr,
            ));
        }
    }
}

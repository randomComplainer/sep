use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

use super::*;

pub struct Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    stream_read: BufDecoder<ReadHalf<Stream>>,
    stream_write: WriteHalf<Stream>,
}

impl<Stream> super::Init for Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    #[allow(refining_impl_trait)]
    async fn receive_greeting_message(
        mut self,
    ) -> Result<(msg::ClientGreeting, Greeted<Stream>), Socks5Error> {
        let greeting_msg = self
            .stream_read
            .read_next(msg::client_greeting_peeker())
            .await
            .map_err(Socks5Error::from_decode_error)
            .and_then(|msg_opt| match msg_opt {
                Some(msg) => Ok(msg),
                None => Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into()),
            })
            .contextualize_err("receving proxyee greeting message")?;

        Ok((
            greeting_msg,
            Greeted::new(self.stream_read, self.stream_write),
        ))
    }
}

impl<Stream> Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    pub fn new(stream: Stream) -> Self {
        let (stream_read, stream_write) = tokio::io::split(stream);

        Self {
            stream_read: BufDecoder::new(stream_read),
            stream_write,
        }
    }
}

pub struct Greeted<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    stream_read: BufDecoder<ReadHalf<Stream>>,
    stream_write: WriteHalf<Stream>,
}

impl<Stream> super::Greeted for Greeted<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    #[allow(refining_impl_trait)]
    async fn send_method_selection_message(
        mut self,
        method: u8,
    ) -> Result<(msg::ClientRequest, Requested<Stream>), Socks5Error> {
        let buf = msg::encode_method_selection(msg::MethodSelection { ver: 5, method })?;
        self.stream_write
            .write_all(buf.as_ref())
            .await
            .contextualize_err("sending method selection message")?;

        let req_msg = self
            .stream_read
            .read_next(msg::client_request_peeker())
            .await
            .map_err(Socks5Error::from_decode_error)
            .and_then(|msg_opt| match msg_opt {
                Some(msg) => Ok(msg),
                None => Err(std::io::Error::other("unexpected eof").into()),
            })
            .contextualize_err("receving request message")?;

        Ok((req_msg, Requested::new(self.stream_read, self.stream_write)))
    }
}

impl<Stream> Greeted<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    pub fn new(stream_read: BufDecoder<ReadHalf<Stream>>, stream_write: WriteHalf<Stream>) -> Self {
        Self {
            stream_read,
            stream_write,
        }
    }
}

pub struct Requested<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    stream_read: BufDecoder<ReadHalf<Stream>>,
    stream_write: WriteHalf<Stream>,
}

impl<Stream> super::Requested for Requested<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    #[allow(refining_impl_trait)]
    async fn reply(
        mut self,
        bound_addr: SocketAddr,
    ) -> Result<(BufDecoder<ReadHalf<Stream>>, WriteHalf<Stream>), std::io::Error> {
        let buf = msg::encode_reply(&msg::Reply {
            ver: 5,
            rep: 0,
            rsv: 0,
            addr: &bound_addr,
        })?;

        self.stream_write.write_all(buf.as_ref()).await?;

        Ok((self.stream_read, self.stream_write))
    }

    async fn reply_error(mut self, err_code: u8) -> Result<(), std::io::Error> {
        let buf = msg::encode_reply(&msg::Reply {
            ver: 5,
            rep: err_code,
            rsv: 0,
            addr: &SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)), 0),
        })?;

        self.stream_write.write_all(buf.as_ref()).await?;

        Ok(())
    }
}

impl<Stream> Requested<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    pub fn new(stream_read: BufDecoder<ReadHalf<Stream>>, stream_write: WriteHalf<Stream>) -> Self {
        Self {
            stream_read,
            stream_write,
        }
    }
}

pub struct Socks5Listener {
    inner: tokio::net::TcpListener,
}

impl Socks5Listener {
    pub async fn bind(addr: SocketAddr) -> Result<Self, Socks5Error> {
        let inner = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self { inner })
    }

    pub async fn accept(&self) -> Result<(Init<TcpStream>, SocketAddr), Socks5Error> {
        let (stream, addr) = self.inner.accept().await?;
        Ok((Init::new(stream), addr))
    }
}

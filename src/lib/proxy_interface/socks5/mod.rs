use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, ReadHalf, WriteHalf};

use crate::codec::{BufDecoder, RequestAddr};

mod msg;

pub struct Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send + Sync,
{
    stream_read: BufDecoder<ReadHalf<Stream>>,
    stream_write: WriteHalf<Stream>,
}

impl<Stream> Init<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send + Sync,
{
    pub fn new(stream: Stream) -> Self {
        let (stream_read, stream_write) = tokio::io::split(stream);

        Self {
            stream_read: BufDecoder::new(stream_read),
            stream_write,
        }
    }

    pub async fn receive_request(
        mut self,
    ) -> Result<(RequestAddr, u16, Requested<Stream>), std::io::Error> {
        let _greeting_msg = self
            .stream_read
            .read_next(
                &msg::client_greeting_peeker(),
                std::time::Duration::from_secs(10),
            )
            .await
            .and_then(|msg_opt| match msg_opt {
                Some(msg) => Ok(msg),
                None => Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into()),
            })?;

        let buf = msg::encode_method_selection(msg::MethodSelection { ver: 5, method: 0 });

        self.stream_write.write_all(buf.as_ref()).await?;

        let req_msg = self
            .stream_read
            .read_next(
                &msg::client_request_peeker(),
                std::time::Duration::from_secs(10),
            )
            .await
            .and_then(|msg_opt| match msg_opt {
                Some(msg) => Ok(msg),
                None => Err(std::io::Error::other("unexpected eof").into()),
            })?;

        Ok((
            req_msg.addr,
            req_msg.port,
            Requested::new(self.stream_read, self.stream_write),
        ))
    }
}

pub struct Requested<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    stream_read: BufDecoder<ReadHalf<Stream>>,
    stream_write: WriteHalf<Stream>,
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

impl<Stream> Requested<Stream>
where
    Stream: AsyncRead + AsyncWrite + 'static + Unpin + Send,
{
    pub async fn reply(
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

    pub async fn reply_error(mut self, err_code: u8) -> Result<(), std::io::Error> {
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

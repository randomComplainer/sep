use std::net::SocketAddr;

use futures::StreamExt;
use futures::prelude::*;
use tracing::*;

use super::msg;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_ahead: u16,
    pub max_packet_size: u16,
}

impl Into<session::sequenced_to_stream::Config> for Config {
    fn into(self) -> session::sequenced_to_stream::Config {
        session::sequenced_to_stream::Config {
            max_packet_ahead: self.max_packet_ahead,
        }
    }
}

impl Into<session::stream_to_sequenced::Config> for Config {
    fn into(self) -> session::stream_to_sequenced::Config {
        session::stream_to_sequenced::Config {
            max_packet_ahead: self.max_packet_ahead,
            max_packet_size: self.max_packet_size,
        }
    }
}

async fn resolve_addrs(
    addr: decode::ReadRequestAddr,
    port: u16,
) -> Result<Vec<SocketAddr>, std::io::Error> {
    let addrs = match addr {
        decode::ReadRequestAddr::Ipv4(addr) => {
            vec![SocketAddr::new(addr.into(), port)]
        }
        decode::ReadRequestAddr::Ipv6(addr) => {
            vec![SocketAddr::new(addr.into(), port)]
        }
        decode::ReadRequestAddr::Domain(addr) => {
            tokio::net::lookup_host((str::from_utf8(addr.as_ref()).unwrap(), port))
                .await?
                .collect()
        }
    };

    Ok(addrs)
}

async fn connect_target(addrs: Vec<SocketAddr>) -> Result<tokio::net::TcpStream, std::io::Error> {
    for addr in addrs {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => {
                return Ok(stream);
            }
            // TODO: collect errors?
            Err(_err) => {
                continue;
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "connect target failed",
    ))
}

pub async fn run(
    mut client_read: impl Stream<Item = msg::ClientMsg> + Unpin,
    mut client_write: impl Sink<msg::ServerMsg, Error = futures::channel::mpsc::SendError>
    + Unpin
    + Clone
    + Send
    + 'static,
    config: Config,
) -> Result<(), std::io::Error> {
    let req = match client_read
        .next()
        .instrument(info_span!("receive request from client"))
        .await
    {
        Some(msg) => match msg {
            msg::ClientMsg::Request(msg) => msg,
            _ => {
                panic!("unexpected client msg: [{:?}], expected request", msg);
            }
        },
        None => {
            error!("client read is broken, exiting");
            return Ok(());
        }
    };

    info!(
        "request received, addr = {:?}, port = {}",
        req.addr, req.port
    );

    let target_stream = match resolve_addrs(req.addr, req.port)
        .instrument(info_span!("resolve target address"))
        .and_then(|resolved| connect_target(resolved).instrument(info_span!("connect target")))
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            error!("connect target failed: {:?}", err);

            match client_write
                .send(msg::ServerMsg::ReplyError(err.into()))
                .instrument(info_span!("send reply error to client"))
                .await
            {
                Ok(_) => {
                    return Ok(());
                }
                Err(err) => {
                    error!("falied to send reply error to client: {:?}", err);
                    return Ok(());
                }
            }
        }
    };

    if let Err(_) = client_write
        .send(msg::ServerMsg::Reply(msg::Reply {
            bound_addr: target_stream.local_addr().unwrap(),
        }))
        .instrument(info_span!("send reply to client"))
        .await
    {
        error!("falied to send reply to client, exiting");
        return Ok(());
    };

    let (target_read, target_write) = target_stream.into_split();

    let (mut target_to_client_cmd_tx, cmd_target_to_client_cmd_rx) =
        futures::channel::mpsc::channel(1);

    let target_to_client = session::stream_to_sequenced::run(
        cmd_target_to_client_cmd_rx,
        client_write.clone().with_sync(|evt| match evt {
            session::stream_to_sequenced::Event::Data(data) => data.into(),
            session::stream_to_sequenced::Event::Eof(eof) => eof.into(),
            session::stream_to_sequenced::Event::IoError(err) => err.into(),
        }),
        target_read,
        None,
        config.into(),
    )
    .instrument(info_span!("target to client"));

    let (mut client_to_target_cmd_tx, client_to_target_cmd_rx) = futures::channel::mpsc::channel(1);
    let client_to_target = session::sequenced_to_stream::run(
        client_to_target_cmd_rx,
        client_write.clone().with_sync(|evt| match evt {
            session::sequenced_to_stream::Event::Ack(ack) => ack.into(),
        }),
        target_write,
        config.into(),
    )
    .instrument(info_span!("client to target"));

    let client_msg_handling = async move {
        while let Some(msg) = client_read.next().await {
            match msg {
                msg::ClientMsg::Data(data) => {
                    if let Err(_) = client_to_target_cmd_tx.send(data.into()).await {
                        debug!("client to target command channel is broken");
                        continue;
                    }
                }
                msg::ClientMsg::Ack(ack) => {
                    if let Err(_) = target_to_client_cmd_tx.send(ack.into()).await {
                        debug!("target to client command channel is broken");
                        continue;
                    }
                }
                msg::ClientMsg::Eof(eof) => {
                    if let Err(_) = client_to_target_cmd_tx.send(eof.into()).await {
                        debug!("client to target command channel is broken");
                        continue;
                    }
                }
                msg::ClientMsg::ProxyeeIoError(_) => {
                    return;
                }
                _ => panic!("unexpected client msg: {:?}", msg),
            }
        }
    }
    .instrument(info_span!("client message handling"));

    let streaming = async move { tokio::try_join!(target_to_client, client_to_target).map(|_| ()) };

    tokio::select! {
        r = streaming => r,
        _ = client_msg_handling => Ok(())
    }
}

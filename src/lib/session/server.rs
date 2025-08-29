use std::net::SocketAddr;

use futures::StreamExt;
use futures::prelude::*;
use tracing::*;

use super::TerminationError;
use super::msg;
use crate::prelude::*;

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

// panic on protocol error (cache overflow/unexpected message)
// Err(()) on closed client message channels
// Ok(()) on completed session OR target io error
// (one can consider a target io error is a completed session)
pub async fn run(
    mut client_read: impl Stream<Item = msg::ClientMsg> + Unpin,
    mut client_write: impl Sink<msg::ServerMsg, Error = futures::channel::mpsc::SendError>
    + Unpin
    + Clone,
) -> Result<(), ()> {
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
            return Err(());
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

            return client_write
                .send(msg::ServerMsg::ReplyError(err.into()))
                .instrument(info_span!("send reply error to client"))
                .await
                .map_err(|_| ());
        }
    };

    client_write
        .send(msg::ServerMsg::Reply(msg::Reply {
            bound_addr: target_stream.local_addr().unwrap(),
        }))
        .instrument(info_span!("send reply to client"))
        .await
        .map_err(|_| ())?;

    let (target_read, target_write) = target_stream.into_split();

    let (mut target_to_client_cmd_tx, cmd_target_to_client_cmd_rx) =
        futures::channel::mpsc::channel(1);

    let target_to_client = crate::session::stream_to_sequenced::run(
        cmd_target_to_client_cmd_rx,
        client_write.clone().with_sync(|evt| match evt {
            crate::session::stream_to_sequenced::Event::Data(data) => data.into(),
            crate::session::stream_to_sequenced::Event::Eof(eof) => eof.into(),
        }),
        target_read,
        None,
    )
    .map_err(TerminationError::from)
    .instrument(info_span!("target to client"));

    let (mut client_to_target_cmd_tx, client_to_target_cmd_rx) = futures::channel::mpsc::channel(1);
    let client_to_target = crate::session::sequenced_to_stream::run(
        client_to_target_cmd_rx,
        client_write.clone().with_sync(|evt| match evt {
            crate::session::sequenced_to_stream::Event::Ack(ack) => ack.into(),
        }),
        target_write,
    )
    .map_err(TerminationError::from)
    .instrument(info_span!("client to target"));

    let client_msg_handling = async move {
        while let Some(msg) = client_read.next().await {
            // it's ok to ignore broken channels for messages to
            // client_to_target & target_to_client
            // because they could be closed due to target io error
            // try_join! will catch it
            match msg {
                msg::ClientMsg::Data(data) => {
                    if let Err(_) = client_to_target_cmd_tx.send(data.into()).await {
                        continue;
                    }
                }
                msg::ClientMsg::Ack(ack) => {
                    if let Err(_) = target_to_client_cmd_tx.send(ack.into()).await {
                        continue;
                    }
                }
                msg::ClientMsg::Eof(eof) => {
                    if let Err(_) = client_to_target_cmd_tx.send(eof.into()).await {
                        continue;
                    }
                }
                msg::ClientMsg::ProxyeeIoError(_) => {
                    return Err::<(), _>(TerminationError::ProxyeeIo);
                }
                _ => panic!("unexpected client msg: {:?}", msg),
            }
        }

        Ok(())
    }
    .instrument(info_span!("client message handling"));

    let streaming = async move { tokio::try_join!(target_to_client, client_to_target).map(|_| ()) };

    match futures::future::select(Box::pin(streaming), Box::pin(client_msg_handling)).await {
        future::Either::Left((r, _)) => TerminationError::to_session_result(r),
        future::Either::Right((r, _)) => TerminationError::to_session_result(r),
    }
}

use futures::prelude::*;
use tracing::*;

use super::TerminationError;
use super::msg;
use crate::prelude::*;

// panic on protocol error (cache overflow/unexpected message)
// Err(()) on closed server message channels
// Ok(()) on completed session OR proxyee io error
// (one can consider a proxyee io error is a completed session)
pub async fn run<ProxyeeStream>(
    proxyee: socks5::agent::Init<ProxyeeStream>,
    mut server_read: impl Stream<Item = msg::ServerMsg> + Send + Unpin + 'static,
    mut server_write: impl Sink<msg::ClientMsg, Error = futures::channel::mpsc::SendError>
    + Unpin
    + Send
    + Clone
    + 'static,
) -> Result<(), ()>
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (_, proxyee) = proxyee
        .receive_greeting_message()
        .instrument(info_span!("receive greeting message from proxyee"))
        .await
        .inspect_err(|err| {
            error!("falied to receive proxyee greeting: {:?}", err);
        })
        .map_err(|_| ())?;

    let (proxyee_req, proxyee) = proxyee
        .send_method_selection_message(0)
        .instrument(info_span!("send method selection message to proxyee"))
        .await
        .inspect_err(|err| {
            error!(
                "falied to send method selection message to proxyee: {:?}",
                err
            );
        })
        .map_err(|_| ())?;

    info!(addr = ?proxyee_req.addr, port = proxyee_req.port, "request addr");

    server_write
        .send(msg::ClientMsg::Request(msg::Request {
            addr: proxyee_req.addr,
            port: proxyee_req.port,
        }))
        .instrument(info_span!("send request to server"))
        .await
        .map_err(|_| ())?;

    let (reply, early_target_cmds) = {
        // target might start send data as soon as server connected to it.
        // so we need to buffer it until client receives server reply.
        let mut early_target_packages: Vec<crate::session::sequenced_to_stream::Command> =
            Vec::new();

        loop {
            match server_read
                .next()
                .instrument(info_span!("receive reply from server"))
                .await
            {
                Some(msg) => match msg {
                    msg::ServerMsg::Reply(msg) => {
                        break (msg, early_target_packages);
                    }
                    msg::ServerMsg::ReplyError(err) => {
                        use session::msg::ConnectionError::*;
                        let _ = proxyee
                            .reply_error(match err {
                                General => 1,
                                NetworkUnreachable => 3,
                                HostUnreachable => 4,
                                ConnectionRefused => 5,
                                TtlExpired => 6,
                            })
                            .instrument(info_span!("send reply error to proxyee"))
                            .await;
                        return Ok(());
                    }
                    msg::ServerMsg::Data(data) => {
                        early_target_packages.push(data.into());
                    }
                    msg::ServerMsg::Eof(eof) => {
                        early_target_packages.push(eof.into());
                    }
                    msg => {
                        panic!("unexpected server msg while receiving reply: [{:?}]", msg);
                    }
                },
                None => {
                    return Err(());
                }
            };
        }
    };

    let (proxyee_read, proxyee_write) = proxyee
        .reply(&reply.bound_addr)
        .instrument(info_span!("reply to proxyee"))
        .await
        .inspect_err(|err| {
            error!("falied to reply to proxyee: {:?}", err);
        })
        // TODO: notify server
        .map_err(|_| ())?;

    let (mut proxyee_to_server_cmd_tx, proxyee_to_server_cmd_rx) =
        futures::channel::mpsc::channel(1);

    let (buf, proxyee_read) = proxyee_read.into_parts();
    let proxyee_to_server = crate::session::stream_to_sequenced::run(
        proxyee_to_server_cmd_rx,
        server_write.clone().with_sync(|evt| match evt {
            crate::session::stream_to_sequenced::Event::Data(data) => data.into(),
            crate::session::stream_to_sequenced::Event::Eof(eof) => eof.into(),
        }),
        proxyee_read,
        Some(buf),
        crate::session::stream_to_sequenced::Config {
            max_package_ahead: crate::session::MAX_DATA_AHEAD,
            max_package_size: crate::session::DATA_BUFF_SIZE,
        },
    )
    .map_err(TerminationError::from)
    .instrument(info_span!("proxyee to server"));

    let (mut server_to_proxyee_cmd_tx, server_to_proxyee_cmd_rx) =
        futures::channel::mpsc::channel(1);

    let server_to_proxyee_early_packages = {
        let mut server_to_proxyee_cmd_tx = server_to_proxyee_cmd_tx.clone();
        async move {
            for cmd in early_target_cmds {
                let _ = server_to_proxyee_cmd_tx.send(cmd).await;
            }
            Ok::<_, TerminationError>(())
        }
    }
    .instrument(info_span!("server to proxyee early packages"));

    let server_to_proxyee = crate::session::sequenced_to_stream::run(
        server_to_proxyee_cmd_rx,
        server_write.clone().with_sync(|evt| match evt {
            crate::session::sequenced_to_stream::Event::Ack(ack) => ack.into(),
        }),
        proxyee_write,
    )
    .map_err(TerminationError::from)
    .instrument(info_span!("server to proxyee"));

    let server_to_proxyee = async move {
        tokio::try_join!(server_to_proxyee, server_to_proxyee_early_packages).map(|_| ())
    };

    let server_msg_handling = async move {
        while let Some(msg) = server_read.next().await {
            match msg {
                msg::ServerMsg::Data(data) => {
                    if let Err(_) = server_to_proxyee_cmd_tx.send(data.into()).await {
                        continue;
                    }
                }
                msg::ServerMsg::Eof(eof) => {
                    if let Err(_) = server_to_proxyee_cmd_tx.send(eof.into()).await {
                        continue;
                    }
                }
                msg::ServerMsg::Ack(ack) => {
                    if let Err(_) = proxyee_to_server_cmd_tx.send(ack.into()).await {
                        continue;
                    }
                }
                msg::ServerMsg::TargetIoError(_) => {
                    return Result::<(), _>::Err(TerminationError::TargetIo);
                }
                _ => panic!("unexpected server msg: {:?}", msg),
            }
        }

        Ok(())
    };

    let streaming =
        async move { tokio::try_join!(server_to_proxyee, proxyee_to_server).map(|_| ()) };

    match futures::future::select(Box::pin(streaming), Box::pin(server_msg_handling)).await {
        future::Either::Left((r, _)) => TerminationError::to_session_result(r),
        future::Either::Right((r, _)) => TerminationError::to_session_result(r),
    }
}

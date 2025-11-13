use futures::prelude::*;
use tracing::*;

use super::msg;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_ahead: u16,
    pub max_packet_size: usize,
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

// panic on protocol error (cache overflow/unexpected message)
// Err(()) on closed server message channels
// Ok(()) on completed session OR proxyee io error
// (one can consider a proxyee io error is a completed session)
pub async fn run<ProxyeeStream>(
    proxyee: socks5::agent::Init<ProxyeeStream>,
    mut server_read: impl Stream<Item = msg::ServerMsg> + Send + Unpin + 'static,
    mut server_write: impl Sink<msg::ClientMsg, Error = impl std::fmt::Debug>
    + Unpin
    + Send
    + Clone
    + 'static,
    config: Config,
) -> Result<(), std::io::Error>
where
    ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // TODO: duplicated code
    let (_, proxyee) = proxyee
        .receive_greeting_message()
        .instrument(info_span!("receive greeting message from proxyee"))
        .await
        .map_err(|err| match err {
            socks5::Socks5Error::Io(err) => err,
            err => {
                panic!("socks5 error: {:?}", err);
            }
        })?;

    let (proxyee_req, proxyee) = proxyee
        .send_method_selection_message(0)
        .instrument(info_span!("send method selection message to proxyee"))
        .await
        .map_err(|err| match err {
            socks5::Socks5Error::Io(err) => err,
            err => {
                panic!("socks5 error: {:?}", err);
            }
        })?;

    info!(addr = ?proxyee_req.addr, port = proxyee_req.port, "request addr");

    match server_write
        .send(msg::ClientMsg::Request(msg::Request {
            addr: proxyee_req.addr,
            port: proxyee_req.port,
        }))
        .instrument(info_span!("send request to server"))
        .await
    {
        Ok(_) => (),
        Err(err) => {
            error!("falied to send request to server, exiting: {:?}", err);
            return Ok(());
        }
    };

    let (reply, early_target_cmds) = {
        // target might start send data as soon as server connected to it.
        // so we need to buffer it until client receives server reply.
        let mut early_target_packages: Vec<session::sequenced_to_stream::Command> =
            Vec::with_capacity(config.max_packet_ahead as usize);

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
                    error!("server read is broken, exiting");
                    return Ok(());
                }
            };
        }
    };

    let (proxyee_read, proxyee_write) = match proxyee
        .reply(&reply.bound_addr)
        .instrument(info_span!("reply to proxyee"))
        .await
    {
        Ok(x) => x,
        Err(err) => {
            // TODO: notify server
            error!("falied to reply to proxyee: {:?}", err);
            return Ok(());
        }
    };

    let (mut proxyee_to_server_cmd_tx, proxyee_to_server_cmd_rx) =
        futures::channel::mpsc::channel(1);

    let (buf, proxyee_read) = proxyee_read.into_parts();
    let proxyee_to_server = session::stream_to_sequenced::run(
        proxyee_to_server_cmd_rx,
        server_write.clone().with_sync(|evt| match evt {
            session::stream_to_sequenced::Event::Data(data) => data.into(),
            session::stream_to_sequenced::Event::Eof(eof) => eof.into(),
        }),
        proxyee_read,
        Some(buf),
        config.into(),
    )
    .instrument(info_span!("proxyee to server"));

    let (mut server_to_proxyee_cmd_tx, server_to_proxyee_cmd_rx) =
        futures::channel::mpsc::channel(1);

    let server_to_proxyee_early_packages = {
        let mut server_to_proxyee_cmd_tx = server_to_proxyee_cmd_tx.clone();
        async move {
            for cmd in early_target_cmds {
                let _ = server_to_proxyee_cmd_tx.send(cmd).await;
            }
            Ok::<_, std::io::Error>(())
        }
    }
    .instrument(info_span!("server to proxyee early packages"));

    let server_to_proxyee = session::sequenced_to_stream::run(
        server_to_proxyee_cmd_rx,
        server_write.clone().with_sync(|evt| match evt {
            session::sequenced_to_stream::Event::Ack(ack) => ack.into(),
        }),
        proxyee_write,
        config.into(),
    )
    .instrument(info_span!("server to proxyee"));

    let server_to_proxyee = async move {
        tokio::try_join!(server_to_proxyee, server_to_proxyee_early_packages).map(|_| ())
    };

    let server_msg_handling = async move {
        while let Some(msg) = server_read
            .next()
            .instrument(info_span!("read next msg from server"))
            .await
        {
            debug!("server msg: {:?}", msg);
            match msg {
                msg::ServerMsg::Data(data) => {
                    let cmd = data.into();
                    let span = info_span!("send command to server to proxyee task", cmd = ?cmd);
                    if let Err(_) = server_to_proxyee_cmd_tx.send(cmd).instrument(span).await {
                        debug!("server to proxyee command channel is broken");
                        continue;
                    }
                }
                msg::ServerMsg::Eof(eof) => {
                    let cmd = eof.into();
                    let span = info_span!("send command to server to proxyee task", cmd = ?cmd);
                    if let Err(_) = server_to_proxyee_cmd_tx.send(cmd).instrument(span).await {
                        debug!("server to proxyee command channel is broken");
                        continue;
                    }
                }
                msg::ServerMsg::Ack(ack) => {
                    let span = tracing::debug_span!("forward ack to server", ack = ?ack);
                    if let Err(_) = proxyee_to_server_cmd_tx
                        .send(ack.into())
                        .instrument(span)
                        .await
                    {
                        debug!("proxyee to server command channel is broken");
                        continue;
                    }
                }
                msg::ServerMsg::TargetIoError(_) => {
                    return;
                }
                _ => panic!("unexpected server msg: {:?}", msg),
            }
        }

        warn!("end of server message, exiting");
    }
    .instrument(info_span!("server_msg_handling"));

    let streaming =
        async move { tokio::try_join!(server_to_proxyee, proxyee_to_server).map(|_| ()) };

    tokio::select! {
        r = streaming => r,
        _ = server_msg_handling => Ok(())
    }
}

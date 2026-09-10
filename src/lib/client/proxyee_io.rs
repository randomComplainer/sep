use derive_more::From;
use futures::prelude::*;
use tracing::Instrument as _;

use crate::{prelude::*, stream_to_sequenced};
use protocol::msg::session as msg;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u64,
}

impl Into<stream_to_sequenced::Config> for Config {
    fn into(self) -> stream_to_sequenced::Config {
        stream_to_sequenced::Config {
            max_bytes_ahead: self.max_bytes_ahead,
        }
    }
}

#[derive(From, Debug)]
pub enum Cmd {
    ServerMsg(#[from] msg::ServerMsg),
}

// panic on protocol error (cache overflow/unexpected message)
// Err(()) on closed server message channels
// Ok(()) on completed session OR proxyee io error
// (one can consider a proxyee io error is a completed session)
pub async fn run(
    proxyee: proxy_interface::Init<
        impl tokio::io::AsyncRead + tokio::io::AsyncWrite + 'static + Unpin + Send + Sync,
    >,
    cmd_read: impl Stream<Item = Cmd> + Send + Unpin + 'static,
    server_write: impl Sink<msg::ClientMsg> + Unpin + Send + Clone + 'static,
    buf_pool: crate::buffer_pool::BufferPool,
    config: Config,
) -> Result<(), std::io::Error> {
    let mut cmd_read = cmd_read.inspect(|cmd| tracing::debug!(cmd = ?cmd, "cmd"));
    let mut server_write = server_write.inspect(|msg| tracing::debug!(msg = ?msg, "client msg"));

    let (addr, port, proxyee) = proxyee
        .receive_request()
        .instrument(tracing::trace_span!("receive request from proxyee"))
        .inspect_err(|err| tracing::error!(?err, "error in receiving request from proxyee"))
        .map_err(|err| Into::<std::io::Error>::into(err))
        .await?;

    tracing::info!(addr = ?addr, port = port, "request");

    let client_msg = msg::Request { addr, port }.into();

    ok_or!(
        server_write
            .send(client_msg)
            .instrument(tracing::trace_span!("send request to server"))
            .await,
        return Ok(())
    );

    let (reply, early_target_cmds) = {
        // target might start send data as soon as server connected to it.
        // so we need to buffer it until client receives server reply.
        // TODO: Magic btw
        let mut early_target_packages: Vec<crate::sequenced_to_stream::Command> =
            Vec::with_capacity(8);

        loop {
            match cmd_read
                .next()
                .instrument(tracing::trace_span!("receive reply from server"))
                .await
            {
                Some(cmd) => match cmd {
                    Cmd::ServerMsg(msg) => match msg {
                        msg::ServerMsg::Reply(msg) => {
                            break (msg, early_target_packages);
                        }
                        msg::ServerMsg::ReplyError(err) => {
                            use protocol::msg::session::ConnectionError::*;
                            let _ = proxyee
                                .reply_error(match err {
                                    General => 1,
                                    NetworkUnreachable => 3,
                                    HostUnreachable => 4,
                                    ConnectionRefused => 5,
                                    TtlExpired => 6,
                                })
                                .instrument(tracing::trace_span!("send reply error to proxyee"))
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
                },
                None => {
                    tracing::warn!("unexpected end of server message, exiting");
                    let _ = proxyee
                        .reply_error(1)
                        .instrument(tracing::trace_span!("send reply error to proxyee"))
                        .await;

                    return Ok(());
                }
            };
        }
    };

    let (proxyee_read, proxyee_write) = ok_or!(
        proxyee
            .reply(reply.bound_addr)
            .instrument(tracing::trace_span!("reply to proxyee"))
            .await,
        return Ok(())
    );

    let (mut proxyee_to_server_cmd_tx, proxyee_to_server_cmd_rx) =
        futures::channel::mpsc::unbounded();

    let (buf, proxyee_read) = proxyee_read.into_parts();
    let proxyee_to_server = stream_to_sequenced::run(
        proxyee_to_server_cmd_rx,
        server_write.clone().with_sync(|evt| match evt {
            stream_to_sequenced::Event::Data(data) => data.into(),
            stream_to_sequenced::Event::Eof(eof) => eof.into(),
        }),
        buf_pool,
        proxyee_read,
        Some(buf),
        config.into(),
    )
    .instrument(tracing::trace_span!("proxyee to server"));

    let (mut server_to_proxyee_cmd_tx, server_to_proxyee_cmd_rx) =
        futures::channel::mpsc::unbounded();

    let server_to_proxyee_early_packages = {
        let mut server_to_proxyee_cmd_tx = server_to_proxyee_cmd_tx.clone();
        async move {
            for cmd in early_target_cmds {
                let _ = server_to_proxyee_cmd_tx.send(cmd).await;
            }
            Ok::<_, std::io::Error>(())
        }
    }
    .instrument(tracing::trace_span!("server to proxyee early packages"));

    let server_to_proxyee = crate::sequenced_to_stream::run(
        server_to_proxyee_cmd_rx,
        server_write.clone().with_sync(|evt| match evt {
            crate::sequenced_to_stream::Event::Ack(ack) => ack.into(),
            crate::sequenced_to_stream::Event::EofAck(eof_ack) => eof_ack.into(),
        }),
        proxyee_write,
    )
    .instrument(tracing::trace_span!("server to proxyee"));

    let server_to_proxyee = async move {
        tokio::try_join!(server_to_proxyee, server_to_proxyee_early_packages).map(|_| ())
    };

    let server_msg_handling = async move {
        while let Some(cmd) = cmd_read
            .next()
            .instrument(tracing::trace_span!("recv server msg"))
            .await
        {
            match cmd {
                Cmd::ServerMsg(msg) => match msg {
                    msg::ServerMsg::Data(data) => {
                        ok_or!(server_to_proxyee_cmd_tx.send(data.into()).await, return);
                    }
                    msg::ServerMsg::Eof(eof) => {
                        ok_or!(server_to_proxyee_cmd_tx.send(eof.into()).await, return);
                    }
                    msg::ServerMsg::Ack(ack) => {
                        ok_or!(proxyee_to_server_cmd_tx.send(ack.into()).await, return);
                    }
                    msg::ServerMsg::EofAck(eof_ack) => {
                        ok_or!(proxyee_to_server_cmd_tx.send(eof_ack.into()).await, return);
                    }
                    _ => panic!("unexpected server msg: {:?}", msg),
                },
            }
        }

        tracing::warn!("end of server message, exiting");
    };

    let streaming = async move {
        // streaming tasks error on Io Error, which doesn't matter
        tokio::try_join!(server_to_proxyee, proxyee_to_server).map(|_| ())
    };

    let result = tokio::select! {
        r = streaming => r,
        _ = server_msg_handling => Ok(())
    };

    tracing::debug!("session ends");

    result
}

#[cfg(test)]
mod tests {}

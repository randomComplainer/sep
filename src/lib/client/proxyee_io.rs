use derive_more::From;
use futures::prelude::*;
use tracing::Instrument as _;

use crate::{ok_or_return, ok_or_return_ok, prelude::*, stream_to_sequenced};
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
    proxyee: impl socks5::server_agent::Init,
    cmd_read: impl Stream<Item = Cmd> + Send + Unpin + 'static,
    server_write: impl Sink<msg::ClientMsg> + Unpin + Send + Clone + 'static,
    buf_pool: crate::buffer_pool::BufferPool,
    config: Config,
) -> Result<(), std::io::Error> {
    let mut cmd_read = cmd_read.inspect(|cmd| tracing::debug!(cmd = ?cmd, "cmd"));
    let mut server_write = server_write.inspect(|msg| tracing::debug!(msg = ?msg, "client msg"));

    // TODO: duplicated code
    let (_, proxyee) = proxyee
        .receive_greeting_message()
        .instrument(tracing::trace_span!(
            "receive greeting message from proxyee"
        ))
        .inspect_err(|err| tracing::error!("socks5 error: {:?}", err))
        .map_err(|err| Into::<std::io::Error>::into(err))
        .await?;

    let (proxyee_req, proxyee) = proxyee
        .send_method_selection_message(0)
        .instrument(tracing::trace_span!(
            "send method selection message to proxyee"
        ))
        .inspect_err(|err| tracing::error!("socks5 error: {:?}", err))
        .map_err(|err| Into::<std::io::Error>::into(err))
        .await?;

    tracing::info!(addr = ?proxyee_req.addr, port = proxyee_req.port, "request");

    let client_msg = msg::Request {
        addr: proxyee_req.addr,
        port: proxyee_req.port,
    }
    .into();

    ok_or_return_ok!(
        server_write
            .send(client_msg)
            .instrument(tracing::trace_span!("send request to server"))
            .await
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

    let (proxyee_read, proxyee_write) = ok_or_return_ok!(
        proxyee
            .reply(reply.bound_addr)
            .instrument(tracing::trace_span!("reply to proxyee"))
            .await
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
                        ok_or_return!(server_to_proxyee_cmd_tx.send(data.into()).await);
                    }
                    msg::ServerMsg::Eof(eof) => {
                        ok_or_return!(server_to_proxyee_cmd_tx.send(eof.into()).await);
                    }
                    msg::ServerMsg::Ack(ack) => {
                        ok_or_return!(proxyee_to_server_cmd_tx.send(ack.into()).await);
                    }
                    msg::ServerMsg::EofAck(eof_ack) => {
                        ok_or_return!(proxyee_to_server_cmd_tx.send(eof_ack.into()).await);
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

    tokio::select! {
        r = streaming => r,
        _ = server_msg_handling => Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use std::net::SocketAddr;
    //
    // use bytes::BytesMut;
    // use tokio::io::{AsyncReadExt as _, AsyncWriteExt};
    //
    // use super::*;
    //
    // #[tokio::test]
    // async fn happy_path() {
    //     let (proxyee_stream, proxyee_stream_handle) = tokio::io::duplex(1024);
    //     let (mut proxyee_stream_handle_read, mut proxyee_stream_handle_write) =
    //         tokio::io::split(proxyee_stream_handle);
    //     proxyee_stream_handle_write
    //         .write_all(&mut [2u8, 3, 4])
    //         .await
    //         .unwrap();
    //     drop(proxyee_stream_handle_write);
    //
    //     let (proxyee_read, proxyee_write) = tokio::io::split(proxyee_stream);
    //
    //     let proxyee = socks5::server_agent::mock::script()
    //         .provide_greeting_message(socks5::msg::ClientGreeting {
    //             ver: 5,
    //             methods: BytesMut::from([0].as_ref()),
    //         })
    //         .expect_method_selection(0)
    //         .provide_request_message(socks5::msg::ClientRequest {
    //             ver: 5,
    //             cmd: 1,
    //             rsv: 0,
    //             addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //             port: 7878,
    //         })
    //         .expect_reply(SocketAddr::new(
    //             std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //             80,
    //         ))
    //         .provide_stream(BytesMut::from([1].as_ref()), proxyee_read, proxyee_write);
    //
    //     let (mut server_msg_tx, server_msg_rx) = futures::channel::mpsc::channel(1);
    //     let (client_msg_tx, mut client_msg_rx) = futures::channel::mpsc::channel(1);
    //
    //     let main_task = run(
    //         proxyee,
    //         server_msg_rx,
    //         client_msg_tx,
    //         Config {
    //             max_packet_ahead: 1,
    //             max_packet_size: 10,
    //         },
    //     );
    //
    //     let server_task = async move {
    //         assert_eq!(
    //             client_msg_rx.next().await.unwrap(),
    //             session::msg::ClientMsg::Request(msg::Request {
    //                 addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                 port: 7878,
    //             })
    //         );
    //
    //         server_msg_tx
    //             .send(
    //                 msg::Reply {
    //                     bound_addr: SocketAddr::new(
    //                         std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                         80,
    //                     ),
    //                 }
    //                 .into(),
    //             )
    //             .await
    //             .unwrap();
    //
    //         assert_eq!(
    //             client_msg_rx.next().await.unwrap(),
    //             session::msg::Data {
    //                 seq: 0,
    //                 data: BytesMut::from([1].as_ref())
    //             }
    //             .into()
    //         );
    //
    //         server_msg_tx
    //             .send(msg::Ack { bytes: 1 }.into())
    //             .await
    //             .unwrap();
    //
    //         assert_eq!(
    //             client_msg_rx.next().await.unwrap(),
    //             session::msg::Data {
    //                 seq: 1,
    //                 data: BytesMut::from([2, 3, 4].as_ref())
    //             }
    //             .into()
    //         );
    //
    //         server_msg_tx
    //             .send(msg::Ack { bytes: 3 }.into())
    //             .await
    //             .unwrap();
    //
    //         server_msg_tx
    //             .send(
    //                 msg::Data {
    //                     seq: 0,
    //                     data: BytesMut::from([4, 3, 2, 1].as_ref()),
    //                 }
    //                 .into(),
    //             )
    //             .await
    //             .unwrap();
    //
    //         assert_eq!(
    //             client_msg_rx.next().await.unwrap(),
    //             session::msg::Ack { seq: 0 }.into()
    //         );
    //
    //         let mut buf = vec![0u8; 4];
    //         proxyee_stream_handle_read
    //             .read_exact(&mut buf)
    //             .await
    //             .unwrap();
    //
    //         assert_eq!(buf, vec![4, 3, 2, 1]);
    //         drop(proxyee_stream_handle_read);
    //
    //         assert_eq!(
    //             client_msg_rx.next().await.unwrap(),
    //             session::msg::Eof { seq: 2 }.into()
    //         );
    //
    //         server_msg_tx
    //             .send(msg::Ack { seq: 2 }.into())
    //             .await
    //             .unwrap();
    //
    //         server_msg_tx
    //             .send(msg::Eof { seq: 1 }.into())
    //             .await
    //             .unwrap();
    //
    //         assert_eq!(
    //             client_msg_rx.next().await.unwrap(),
    //             session::msg::Ack { seq: 1 }.into()
    //         );
    //     };
    //
    //     tokio::join!(main_task, server_task);
    // }
    //
    // mod proxyee_interaction {
    //     use super::*;
    //
    //     #[tokio::test]
    //     async fn server_failed_to_connect_to_target() {
    //         let proxyee = socks5::server_agent::mock::script()
    //             .provide_greeting_message(socks5::msg::ClientGreeting {
    //                 ver: 5,
    //                 methods: BytesMut::from([0].as_ref()),
    //             })
    //             .expect_method_selection(0)
    //             .provide_request_message(socks5::msg::ClientRequest {
    //                 ver: 5,
    //                 cmd: 1,
    //                 rsv: 0,
    //                 addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                 port: 7878,
    //             })
    //             .expect_reply_error(1);
    //
    //         let (mut server_msg_tx, server_msg_rx) = futures::channel::mpsc::channel(1);
    //         let (client_msg_tx, mut client_msg_rx) = futures::channel::mpsc::channel(1);
    //
    //         let main_task = run(
    //             proxyee,
    //             server_msg_rx,
    //             client_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1,
    //                 max_packet_size: 10,
    //             },
    //         );
    //
    //         let server_task = async move {
    //             assert_eq!(
    //                 client_msg_rx.next().await.unwrap(),
    //                 session::msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                     port: 7878,
    //                 })
    //             );
    //
    //             server_msg_tx
    //                 .send(msg::ConnectionError::General.into())
    //                 .await
    //                 .unwrap();
    //             drop(server_msg_tx);
    //         };
    //
    //         tokio::join!(main_task, server_task);
    //     }
    //
    //     #[tokio::test]
    //     async fn broken_server_connection_during_requesting() {
    //         let proxyee = socks5::server_agent::mock::script()
    //             .provide_greeting_message(socks5::msg::ClientGreeting {
    //                 ver: 5,
    //                 methods: BytesMut::from([0].as_ref()),
    //             })
    //             .expect_method_selection(0)
    //             .provide_request_message(socks5::msg::ClientRequest {
    //                 ver: 5,
    //                 cmd: 1,
    //                 rsv: 0,
    //                 addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                 port: 7878,
    //             })
    //             .expect_reply_error(1);
    //
    //         let (server_msg_tx, server_msg_rx) = futures::channel::mpsc::channel(1);
    //         let (client_msg_tx, mut client_msg_rx) = futures::channel::mpsc::channel(1);
    //
    //         let main_task = run(
    //             proxyee,
    //             server_msg_rx,
    //             client_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1,
    //                 max_packet_size: 10,
    //             },
    //         );
    //
    //         let server_task = async move {
    //             assert_eq!(
    //                 client_msg_rx.next().await.unwrap(),
    //                 session::msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                     port: 7878,
    //                 })
    //             );
    //
    //             drop(server_msg_tx);
    //         };
    //
    //         tokio::join!(main_task, server_task);
    //     }
    //
    //     #[tokio::test]
    //     async fn broken_server_connection_during_streaming() {
    //         let (proxyee_stream, proxyee_stream_handle) = tokio::io::duplex(1024);
    //         let (mut proxyee_stream_handle_read, proxyee_stream_handle_write) =
    //             tokio::io::split(proxyee_stream_handle);
    //         drop(proxyee_stream_handle_write);
    //
    //         let (proxyee_read, proxyee_write) = tokio::io::split(proxyee_stream);
    //
    //         let proxyee = socks5::server_agent::mock::script()
    //             .provide_greeting_message(socks5::msg::ClientGreeting {
    //                 ver: 5,
    //                 methods: BytesMut::from([0].as_ref()),
    //             })
    //             .expect_method_selection(0)
    //             .provide_request_message(socks5::msg::ClientRequest {
    //                 ver: 5,
    //                 cmd: 1,
    //                 rsv: 0,
    //                 addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                 port: 7878,
    //             })
    //             .expect_reply(SocketAddr::new(
    //                 std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                 80,
    //             ))
    //             .provide_stream(BytesMut::new(), proxyee_read, proxyee_write);
    //
    //         let (mut server_msg_tx, server_msg_rx) = futures::channel::mpsc::channel(1);
    //         let (client_msg_tx, mut client_msg_rx) = futures::channel::mpsc::channel(1);
    //
    //         let main_task = run(
    //             proxyee,
    //             server_msg_rx,
    //             client_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1,
    //                 max_packet_size: 10,
    //             },
    //         );
    //
    //         let server_task = async move {
    //             assert_eq!(
    //                 client_msg_rx.next().await.unwrap(),
    //                 session::msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                     port: 7878,
    //                 })
    //             );
    //
    //             server_msg_tx
    //                 .send(
    //                     msg::Reply {
    //                         bound_addr: SocketAddr::new(
    //                             std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                             80,
    //                         ),
    //                     }
    //                     .into(),
    //                 )
    //                 .await
    //                 .unwrap();
    //
    //             server_msg_tx
    //                 .send(
    //                     msg::Data {
    //                         seq: 0,
    //                         data: BytesMut::from([1, 2, 3, 4].as_ref()),
    //                     }
    //                     .into(),
    //                 )
    //                 .await
    //                 .unwrap();
    //
    //             let mut buf = vec![0u8; 4];
    //             proxyee_stream_handle_read
    //                 .read_exact(&mut buf)
    //                 .await
    //                 .unwrap();
    //
    //             assert_eq!(buf, vec![1, 2, 3, 4]);
    //
    //             drop(server_msg_tx);
    //         };
    //
    //         tokio::join!(main_task, server_task);
    //     }
    //
    //     #[tokio::test]
    //     async fn io_error_message_during_streaming() {
    //         let (proxyee_stream, proxyee_stream_handle) = tokio::io::duplex(1024);
    //         let (mut proxyee_stream_handle_read, _proxyee_stream_handle_write) =
    //             tokio::io::split(proxyee_stream_handle);
    //
    //         let (proxyee_read, proxyee_write) = tokio::io::split(proxyee_stream);
    //
    //         let proxyee = socks5::server_agent::mock::script()
    //             .provide_greeting_message(socks5::msg::ClientGreeting {
    //                 ver: 5,
    //                 methods: BytesMut::from([0].as_ref()),
    //             })
    //             .expect_method_selection(0)
    //             .provide_request_message(socks5::msg::ClientRequest {
    //                 ver: 5,
    //                 cmd: 1,
    //                 rsv: 0,
    //                 addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                 port: 7878,
    //             })
    //             .expect_reply(SocketAddr::new(
    //                 std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                 80,
    //             ))
    //             .provide_stream(BytesMut::new(), proxyee_read, proxyee_write);
    //
    //         let (mut server_msg_tx, server_msg_rx) = futures::channel::mpsc::channel(1);
    //         let (client_msg_tx, mut client_msg_rx) = futures::channel::mpsc::channel(1);
    //
    //         let main_task = run(
    //             proxyee,
    //             server_msg_rx,
    //             client_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1,
    //                 max_packet_size: 10,
    //             },
    //         );
    //
    //         let server_task = async move {
    //             assert_eq!(
    //                 client_msg_rx.next().await.unwrap(),
    //                 session::msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                     port: 7878,
    //                 })
    //             );
    //
    //             server_msg_tx
    //                 .send(
    //                     msg::Reply {
    //                         bound_addr: SocketAddr::new(
    //                             std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                             80,
    //                         ),
    //                     }
    //                     .into(),
    //                 )
    //                 .await
    //                 .unwrap();
    //
    //             server_msg_tx
    //                 .send(
    //                     msg::Data {
    //                         seq: 0,
    //                         data: BytesMut::from([1, 2, 3, 4].as_ref()),
    //                     }
    //                     .into(),
    //                 )
    //                 .await
    //                 .unwrap();
    //
    //             let mut buf = vec![0u8; 4];
    //             proxyee_stream_handle_read
    //                 .read_exact(&mut buf)
    //                 .await
    //                 .unwrap();
    //
    //             assert_eq!(buf, vec![1, 2, 3, 4]);
    //
    //             server_msg_tx.send(msg::IoError.into()).await.unwrap();
    //
    //             let mut buf = vec![0u8; 4];
    //             let n = proxyee_stream_handle_read.read(&mut buf).await.unwrap();
    //             assert_eq!(n, 0);
    //         };
    //
    //         tokio::join!(main_task, server_task);
    //     }
    // }
    //
    // mod server_interaction {
    //     use super::*;
    //
    //     #[tokio::test]
    //     async fn proxyee_disconnects_after_request() {
    //         let proxyee = socks5::server_agent::mock::script()
    //             .provide_greeting_message(socks5::msg::ClientGreeting {
    //                 ver: 5,
    //                 methods: BytesMut::from([0].as_ref()),
    //             })
    //             .expect_method_selection(0)
    //             .provide_request_message(socks5::msg::ClientRequest {
    //                 ver: 5,
    //                 cmd: 1,
    //                 rsv: 0,
    //                 addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                 port: 7878,
    //             })
    //             .expect_reply(SocketAddr::new(
    //                 std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                 80,
    //             ))
    //             .provide_io_error(std::io::Error::new(std::io::ErrorKind::Other, "test"));
    //
    //         let (mut server_msg_tx, server_msg_rx) = futures::channel::mpsc::channel(1);
    //         let (client_msg_tx, mut client_msg_rx) = futures::channel::mpsc::channel(1);
    //
    //         let main_task = run(
    //             proxyee,
    //             server_msg_rx,
    //             client_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1,
    //                 max_packet_size: 10,
    //             },
    //         );
    //
    //         let server_task = async move {
    //             assert_eq!(
    //                 client_msg_rx.next().await.unwrap(),
    //                 session::msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                     port: 7878,
    //                 })
    //             );
    //
    //             server_msg_tx
    //                 .send(
    //                     msg::Reply {
    //                         bound_addr: SocketAddr::new(
    //                             std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                             80,
    //                         ),
    //                     }
    //                     .into(),
    //                 )
    //                 .await
    //                 .unwrap();
    //         };
    //
    //         tokio::join!(main_task, server_task);
    //     }
    //
    //     #[tokio::test]
    //     async fn proxyee_io_error_during_streaming() {
    //         let proxyee = socks5::server_agent::mock::script()
    //             .provide_greeting_message(socks5::msg::ClientGreeting {
    //                 ver: 5,
    //                 methods: BytesMut::from([0].as_ref()),
    //             })
    //             .expect_method_selection(0)
    //             .provide_request_message(socks5::msg::ClientRequest {
    //                 ver: 5,
    //                 cmd: 1,
    //                 rsv: 0,
    //                 addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                 port: 7878,
    //             })
    //             .expect_reply(SocketAddr::new(
    //                 std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                 80,
    //             ))
    //             .provide_stream(
    //                 BytesMut::new(),
    //                 tokio_test::io::Builder::new()
    //                     .read_error(std::io::Error::new(std::io::ErrorKind::Other, "test"))
    //                     .build(),
    //                 tokio_test::io::Builder::new().build(),
    //             );
    //
    //         let (mut server_msg_tx, server_msg_rx) = futures::channel::mpsc::channel(1);
    //         let (client_msg_tx, mut client_msg_rx) = futures::channel::mpsc::channel(1);
    //
    //         let main_task = run(
    //             proxyee,
    //             server_msg_rx,
    //             client_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1,
    //                 max_packet_size: 10,
    //             },
    //         );
    //
    //         let server_task = async move {
    //             assert_eq!(
    //                 client_msg_rx.next().await.unwrap(),
    //                 session::msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain(BytesMut::from("example.com")),
    //                     port: 7878,
    //                 })
    //             );
    //
    //             server_msg_tx
    //                 .send(
    //                     msg::Reply {
    //                         bound_addr: SocketAddr::new(
    //                             std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    //                             80,
    //                         ),
    //                     }
    //                     .into(),
    //                 )
    //                 .await
    //                 .unwrap();
    //
    //             assert_eq!(
    //                 client_msg_rx.next().await.unwrap(),
    //                 session::msg::IoError.into()
    //             );
    //         };
    //
    //         tokio::join!(main_task, server_task);
    //     }
    // }
}

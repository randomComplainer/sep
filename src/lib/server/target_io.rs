use derive_more::From;
use futures::StreamExt;
use futures::prelude::*;
use tracing::Instrument as _;

use crate::ok_or;
use crate::prelude::*;
use crate::some_or;
use crate::stream_to_sequenced;
use protocol::msg::session as msg;

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u64,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<stream_to_sequenced::Config> for Config<TConnectTarget> {
    fn into(self) -> stream_to_sequenced::Config {
        stream_to_sequenced::Config {
            max_bytes_ahead: self.max_bytes_ahead,
        }
    }
}

#[derive(From, Debug)]
pub enum Cmd {
    ClientMsg(#[from] msg::ClientMsg),
}

pub async fn run<TConnectTarget>(
    cmd_read: impl Stream<Item = Cmd> + Unpin,
    server_msg_write: impl Sink<msg::ServerMsg> + Unpin + Clone + Send + 'static,
    buf_pool: crate::buffer_pool::BufferPool,
    config: Config<TConnectTarget>,
) -> Result<(), std::io::Error>
where
    TConnectTarget: ConnectTarget,
{
    let mut cmd_read = cmd_read.inspect(|cmd| tracing::debug!( cmd = ?cmd, "cmd"));

    let mut server_msg_write =
        server_msg_write.inspect(|msg| tracing::debug!(msg = ?msg, "server msg"));

    let req = match some_or!(
        cmd_read
            .next()
            .instrument(tracing::trace_span!("receive request from client"))
            .await,
        return Ok(())
    ) {
        Cmd::ClientMsg(msg::ClientMsg::Request(msg)) => msg,
        cmd => {
            panic!("unexpected cmd: [{:?}], expected request message", cmd);
        }
    };

    tracing::debug!(addr = ?req.addr, port = ?req.port, "request");

    let (target_stream, local_addr) = match config
        .connect_target
        .connect(req.addr, req.port)
        .instrument(tracing::trace_span!("connect to target"))
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            tracing::error!(?err, "failed to connect to target");

            let msg = msg::ServerMsg::ReplyError(err.into());
            let _ = server_msg_write
                .send(msg)
                .instrument(tracing::trace_span!("send reply error to client"))
                .await;
            return Ok(());
        }
    };

    let server_msg = msg::Reply {
        bound_addr: local_addr,
    }
    .into();

    ok_or!(
        server_msg_write
            .send(server_msg)
            .instrument(tracing::trace_span!("send reply to client"))
            .await,
        return Ok(())
    );

    let (target_read, target_write) = tokio::io::split(target_stream);

    let (mut target_to_client_cmd_tx, cmd_target_to_client_cmd_rx) =
        futures::channel::mpsc::unbounded();

    let target_to_client = stream_to_sequenced::run(
        cmd_target_to_client_cmd_rx,
        server_msg_write.clone().with_sync(|evt| match evt {
            stream_to_sequenced::Event::Data(data) => data.into(),
            stream_to_sequenced::Event::Eof(eof) => eof.into(),
        }),
        buf_pool,
        target_read,
        None,
        config.clone().into(),
    )
    .instrument_with_result(tracing::trace_span!("target to client"));

    let (mut client_to_target_cmd_tx, client_to_target_cmd_rx) =
        futures::channel::mpsc::unbounded();
    let client_to_target = crate::sequenced_to_stream::run(
        client_to_target_cmd_rx,
        server_msg_write.clone().with_sync(|evt| match evt {
            crate::sequenced_to_stream::Event::Ack(ack) => ack.into(),
            crate::sequenced_to_stream::Event::EofAck(eof_ack) => eof_ack.into(),
        }),
        target_write,
    )
    .instrument_with_result(tracing::trace_span!("client to target"));

    let client_msg_handling = async move {
        while let Some(cmd) = cmd_read.next().await {
            match cmd {
                Cmd::ClientMsg(msg) => match msg {
                    msg::ClientMsg::Data(data) => {
                        ok_or!(client_to_target_cmd_tx.send(data.into()).await, return);
                    }
                    msg::ClientMsg::Ack(ack) => {
                        ok_or!(target_to_client_cmd_tx.send(ack.into()).await, return);
                    }
                    msg::ClientMsg::Eof(eof) => {
                        ok_or!(client_to_target_cmd_tx.send(eof.into()).await, return);
                    }
                    msg::ClientMsg::EofAck(eof_ack) => {
                        ok_or!(target_to_client_cmd_tx.send(eof_ack.into()).await, return);
                    }
                    _ => panic!("unexpected client msg: {:?}", msg),
                },
            }
        }
    };

    let streaming = async move { tokio::try_join!(target_to_client, client_to_target).map(|_| ()) };

    let result = tokio::select! {
        r = streaming => r,
        _ = client_msg_handling => Ok(())
    };

    tracing::debug!("session ends");

    result
}

#[cfg(test)]
mod tests {
    // use std::net::{Ipv4Addr, SocketAddr};
    //
    // use bytes::BytesMut;
    // use futures::channel::mpsc;
    //
    // use super::*;
    //
    // struct ServerMessageExpectation {
    //     pub bound_addr: SocketAddr,
    //     pub ack_till: u16,
    //     pub data_sequence: Vec<BytesMut>,
    //     pub io_error: bool,
    // }
    //
    // type Expectation = ServerMessageExpectation;
    //
    // trait ServerMessageExpectationExt {
    //     fn expect(
    //         self,
    //         exp: ServerMessageExpectation,
    //         client_msg_tx: impl Sink<msg::ClientMsg, Error = impl std::fmt::Debug> + Unpin,
    //     ) -> impl Future<Output = ()>;
    // }
    //
    // impl<T> ServerMessageExpectationExt for T
    // where
    //     T: Stream<Item = msg::ServerMsg> + Unpin,
    // {
    //     fn expect(
    //         mut self,
    //         exp: ServerMessageExpectation,
    //         mut client_msg_tx: impl Sink<msg::ClientMsg, Error = impl std::fmt::Debug> + Unpin,
    //     ) -> impl Future<Output = ()> {
    //         let mut next_ack = 0;
    //         let mut next_packate_seq = 0;
    //         let mut error_received = false;
    //
    //         async move {
    //             let msg::ServerMsg::Reply(msg::Reply { bound_addr }) = self.next().await.unwrap()
    //             else {
    //                 panic!("unexpected reply");
    //             };
    //
    //             assert_eq!(bound_addr, exp.bound_addr);
    //
    //             while let Some(msg) = self.next().await {
    //                 match msg {
    //                     msg::ServerMsg::Reply(_) => {
    //                         panic!("unexpected reply");
    //                     }
    //                     msg::ServerMsg::ReplyError(_) => {
    //                         panic!("unexpected reply error");
    //                     }
    //                     msg::ServerMsg::Data(data) => {
    //                         assert_eq!(next_packate_seq, data.seq);
    //                         assert_eq!(exp.data_sequence[next_packate_seq as usize], data.data);
    //                         client_msg_tx
    //                             .send(msg::Ack { seq: data.seq }.into())
    //                             .await
    //                             .unwrap();
    //                         next_packate_seq += 1;
    //                     }
    //                     msg::ServerMsg::Eof(eof) => {
    //                         assert_eq!(next_packate_seq, eof.seq);
    //                         assert_eq!(eof.seq, exp.data_sequence.len() as u16);
    //                         client_msg_tx
    //                             .send(msg::Ack { seq: eof.seq }.into())
    //                             .await
    //                             .unwrap();
    //                         next_packate_seq += 1;
    //                     }
    //                     msg::ServerMsg::Ack(ack) => {
    //                         assert_eq!(next_ack, ack.seq);
    //                         assert!(ack.seq <= exp.ack_till);
    //                         next_ack += 1;
    //                     }
    //                     msg::ServerMsg::TargetIoError(_) => {
    //                         assert_eq!(next_packate_seq, exp.data_sequence.len() as u16);
    //                         assert!(exp.io_error);
    //                         error_received = true;
    //                     }
    //                 }
    //             }
    //
    //             // last data packate = exp.data_sequence.len() - 1
    //             // eof  = exp.data_sequence.len()
    //             // next seq = exp.data_sequence.len() + 1
    //             // skip eof when io_error
    //             assert_eq!(
    //                 next_packate_seq,
    //                 exp.data_sequence.len() as u16 + (if exp.io_error { 0 } else { 1 })
    //             );
    //             assert_eq!(next_ack, exp.ack_till + (if exp.io_error { 0 } else { 1 }));
    //             assert_eq!(exp.io_error, error_received);
    //         }
    //     }
    // }
    //
    // #[test_log::test(tokio::test)]
    // async fn happy_path() {
    //     let (client_msg_tx, client_msg_rx) = mpsc::channel(1);
    //     let (server_msg_tx, server_msg_rx) = mpsc::channel(1);
    //
    //     let main_task = run(
    //         client_msg_rx,
    //         server_msg_tx,
    //         Config {
    //             max_packet_ahead: 1024,
    //             max_packet_size: 1024,
    //             connect_target: crate::connect_target::make_mock([(
    //                 (ReadRequestAddr::Domain("example.com".into()), 8080),
    //                 Ok((
    //                     // target_stream,
    //                     tokio_test::io::Builder::new()
    //                         .read(&[1, 2, 3, 4])
    //                         .write(&[4, 3, 2, 1])
    //                         .build(),
    //                     SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
    //                 )),
    //             )]),
    //         },
    //     );
    //
    //     let verify_server_msg = server_msg_rx.expect(
    //         Expectation {
    //             bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
    //             ack_till: 1,
    //             data_sequence: vec![[1, 2, 3, 4].as_ref().into()],
    //             io_error: false,
    //         },
    //         client_msg_tx.clone(),
    //     );
    //
    //     let client_task = {
    //         let mut client_msg_tx = client_msg_tx.clone();
    //         async move {
    //             client_msg_tx
    //                 .send(msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain("example.com".into()),
    //                     port: 8080,
    //                 }))
    //                 .await
    //                 .unwrap();
    //
    //             client_msg_tx
    //                 .send(
    //                     msg::Data {
    //                         seq: 0,
    //                         data: [4, 3, 2, 1].as_ref().into(),
    //                     }
    //                     .into(),
    //                 )
    //                 .await
    //                 .unwrap();
    //
    //             client_msg_tx
    //                 .send(msg::Eof { seq: 1 }.into())
    //                 .await
    //                 .unwrap();
    //         }
    //     };
    //
    //     tokio::join!(
    //         main_task.map(|r| r.unwrap()),
    //         verify_server_msg,
    //         client_task
    //     );
    //
    //     drop(client_msg_tx);
    // }
    //
    // #[cfg(test)]
    // mod interact_with_target {
    //     use super::*;
    //
    //     #[tokio::test]
    //     async fn reply_target_conn_error() {
    //         let (mut client_msg_tx, client_msg_rx) = mpsc::channel(1);
    //         let (server_msg_tx, mut server_msg_rx) = mpsc::channel(1);
    //         let main_task = run(
    //             client_msg_rx,
    //             server_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1024,
    //                 max_packet_size: 1024,
    //                 connect_target: crate::connect_target::make_mock::<tokio_test::io::Mock>([(
    //                     (ReadRequestAddr::Domain("example.com".into()), 80),
    //                     Err(std::io::Error::new(
    //                         std::io::ErrorKind::NotFound,
    //                         "not found",
    //                     )),
    //                 )]),
    //             },
    //         );
    //
    //         let operate_task = async move {
    //             client_msg_tx
    //                 .send(msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain("example.com".into()),
    //                     port: 80,
    //                 }))
    //                 .await
    //                 .unwrap();
    //
    //             let _ = match server_msg_rx.next().await.unwrap() {
    //                 msg::ServerMsg::ReplyError(e) => e,
    //                 x => panic!("unexpected server msg: {:?}", x),
    //             };
    //
    //             drop(client_msg_tx);
    //
    //             assert!(server_msg_rx.next().await.is_none());
    //         };
    //
    //         tokio::join!(main_task.map(|r| r.unwrap()), operate_task);
    //     }
    //
    //     #[tokio::test]
    //     async fn forawrd_proxyee_io_error_during_streaming() {
    //         let (client_msg_tx, client_msg_rx) = mpsc::channel(1);
    //         let (server_msg_tx, mut server_msg_rx) = mpsc::channel(1);
    //
    //         let main_task = run(
    //             client_msg_rx,
    //             server_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1024,
    //                 max_packet_size: 1024,
    //                 connect_target: crate::connect_target::make_mock([(
    //                     (ReadRequestAddr::Domain("example.com".into()), 80),
    //                     Ok((
    //                         tokio_test::io::Builder::new().build(),
    //                         SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
    //                     )),
    //                 )]),
    //             },
    //         );
    //
    //         let operate_task = {
    //             let mut client_msg_tx = client_msg_tx.clone();
    //             async move {
    //                 client_msg_tx
    //                     .send(msg::ClientMsg::Request(msg::Request {
    //                         addr: ReadRequestAddr::Domain("example.com".into()),
    //                         port: 80,
    //                     }))
    //                     .await
    //                     .unwrap();
    //
    //                 assert_eq!(
    //                     server_msg_rx.next().await.unwrap(),
    //                     msg::ServerMsg::Reply(msg::Reply {
    //                         bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999)
    //                     })
    //                 );
    //
    //                 client_msg_tx.send(msg::IoError.into()).await.unwrap();
    //             }
    //         };
    //
    //         tokio::join!(main_task.map(|r| r.unwrap()), operate_task);
    //         drop(client_msg_tx);
    //     }
    // }
    //
    // mod interact_with_client {
    //     use super::*;
    //
    //     #[tokio::test]
    //     async fn reply_error() {
    //         let (mut client_msg_tx, client_msg_rx) = mpsc::channel(1);
    //         let (server_msg_tx, mut server_msg_rx) = mpsc::channel(1);
    //
    //         let main_task = run(
    //             client_msg_rx,
    //             server_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1024,
    //                 max_packet_size: 1024,
    //                 connect_target: crate::connect_target::make_mock::<tokio_test::io::Mock>([(
    //                     (ReadRequestAddr::Domain("example.com".into()), 80),
    //                     Err(std::io::Error::new(
    //                         std::io::ErrorKind::NotFound,
    //                         "not found",
    //                     )),
    //                 )]),
    //             },
    //         );
    //
    //         let operate_task = async move {
    //             client_msg_tx
    //                 .send(msg::ClientMsg::Request(msg::Request {
    //                     addr: ReadRequestAddr::Domain("example.com".into()),
    //                     port: 80,
    //                 }))
    //                 .await
    //                 .unwrap();
    //
    //             assert_eq!(
    //                 server_msg_rx.next().await.unwrap(),
    //                 msg::ConnectionError::General.into()
    //             );
    //
    //             assert!(server_msg_rx.next().await.is_none());
    //         };
    //
    //         tokio::join!(main_task.map(|r| r.unwrap()), operate_task);
    //     }
    //
    //     #[tokio::test]
    //     async fn forward_target_io_error_during_streaming() {
    //         let (client_msg_tx, client_msg_rx) = mpsc::channel(1);
    //         let (server_msg_tx, server_msg_rx) = mpsc::channel(1);
    //
    //         let main_task = run(
    //             client_msg_rx,
    //             server_msg_tx,
    //             Config {
    //                 max_packet_ahead: 1024,
    //                 max_packet_size: 1024,
    //                 connect_target: crate::connect_target::make_mock([(
    //                     (ReadRequestAddr::Domain("example.com".into()), 80),
    //                     Ok((
    //                         tokio_test::io::Builder::new()
    //                             .read_error(std::io::Error::new(std::io::ErrorKind::Other, "other"))
    //                             .build(),
    //                         SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
    //                     )),
    //                 )]),
    //             },
    //         );
    //
    //         let verify_server_msg = server_msg_rx.expect(
    //             Expectation {
    //                 bound_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9999),
    //                 ack_till: 0,
    //                 data_sequence: vec![],
    //                 io_error: true,
    //             },
    //             client_msg_tx.clone(),
    //         );
    //
    //         let operate_task = {
    //             let mut client_msg_tx = client_msg_tx.clone();
    //             async move {
    //                 client_msg_tx
    //                     .send(msg::ClientMsg::Request(msg::Request {
    //                         addr: ReadRequestAddr::Domain("example.com".into()),
    //                         port: 80,
    //                     }))
    //                     .await
    //                     .unwrap();
    //             }
    //         };
    //
    //         tokio::join!(
    //             main_task.map(|r| r.unwrap_err()),
    //             verify_server_msg,
    //             operate_task
    //         );
    //     }
    // }
}

use std::ops::Add;

use futures::prelude::*;
use rand::prelude::*;
use tracing::*;

use crate::handover;
use crate::prelude::*;

pub async fn run(
    mut client_read: impl protocol::server_agent::GreetedRead,
    mut client_write: impl protocol::server_agent::GreetedWrite,
    mut client_msg_tx: impl Sink<protocol::msg::ClientMsg> + Unpin + Send + 'static,
    mut server_msg_rx: handover::Receiver<protocol::msg::ServerMsg>,
) -> Result<(), std::io::Error> {
    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let ping_waiting_task = async move {
        let mut ping_counter = 0;
        let mut ping_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(5)));
        loop {
            tokio::select! {
                _ = ping_timer.as_mut() => {
                    warn!("ping timeout, exiting");
                    return Err::<(), _>(std::io::Error::new(std::io::ErrorKind::Other, "ping timeout"));
                },
                ping = ping_rx.recv() => {
                    match ping {
                        Some(_) => {
                            debug!(count=ping_counter, "ping");
                            ping_counter += 1;
                            ping_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(5)));
                        }
                        None => {
                            debug!("ping rx is broken, exiting");
                            return Ok::<_, std::io::Error>(());
                        }
                    }
                }
            }
        }
    }.instrument(info_span!("ping waiting task"));

    let reciving_msg_from_client = async move {
        while let Some(msg) = match client_read.recv_msg().await {
            Ok(msg_opt) => msg_opt,
            Err(e) => match e {
                DecodeError::Io(err) => return Err(err),
                DecodeError::InvalidStream(err) => panic!("invalid stream: {:?}", err),
            },
        } {
            debug!("message from client: {:?}", &msg);

            use protocol::msg::conn::ClientMsg::*;

            match msg {
                Protocol(msg) => {
                    if let Err(_) = client_msg_tx.send(msg).await {
                        warn!("failed to forward client message, exiting");
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "failed to forward client message",
                        ));
                    }
                }
                Ping => {
                    if let Err(_) = ping_tx.send(()) {
                        warn!("ping tx is broken, exiting");
                        return Ok(());
                    }
                }
            }
        }

        Ok::<_, std::io::Error>(())
    };

    let sending_msg_to_client = async move {
        let duration = std::time::Duration::from_secs(10).add(std::time::Duration::from_mins(
            rand::rng().random_range(..=10u64),
        ));
        let mut time_limit_task = Box::pin(tokio::time::sleep(duration));
        let mut pong_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(0)));
        let mut pong_counter = 0;

        macro_rules! send_msg {
            ($msg:expr) => {
                tokio::time::timeout(
                    std::time::Duration::from_secs(4),
                    client_write.send_msg($msg),
                )
                .map(|timeout_result| match timeout_result {
                    Ok(x) => x,
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout sending msg to client",
                    )),
                })
            };
        }

        loop {
            tokio::select! {
                _ = time_limit_task.as_mut() => {
                    client_write
                        .send_msg(protocol::msg::conn::ServerMsg::EndOfStream)
                        .await?;
                    // let _ = client_write.close().await;

                    return Ok::<_, std::io::Error>(());
                },
                server_msg_opt = server_msg_rx.recv() => {
                    if let Some(server_msg) = server_msg_opt {
                        let span = debug_span!("message to client", ?server_msg);
                        send_msg!(server_msg.into())
                            .instrument(span)
                            .await?;
                    } else {
                        return Ok::<_, std::io::Error>(());
                    }

                },
                _ = pong_timer.as_mut() => {
                    debug!(count = pong_counter, "pong");

                    send_msg!(protocol::msg::conn::ServerMsg::Pong)
                        .instrument(debug_span!("send pong", count = pong_counter))
                        .await?;

                    pong_counter += 1;
                    pong_timer = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(1)));
                }
            }
        }
    };

    let recv_loop = async move {
        tokio::try_join! {
            reciving_msg_from_client,
            ping_waiting_task,
        }
        .map(|_| ())
    };

    match futures::future::select(Box::pin(sending_msg_to_client), Box::pin(recv_loop)).await {
        future::Either::Left((send_result, _)) => send_result,

        // do not cancel send_loop
        future::Either::Right((recv_result, send_loop)) => {
            let send_result = send_loop.await;
            match recv_result {
                Ok(_) => send_result,
                Err(send_err) => Err(send_err),
            }
        }
    }
}

use std::ops::Add;

use futures::channel::mpsc;
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
    //TODO: error handling
    debug!("client connection lifetime task started");

    let duration = std::time::Duration::from_secs(10).add(std::time::Duration::from_mins(
        rand::rng().random_range(..=10u64),
    ));
    let mut time_limit_task = Box::pin(tokio::time::sleep(duration));

    let reciving_msg_from_client = async move {
        while let Some(msg) = match client_read.recv_msg().await {
            Ok(msg_opt) => msg_opt,
            Err(e) => match e {
                DecodeError::Io(err) => return Err(err),
                DecodeError::InvalidStream(err) => panic!("invalid stream: {:?}", err),
            },
        } {
            debug!("message from client: {:?}", &msg);
            if let Err(_) = client_msg_tx.send(msg).await {
                warn!("failed to forward client message, exiting");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "failed to forward client message",
                ));
            }
        }

        Ok::<_, std::io::Error>(())
    };

    let sending_msg_to_client = async move {
        loop {
            match futures::future::select(time_limit_task, server_msg_rx.recv().boxed()).await {
                futures::future::Either::Left(x) => {
                    debug!("hit connection lifetime limit");
                    drop(x);
                    client_write
                        .send_msg(protocol::msg::ServerMsg::EndOfStream)
                        .await?;
                    // let _ = client_write.close().await;
                    drop(client_write);

                    return Ok::<_, std::io::Error>(());
                }
                futures::future::Either::Right((server_msg_opt, not_yet)) => {
                    if let Some(server_msg) = server_msg_opt {
                        time_limit_task = not_yet;
                        debug!("message to client: {:?}", &server_msg);
                        client_write.send_msg(server_msg).await?;
                    } else {
                        return Ok::<_, std::io::Error>(());
                    }
                }
            }
        }
    };

    tokio::try_join! {
        sending_msg_to_client,
        reciving_msg_from_client,
    }
    .map(|_| ())
}

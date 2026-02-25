use std::fmt::Debug;

use futures::{channel::mpsc, prelude::*};

use crate::prelude::*;
use protocol::msg::AtLeastOnce;

pub enum Event<Msg>
where
    Msg: Clone + PartialEq + Eq,
{
    Send(AtLeastOnce<Msg>),
}

pub struct Handle<Msg>
where
    Msg: Clone + PartialEq + Eq + Send,
{
    queue_tx: mpsc::UnboundedSender<Msg>,
    ack_tx: mpsc::UnboundedSender<u32>,
}

impl<Msg> Handle<Msg>
where
    Msg: Clone + PartialEq + Eq + Send,
{
    pub async fn queue(&mut self, msg: Msg) {
        let _ = self.queue_tx.send(msg).await;
    }

    pub async fn ack(&mut self, ack: u32) {
        let _ = self.ack_tx.send(ack).await;
    }
}

pub fn run<Msg>(
    mut evt_tx: impl Sink<Event<Msg>> + Unpin + Send + Clone + 'static,
) -> (
    impl Future<Output = std::io::Result<()>> + Send,
    Handle<Msg>,
)
where
    Msg: Clone + PartialEq + Eq + Send + Debug,
{
    let (queue_tx, mut queue_rx) = mpsc::unbounded::<Msg>();
    let (ack_tx, mut ack_rx) = mpsc::unbounded::<u32>();
    let mut seq = 0;

    let fut = async move {
        loop {
            let msg = match queue_rx.next().await {
                Some(x) => x,
                None => {
                    tracing::warn!("end of queue_rx, exiting");
                    return Ok(());
                }
            };

            if let Err(_) = evt_tx
                .send(Event::Send(AtLeastOnce::Msg(seq, msg.clone())))
                .await
            {
                tracing::warn!("evt_tx is broken, exiting");
                return Ok(());
            }

            let mut timeout = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(3)));
            let mut retry_count = 0;

            loop {
                tokio::select! {
                    _ = &mut timeout => {
                        if retry_count >= 8 {
                            tracing::error!(?seq, ?msg, "timeout waiting for command's ack");
                            return Err(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "timeout waiting for command's ack",
                            ));
                        }
                        else {
                            if let Err(_) = evt_tx.send(Event::Send(AtLeastOnce::Msg(seq, msg.clone()))).await {
                                tracing::warn!("evt_tx is broken, exiting");
                                return Ok(());
                            }
                            retry_count +=1;
                            timeout = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(3)));
                            continue;
                        }
                    },

                    ack = ack_rx.next() => {
                        let ack = match ack {
                            Some(x) => x,
                            None => {
                                tracing::warn!("end of ack_rx, exiting");
                                return Ok(());
                            },
                        };

                        if ack == seq  {
                            seq += 1;
                            break;
                        }
                        else {
                            continue;
                        }
                    }
                }
            }
        }
    };

    (fut, Handle { queue_tx, ack_tx })
}

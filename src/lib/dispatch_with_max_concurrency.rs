use std::fmt::Debug;
use std::sync::Arc;

use futures::prelude::*;
use tokio::sync::Semaphore;
use tracing::Instrument as _;

use crate::prelude::*;

pub enum Error {
    MessageLost,
}

// Err(()) on message lost
pub async fn run<ConnId, Msg>(
    mut message_queue: impl Stream<Item = Msg> + Unpin + Send + 'static,
    sender_rx: async_channel::Receiver<(ConnId, crate::oneshot_with_ack::RawSender<Msg>)>,
) -> Result<(), Error>
where
    Msg: Send + Unpin + Debug + 'static,
    ConnId: Debug + Send + 'static,
{
    const MAX_CONCURRENCY: u32 = 2;

    let (mut scope_handle, scope_task) = task_scope::new_scope::<Error>();

    let main_loop = {
        async move {
            let limiter = Arc::new(Semaphore::new(MAX_CONCURRENCY as usize));

            while let Some(msg) = message_queue.next().await {
                let permit = limiter.clone().acquire_owned().await.unwrap();

                let forward_message_span = tracing::trace_span!("forward message", ?msg);

                let sender_rx = sender_rx.clone();
                let send_task = async move {
                    let mut msg = msg;

                    loop {
                        let (conn_id, sender) = match sender_rx.recv().await {
                            Ok(x) => x,
                            Err(_) => {
                                tracing::warn!("sender_queue_rx is broken, exiting");
                                return Ok(());
                            }
                        };

                        let forward_to_conn_span =
                            tracing::trace_span!("forward message to conn", ?conn_id);

                        match sender.send(msg).instrument(forward_to_conn_span).await {
                            Ok(()) => {
                                drop(permit);
                                return Ok(());
                            }
                            Err(crate::oneshot_with_ack::SendError::Rejected(msg_rejected)) => {
                                tracing::debug!("message rejected, try again");
                                msg = msg_rejected;
                                continue;
                            }
                            Err(crate::oneshot_with_ack::SendError::Lost) => {
                                tracing::error!("message lost");
                                return Err(Error::MessageLost);
                            }
                        }
                    }
                }
                .instrument(forward_message_span);

                scope_handle.spawn(send_task).await;
            }

            // wait for all task done
            let _ = limiter.acquire_many(MAX_CONCURRENCY).await.unwrap();
        }
    }
    .instrument(tracing::info_span!("main_loop"));

    match futures::future::select(Box::pin(main_loop), Box::pin(scope_task)).await {
        // main_loop waits for all send task done
        futures::future::Either::Left((_, _)) => Ok(()),
        futures::future::Either::Right((scope_task_result, main_loop)) => {
            scope_task_result?;
            main_loop.await;
            Ok(())
        }
    }
}

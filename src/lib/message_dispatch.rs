use std::collections::HashMap;
use std::fmt::Debug;

use futures::channel::mpsc;
use futures::prelude::*;
use tokio::sync::oneshot;
use tracing::*;

use crate::prelude::*;
use crate::protocol_conn_lifetime::SendEntry;
use protocol::SessionId;

pub enum Command<ConnId, Msg>
where
    Msg: Send + 'static,
{
    Msg(SessionId, Msg),
    Sender(ConnId, oneshot::Sender<SendEntry<Msg>>),
    NewSession(SessionId),
    EndSession(SessionId),
}

// send given messages to senders
// senders are reused until it returns Err
pub fn run<ConnId, Msg>(
    mut cmd_rx: futures::channel::mpsc::UnboundedReceiver<Command<ConnId, Msg>>,
) -> impl Future<Output = ()>
where
    ConnId: Debug + Send + 'static,
    Msg: 'static + Send + Debug,
{
    let (sender_queue_tx, sender_queue_rx) =
        async_channel::unbounded::<(ConnId, oneshot::Sender<SendEntry<Msg>>)>();

    let (mut sessions_scope_handle, sessions_scope_task) = task_scope::new_scope::<()>();

    let cmd_loop = async move {
        let mut sessions = HashMap::<SessionId, mpsc::UnboundedSender<Msg>>::new();

        while let Some(cmd) = cmd_rx.next().await {
            match cmd {
                Command::Msg(session_id, msg) => {
                    if let Some(session_msg_queue_tx) = sessions.get_mut(&session_id) {
                        if let Err(_) = session_msg_queue_tx.send(msg).await {
                            warn!(?session_id, "message queue for session is broken");
                            let _ = sessions.remove(&session_id);
                            continue;
                        }
                    }
                }
                Command::Sender(conn_id, entry) => {
                    if let Err(_) = sender_queue_tx.send((conn_id, entry)).await {
                        warn!("sender_queue_tx is broken, exiting");
                        return;
                    }
                }
                Command::NewSession(session_id) => {
                    let (session_msg_queue_tx, session_msg_queue_rx) = mpsc::unbounded::<Msg>();
                    sessions.insert(session_id, session_msg_queue_tx);
                    sessions_scope_handle
                        .run_async(
                            message_group::run(session_msg_queue_rx, sender_queue_rx.clone())
                                .instrument(tracing::trace_span!(
                                    "session message sending scope",
                                    ?session_id
                                )),
                        )
                        .await;
                }
                Command::EndSession(session_id) => {
                    let _ = sessions.remove(&session_id);
                }
            };
        }
    };

    async move {
        tokio::select! {
            _ = sessions_scope_task => {
                warn!("error in session message sending scope, exiting");
                ()
            },
            _ = cmd_loop.instrument(info_span!("cmd loop")) => (),
        }
    }
}

mod message_group {
    use std::sync::Arc;

    use super::*;

    use tokio::sync::Semaphore;

    enum ForwardError<Msg> {
        Rejected(Msg),
        Lost,
    }

    pub async fn run<ConnId, Msg>(
        mut message_queue: impl Stream<Item = Msg> + Unpin + Send + 'static,
        sender_rx: async_channel::Receiver<(ConnId, oneshot::Sender<SendEntry<Msg>>)>,
    ) -> Result<(), ()>
    where
        Msg: Send + Debug + 'static,
        ConnId: Debug + Send + 'static,
    {
        const MAX_CONCURRENCY: u32 = 2;

        let (mut scope_handle, scope_task) = task_scope::new_scope::<()>();

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
                                    warn!("sender_queue_rx is broken, exiting");
                                    return Err(());
                                }
                            };

                            let forward_to_conn_span =
                                tracing::trace_span!("forward message to conn", ?conn_id);

                            match async move {
                                let (ack_tx, ack_rx) = oneshot::channel::<()>();

                                if let Err((msg_rejected, _)) = sender.send((msg, ack_tx)) {
                                    tracing::trace!(
                                        "server_write is closed, retry sending message"
                                    );
                                    return Err(ForwardError::Rejected(msg_rejected));
                                }

                                ack_rx.await.map_err(|_| ForwardError::Lost)
                            }
                            .instrument(forward_to_conn_span)
                            .await
                            {
                                Ok(()) => {
                                    drop(permit);
                                    return Ok(());
                                }
                                Err(ForwardError::Rejected(msg_rejected)) => {
                                    msg = msg_rejected;
                                    continue;
                                }
                                Err(ForwardError::Lost) => {
                                    error!("message lost");
                                    return Err(());
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
        .instrument(info_span!("main_loop"));

        match futures::future::select(Box::pin(main_loop), Box::pin(scope_task)).await {
            // main_loop waits for all send task done
            futures::future::Either::Left((_, _)) => Ok(()),
            futures::future::Either::Right((scope_task_err, _)) => Err(scope_task_err),
        }
    }
}

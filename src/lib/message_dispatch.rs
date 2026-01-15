use std::fmt::Debug;

use futures::prelude::*;
use tokio::sync::oneshot;
use tracing::*;

use crate::prelude::*;
use crate::protocol_conn_lifetime::SendEntry;

pub enum Command<ConnId, Msg>
where
    Msg: Send + 'static,
{
    Msg(Msg),
    Sender(ConnId, oneshot::Sender<SendEntry<Msg>>),
}

// send given messages to senders
// senders are reused until it returns Err
pub fn run<ConnId, Msg>() -> (
    futures::channel::mpsc::UnboundedSender<Command<ConnId, Msg>>,
    impl Future<Output = ()>,
)
where
    ConnId: Debug + Send + 'static,
    Msg: 'static + Send + Debug,
{
    let (cmd_tx, mut cmd_rx) = futures::channel::mpsc::unbounded::<Command<ConnId, Msg>>();

    let (sender_queue_tx, sender_queue_rx) =
        async_channel::unbounded::<(ConnId, oneshot::Sender<SendEntry<Msg>>)>();

    // error on borken channel
    let (mut sending_scope_handle, sending_scope_task) = task_scope::new_scope::<()>();

    let send = move |msg: Msg| {
        let sender_queue_rx = sender_queue_rx.clone();
        let outer_span = tracing::trace_span!("dispatch message", msg = ?msg);

        async move {
            let mut msg = msg;

            loop {
                let (conn_id, sender) = match sender_queue_rx.recv().await {
                    Ok(x) => x,
                    Err(_) => {
                        warn!("sender_queue_rx is broken, exiting");
                        return Err(());
                    }
                };

                let span = tracing::trace_span!("forward message to conn", ?conn_id);

                match async move {
                    let (ack_tx, ack_rx) = oneshot::channel::<()>();

                    if let Err((msg_rejected, _)) = sender.send((msg, ack_tx)) {
                        tracing::trace!("server_write is closed, retry sending message");
                        return Err(msg_rejected);
                    }

                    match ack_rx.await {
                        Ok(()) => Ok(()),
                        Err(_) => {
                            error!("message lost");
                            panic!("message lost")
                        }
                    }
                }
                .instrument(span)
                .await
                {
                    Ok(()) => {
                        return Ok(());
                    }
                    Err(msg_rejected) => {
                        msg = msg_rejected;
                        continue;
                    }
                }
            }
        }
        .instrument(outer_span)
    };

    let cmd_loop = async move {
        while let Some(cmd) = cmd_rx.next().await {
            match cmd {
                Command::Msg(msg) => {
                    sending_scope_handle.run_async(send(msg)).await;
                }
                Command::Sender(conn_id, entry) => {
                    if let Err(_) = sender_queue_tx.send((conn_id, entry)).await {
                        warn!("sender_queue_tx is broken, exiting");
                        return;
                    }
                }
            };
        }
    };

    (cmd_tx, async move {
        tokio::join! {
            sending_scope_task.map(|_| ()).instrument(info_span!("sending scope")),
            cmd_loop.instrument(info_span!("cmd loop")),
        };
        ()
    })
}

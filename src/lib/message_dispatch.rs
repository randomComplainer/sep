use std::fmt::Debug;

use futures::prelude::*;
use tracing::*;

use crate::handover;
use crate::prelude::*;

pub enum Command<ConnId, Msg>
where
    Msg: Send + 'static,
{
    Msg(Msg),
    Sender(ConnId, handover::Sender<Msg>),
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
        async_channel::unbounded::<(ConnId, handover::Sender<Msg>)>();

    // error on borken channel
    let (mut sending_scope_handle, sending_scope_task) = task_scope::new_scope::<()>();

    let send = {
        let sender_queue_tx = sender_queue_tx.clone();

        move |msg: Msg| {
            let sender_queue_tx = sender_queue_tx.clone();
            let sender_queue_rx = sender_queue_rx.clone();
            let outer_span = info_span!("forward message outer", msg = ?msg);

            async move {
                let mut msg = msg;

                loop {
                    let (conn_id, mut sender) = match sender_queue_rx.recv().await {
                        Ok(x) => x,
                        Err(_) => {
                            warn!("sender_queue_rx is broken, exiting");
                            return Err(());
                        }
                    };

                    let span = info_span!("forward message to connection", ?msg, ?conn_id);

                    match sender.send(msg).instrument(span).await {
                        Ok(()) => {
                            if let Err(_) = sender_queue_tx.send((conn_id, sender)).await {
                                warn!("sender_queue_tx is broken, exiting");
                                return Err(());
                            }
                            return Ok(());
                        }
                        Err(msg_opt) => {
                            debug!("server_write is closed");
                            msg = msg_opt.expect("message consumed unexpectedly");
                            debug!("retry sending message");
                            continue;
                        }
                    };
                }
            }
            .instrument(outer_span)
        }
    };

    let cmd_loop = async move {
        while let Some(cmd) = cmd_rx.next().await {
            match cmd {
                Command::Msg(msg) => {
                    sending_scope_handle.run_async(send(msg)).await;
                }
                Command::Sender(conn_id, sender) => {
                    if let Err(_) = sender_queue_tx.send((conn_id, sender)).await {
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

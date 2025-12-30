use futures::prelude::*;
use tracing::*;

use crate::handover;
use crate::prelude::*;

type ConnId = Box<str>;

pub enum Command {
    ClientMsg(protocol::msg::ClientMsg),
    Sender(ConnId, handover::Sender<protocol::msg::ClientMsg>),
}

pub fn run() -> (
    futures::channel::mpsc::UnboundedSender<Command>,
    impl Future<Output = ()>,
) {
    let (cmd_tx, mut cmd_rx) = futures::channel::mpsc::unbounded::<Command>();

    let (sender_queue_tx, sender_queue_rx) =
        async_channel::unbounded::<(ConnId, handover::Sender<protocol::msg::ClientMsg>)>();

    // error on borken channel
    let (mut sending_scope_handle, sending_scope_task) = task_scope::new_scope::<()>();

    let send = {
        let sender_queue_tx = sender_queue_tx.clone();

        move |msg: protocol::msg::ClientMsg| {
            let sender_queue_tx = sender_queue_tx.clone();
            let sender_queue_rx = sender_queue_rx.clone();
            let scope = info_span!("forward client msg to connection", msg = ?msg);

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

                    match sender.send(msg).await {
                        Ok(()) => {
                            if let Err(_) = sender_queue_tx.send((conn_id, sender)).await {
                                warn!("sender_queue_tx is broken, exiting");
                                return Err(());
                            }
                            return Ok(());
                        }
                        Err(msg_opt) => {
                            debug!("server_write is closed");
                            msg = msg_opt.expect("client msg consumed unexpectedly");
                            debug!("retry sending client message");
                            continue;
                        }
                    };
                }
            }
            .instrument(scope)
        }
    };

    let cmd_loop = async move {
        while let Some(cmd) = cmd_rx.next().await {
            match cmd {
                Command::ClientMsg(client_msg) => {
                    sending_scope_handle.run_async(send(client_msg)).await;
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

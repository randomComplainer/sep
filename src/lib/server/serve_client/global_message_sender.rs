use futures::channel::mpsc;
use futures::prelude::*;

use crate::{
    prelude::*,
    protocol_conn_lifetime::{NextSender, WriteHandle},
};

#[derive(Clone)]
pub struct Handle {
    global_kill_session_tx: mpsc::UnboundedSender<SessionId>,
    global_msg_sender_tx: mpsc::UnboundedSender<(ConnId, WriteHandle<protocol::msg::ServerMsg>)>,
}

impl Handle {
    pub async fn kill_session(&mut self, session_id: SessionId) {
        if let Err(_) = self.global_kill_session_tx.send(session_id).await {
            tracing::warn!("global_kill_session_tx is broken");
        }
    }

    pub async fn new_conn(
        &mut self,
        conn_id: ConnId,
        write_handle: WriteHandle<protocol::msg::ServerMsg>,
    ) {
        if let Err(_) = self
            .global_msg_sender_tx
            .send((conn_id, write_handle))
            .await
        {
            tracing::warn!("global_msg_sender_tx is broken");
        }
    }
}

pub fn run() -> (Handle, impl Future<Output = Result<(), Never>>) {
    let (global_kill_session_tx, mut global_kill_session_rx) = mpsc::unbounded();
    let (global_msg_sender_tx, mut global_msg_sender_rx) = mpsc::unbounded();

    let fut = async move {
        // TODO: this is not guaranteed delivery
        // you need ids and acks to ensure delivery

        use NextSender::*;

        let mut handles: Vec<(ConnId, WriteHandle<protocol::msg::ServerMsg>)> = Vec::new();

        let mut head_session_id = None::<SessionId>;
        loop {
            let session_id = match head_session_id.take() {
                Some(x) => x,
                None => match global_kill_session_rx.next().await {
                    Some(x) => x,
                    None => {
                        tracing::error!("global_kill_session_rx is broken, exiting");
                        return Ok(());
                    }
                },
            };

            head_session_id = Some(session_id);

            let msg = protocol::msg::ServerMsg::KillSession(session_id);

            tokio::select! {
                next_sender_result = WriteHandle::next_sender(&handles)  => {
                    let (conn_id, sender ) = match next_sender_result {
                        Found(conn_id, sender) => (conn_id, sender),
                        Closed(conn_id) => {
                            let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                            handles.swap_remove(idx);
                            continue;
                        },
                        IoError(conn_id) => {
                            let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                            handles.swap_remove(idx);
                            continue;
                        },
                    };

                    match sender.send(msg) {
                        Ok(_) => {
                            head_session_id = None;
                            continue;
                        },
                        Err(_) => {
                            // Closed
                            let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                            handles.swap_remove(idx);
                            continue;
                        }
                    }
                },

                conn = global_msg_sender_rx.next() => {
                    let (conn_id, write_handle) = match conn {
                        Some(conn) => conn,
                        None => {
                            tracing::warn!("end of conn_writer_rx, exiting");
                            return Ok(());
                        }
                    };

                    handles.push((conn_id, write_handle));
                    continue;
                }
            };
        }
    };

    (
        Handle {
            global_kill_session_tx,
            global_msg_sender_tx,
        },
        fut,
    )
}

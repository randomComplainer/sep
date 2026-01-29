use std::fmt::Debug;

use futures::channel::mpsc;
use futures::prelude::*;

use crate::{
    prelude::*,
    protocol_conn_lifetime::{NextSender, WriteHandle},
};

pub fn run<MsgToSend, ConnHandleMsg>() -> (
    impl Future<Output = Result<(), ()>> + Send,
    Handle<MsgToSend, ConnHandleMsg>,
)
where
    MsgToSend: Clone + Eq + Send + Sync + Debug + Unpin + 'static,
    ConnHandleMsg: From<protocol::msg::AtLeastOnce<MsgToSend>> + Send + Debug + Unpin + 'static,
{
    let (msg_to_send_tx, msg_to_send_rx) = mpsc::unbounded();
    let (conn_writer_tx, conn_writer_rx) = mpsc::unbounded();
    let (incoming_ack_tx, incoming_ack_rx) = mpsc::unbounded();
    let (outgoing_ack_tx, outgoing_ack_rx) = mpsc::unbounded();

    let handle = Handle {
        msg_to_send_tx,
        conn_writer_tx,
        incoming_ack_tx,
        outgoing_ack_tx,
    };

    let fut = async move {
        send_loop(
            msg_to_send_rx,
            conn_writer_rx,
            incoming_ack_rx,
            outgoing_ack_rx,
        )
        .await
    };

    (fut, handle)
}

#[derive(Clone)]
pub struct Handle<MsgToSend, ConnHandleMsg> {
    msg_to_send_tx: mpsc::UnboundedSender<MsgToSend>,
    conn_writer_tx: mpsc::UnboundedSender<(ConnId, WriteHandle<ConnHandleMsg>)>,
    incoming_ack_tx: mpsc::UnboundedSender<u32>,
    outgoing_ack_tx: mpsc::UnboundedSender<u32>,
}

impl<MsgToSend, ConnHandleMsg> Handle<MsgToSend, ConnHandleMsg>
where
    MsgToSend: Clone + Eq + Send + Sync + Debug + Unpin + 'static,
    ConnHandleMsg: From<protocol::msg::AtLeastOnce<MsgToSend>> + Send + Debug + Unpin + 'static,
{
    pub async fn new_conn(&mut self, conn_id: ConnId, write_handle: WriteHandle<ConnHandleMsg>) {
        if let Err(_) = self.conn_writer_tx.send((conn_id, write_handle)).await {
            tracing::warn!("conn_writer_tx is broken");
        }
    }

    pub async fn send_msg(&mut self, msg: MsgToSend) {
        if let Err(_) = self.msg_to_send_tx.send(msg).await {
            tracing::warn!("msg_queue_tx is broken");
        }
    }

    pub async fn recv_ack(&mut self, ack: u32) {
        if let Err(_) = self.incoming_ack_tx.send(ack).await {
            tracing::warn!("ack_tx is broken");
        }
    }

    pub async fn send_ack(&mut self, ack: u32) {
        if let Err(_) = self.outgoing_ack_tx.send(ack).await {
            tracing::warn!("ack_tx is broken");
        }
    }
}

// Err on retry exceeded
async fn send_loop<MsgToSend, ConnHandleMsg>(
    mut msg_queue_rx: impl Stream<Item = MsgToSend> + Send + Unpin,
    mut conn_writer_rx: impl Stream<Item = (ConnId, WriteHandle<ConnHandleMsg>)> + Send + Unpin,
    mut incoming_ack_rx: mpsc::UnboundedReceiver<u32>,
    mut outgoing_ack_rx: mpsc::UnboundedReceiver<u32>,
) -> Result<(), ()>
where
    MsgToSend: Clone + Eq + Send + Sync + Debug + Unpin + 'static,
    ConnHandleMsg: From<protocol::msg::AtLeastOnce<MsgToSend>> + Send + Debug + Unpin + 'static,
{
    let mut handles: Vec<(ConnId, WriteHandle<ConnHandleMsg>)> = Vec::new();
    let mut seq = 0u32;

    loop {
        tokio::select! {
            msg = msg_queue_rx.next() => {
                let msg = match msg {
                    Some(msg) => msg,
                    None => {
                        tracing::warn!("msg_queue_rx is broken, exiting");
                        return Ok(());
                    }
                };

                let mut current_retries = 0u8;
                loop {
                    match try_send_and_wait_for_ack_once(
                        seq,
                        &msg,
                        &mut handles,
                        &mut conn_writer_rx,
                        &mut incoming_ack_rx,
                    )
                    .await
                    {
                        WaitForAck::Acked => break,
                        WaitForAck::Timeout => {
                            tracing::warn!("timeout sending msg");
                            if current_retries >= 5 {
                                tracing::error!("retries exceeded, exiting");
                                return Err(());
                            }
                            current_retries += 1;
                            continue;
                        }
                        WaitForAck::ChannelClosed => {
                            return Ok(());
                        }
                    };
                }

                seq += 1;
            },

            ack = outgoing_ack_rx.next() => {
                let ack = match ack {
                    Some(ack) => ack,
                    None => {
                        tracing::warn!("incoming_ack_rx is broken, exiting");
                        return Ok(());
                    }
                };

                match send_ack(ack, &mut handles, &mut conn_writer_rx).await {
                    Ok(_) => {
                        continue;
                    },
                    Err(_) => {
                        return Ok(());
                    },
                };
            }
        }
    }
}

//  Err on closed channel
async fn send_ack<MsgToSend, ConnHandleMsg>(
    ack: u32,
    handles: &mut Vec<(ConnId, WriteHandle<ConnHandleMsg>)>,
    conn_writer_rx: &mut (impl Stream<Item = (ConnId, WriteHandle<ConnHandleMsg>)> + Send + Unpin),
) -> Result<(), ()>
where
    MsgToSend: Clone + Eq + Send + Debug + Unpin + 'static,
    ConnHandleMsg: From<protocol::msg::AtLeastOnce<MsgToSend>> + Send + Debug + Unpin + 'static,
{
    loop {
        let (conn_id, sender) = match get_sender(handles, conn_writer_rx).await {
            GetSender::Found(conn_id, sender) => (conn_id, sender),
            GetSender::ChannelClosed => return Err(()),
        };

        // TODO: duplicated code
        if let Err(_) = sender.send(protocol::msg::AtLeastOnce::Ack(ack).into()) {
            // Closed
            let idx = handles
                .iter()
                .position(|(item_id, _)| conn_id.eq(item_id))
                .unwrap();
            handles.swap_remove(idx);
            continue;
        } else {
            return Ok(());
        }
    }
}

async fn try_send_and_wait_for_ack_once<MsgToSend, ConnHandleMsg>(
    seq: u32,
    msg: &MsgToSend,
    mut handles: &mut Vec<(ConnId, WriteHandle<ConnHandleMsg>)>,
    mut write_handle_rx: &mut (impl Stream<Item = (ConnId, WriteHandle<ConnHandleMsg>)> + Send + Unpin),
    mut ack_rx: &mut mpsc::UnboundedReceiver<u32>,
) -> WaitForAck
where
    MsgToSend: Clone + Eq + Send + Debug + Unpin + 'static,
    ConnHandleMsg: From<protocol::msg::AtLeastOnce<MsgToSend>> + Send + Debug + Unpin + 'static,
{
    loop {
        let (conn_id, sender) = tokio::select! {
            get_sender_result = get_sender(&mut handles, &mut write_handle_rx) => {
                match get_sender_result {
                    GetSender::Found(conn_id, sender) => (conn_id, sender),
                    GetSender::ChannelClosed => return WaitForAck::ChannelClosed,
                }
            },
            ack_result = recv_ack(seq, ack_rx) => {
                match ack_result {
                    Ok(()) => return WaitForAck::Acked,
                    Err(()) => return WaitForAck::ChannelClosed,
                }
            }
        };

        if let Err(_) = sender.send(protocol::msg::AtLeastOnce::Msg(seq, msg.clone()).into()) {
            // Closed
            let idx = handles
                .iter()
                .position(|(item_id, _)| conn_id.eq(item_id))
                .unwrap();
            handles.swap_remove(idx);
            continue;
        }

        break;
    }

    wait_for_ack(&mut ack_rx, seq, std::time::Duration::from_secs(5)).await
}

// Err on closed channel
async fn recv_ack(seq: u32, mut ack_rx: &mut mpsc::UnboundedReceiver<u32>) -> Result<(), ()> {
    loop {
        let ack = match ack_rx.next().await {
            Some(ack) => ack,
            None => {
                tracing::warn!("ack_rx is broken");
                return Err(());
            }
        };

        if ack == seq {
            return Ok(());
        } else {
            continue;
        }
    }
}

enum WaitForAck {
    Acked,
    Timeout,
    ChannelClosed,
}

async fn wait_for_ack(
    ack_rx: &mut mpsc::UnboundedReceiver<u32>,
    seq: u32,
    timeout: std::time::Duration,
) -> WaitForAck {
    let mut timeout = Box::pin(tokio::time::sleep(timeout));
    loop {
        tokio::select! {
            _ = timeout.as_mut() => {
                tracing::warn!("act timeout");
                return WaitForAck::Timeout;
            },
            ack_result = recv_ack(seq, ack_rx) => {
                match ack_result {
                    Ok(()) => return WaitForAck::Acked,
                    Err(()) => return WaitForAck::ChannelClosed,
                }
            }
        };
    }
}

enum GetSender<ConnHandleMsg> {
    Found(ConnId, tokio::sync::oneshot::Sender<ConnHandleMsg>),
    ChannelClosed,
}

async fn get_sender<ConnHandleMsg>(
    handles: &mut Vec<(ConnId, WriteHandle<ConnHandleMsg>)>,
    conn_writer_rx: &mut (impl Stream<Item = (ConnId, WriteHandle<ConnHandleMsg>)> + Send + Unpin),
) -> GetSender<ConnHandleMsg>
where
    ConnHandleMsg: Send + Debug + Unpin + 'static,
{
    loop {
        tokio::select! {
            next_sender_result = WriteHandle::next_sender(handles) => {
                match next_sender_result {
                    NextSender::Found(conn_id, sender) => { return GetSender::Found(conn_id, sender); },
                    NextSender::Closed(conn_id) => {
                        let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                        handles.swap_remove(idx);
                        continue;
                    },
                    NextSender::IoError(conn_id) => {
                        let idx = handles.iter().position(|(item_id, _)| conn_id.eq(item_id)).unwrap();
                        handles.swap_remove(idx);
                        continue;
                    },
                }
            },

            conn = conn_writer_rx.next() => {
                let (conn_id, write_handle) = match conn {
                    Some(conn) => conn,
                    None => {
                        tracing::warn!("end of conn_writer_rx, exiting");
                        return GetSender::ChannelClosed;
                    }
                };

                handles.push((conn_id, write_handle));
                continue;
            }
        }
    }
}

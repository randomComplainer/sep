use std::collections::HashMap;
use std::ops::Add as _;

use futures::channel::mpsc;
use futures::prelude::*;
use rand::Rng as _;
use tracing::*;

use crate::async_channel_ext::SenderExt as _;
use crate::prelude::*;
use crate::protocol::ConnId;

pub enum Event {
    Closed(ConnId),
    ClientMsg(protocol::msg::ClientMsg),
}

struct ConnEntry {}

pub struct State {
    conns: HashMap<ConnId, ConnEntry>,
    conns_scope_handle: task_scope::ScopeHandle<Never>,
    evt_tx: mpsc::UnboundedSender<Event>,
    server_msg_sender_tx: async_channel::Sender<(
        ConnId,
        oneshot_with_ack::RawSender<protocol::msg::ServerMsg>,
    )>,
}

impl State {
    pub fn new(
        evt_tx: mpsc::UnboundedSender<Event>,
        server_msg_sender_tx: async_channel::Sender<(
            ConnId,
            oneshot_with_ack::RawSender<protocol::msg::ServerMsg>,
        )>,
    ) -> (
        Self,
        impl Future<Output = Result<(), Never>> + Send + 'static,
    ) {
        let (conns_scope_handle, conns_scope_task) = task_scope::new_scope();

        (
            Self {
                conns: HashMap::new(),
                conns_scope_handle,
                evt_tx,
                server_msg_sender_tx,
            },
            conns_scope_task,
        )
    }

    pub async fn on_new_connection<ClientRead, ClientWrite>(
        &mut self,
        conn_id: ConnId,
        client_read: ClientRead,
        client_write: ClientWrite,
    ) where
        ClientRead: protocol::MessageReader<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>,
            >,
        ClientWrite: protocol::MessageWriter<
                Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>,
            >,
    {
        let (lifetime_task, conn_close_tx) = crate::protocol_conn_lifetime::run(
            Default::default(),
            client_read,
            client_write,
            self.evt_tx
                .clone()
                .with_sync(|client_msg| Event::ClientMsg(client_msg)),
            self.server_msg_sender_tx
                .clone()
                .with(move |sender| (conn_id, sender)),
        );

        let lifetime_task =
            lifetime_task.instrument(tracing::trace_span!("conn lifetime", ?conn_id));

        let duration = std::time::Duration::from_secs(10).add(std::time::Duration::from_mins(
            rand::rng().random_range(..=10u64),
        ));

        // let duration = std::time::Duration::from_secs(5);

        let mut evt_tx = self.evt_tx.clone();
        let managed_task = async move {
            let result = match futures::future::select(
                Box::pin(lifetime_task),
                Box::pin(tokio::time::sleep(duration)),
            )
            .await
            {
                future::Either::Left((lifetime_result, _)) => lifetime_result,
                future::Either::Right((_, life_time_task)) => {
                    tracing::debug!(?conn_id, "end of conn lifetime reached");
                    let _ = conn_close_tx.send(()).await;
                    life_time_task.await
                }
            };

            if let Err(err) = result {
                tracing::error!(?err, "connection error");
            }

            if let Err(_) = evt_tx.send(Event::Closed(conn_id)).await {
                tracing::warn!("evt_tx is broken, exiting");
            }

            Ok(())
        };

        self.conns_scope_handle.run_async(managed_task).await;
        self.conns.insert(conn_id, ConnEntry {});
    }

    pub async fn on_connection_closed(&mut self, conn_id: ConnId) {
        assert!(self.conns.remove(&conn_id).is_some());
    }

    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }
}

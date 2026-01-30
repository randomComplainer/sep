use std::collections::HashMap;
use std::ops::Add as _;

use futures::channel::mpsc;
use futures::prelude::*;
use rand::Rng as _;
use tracing::*;

use crate::prelude::*;
use crate::protocol::ConnId;
use crate::protocol_conn_lifetime::WriteHandle;

pub enum Event {
    Closed(ConnId),
    Errored(ConnId),
    ClientMsg(ConnId, protocol::msg::ClientMsg),
}

struct ConnEntry {
    write_handle: WriteHandle<protocol::msg::ServerMsg>,
}

pub struct State {
    conns: HashMap<ConnId, ConnEntry>,
    conns_scope_handle: task_scope::ScopeHandle<Never>,
    evt_tx: mpsc::UnboundedSender<Event>,
}

impl State {
    pub fn new(
        evt_tx: mpsc::UnboundedSender<Event>,
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
            },
            conns_scope_task,
        )
    }

    pub fn get_write_handle(
        &mut self,
        conn_id: ConnId,
    ) -> Option<WriteHandle<protocol::msg::ServerMsg>> {
        self.conns.get(&conn_id).map(|x| x.write_handle.clone())
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
        let (lifetime_task, write_handle, close_handle) = crate::protocol_conn_lifetime::run(
            Default::default(),
            client_read,
            client_write,
            self.evt_tx
                .clone()
                .with_sync(move |client_msg| Event::ClientMsg(conn_id, client_msg)),
        );

        let duration = std::time::Duration::from_secs(10).add(std::time::Duration::from_mins(
            rand::rng().random_range(..=10u64),
        ));

        // let duration = std::time::Duration::from_secs(5);

        let mut evt_tx = self.evt_tx.clone();

        let managed_task = async move {
            let result = match futures::future::select(
                Box::pin(lifetime_task.instrument(tracing::trace_span!("conn lifetime", ?conn_id))),
                Box::pin(tokio::time::sleep(duration)),
            )
            .await
            {
                future::Either::Left((lifetime_result, _)) => lifetime_result,
                future::Either::Right((_, lifetime_task)) => {
                    tracing::debug!(?conn_id, "end of conn lifetime reached");
                    close_handle.graceful_close().await;
                    lifetime_task.await
                }
            };

            let evt = match result {
                Ok(_) => Event::Closed(conn_id),
                Err(_) => Event::Errored(conn_id),
            };

            if let Err(_) = evt_tx.send(evt).await {
                tracing::warn!("evt_tx is broken");
            }

            Ok(())
        };

        self.conns_scope_handle.run_async(managed_task).await;
        self.conns.insert(conn_id, ConnEntry { write_handle });
    }

    pub async fn on_connection_closed(&mut self, conn_id: ConnId) {
        assert!(self.conns.remove(&conn_id).is_some());
    }

    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }
}

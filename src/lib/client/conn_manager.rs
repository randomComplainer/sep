use std::collections::HashMap;

use futures::channel::mpsc;
use futures::prelude::*;
use tracing::Instrument as _;

use crate::prelude::*;
use crate::protocol::ConnId;
use crate::protocol_conn_lifetime::WriteHandle;

struct ConnEntry {
    write_handle: WriteHandle<protocol::msg::ClientMsg>,
}

pub enum Event {
    Connected(ConnId, WriteHandle<protocol::msg::ClientMsg>),
    ConnectAttemptFailed,
    ServerMsg(protocol::msg::ServerMsg),
    Closed(ConnId),
    Errored(ConnId),
}

pub struct State<ServerConnector>
where
    ServerConnector: super::ServerConnector,
{
    conns: HashMap<ConnId, ConnEntry>,
    conns_scope_handle: task_scope::ScopeHandle<Never>,
    connecting_count: usize,
    server_connector: ServerConnector,
    event_tx: mpsc::UnboundedSender<Event>,
    expected_conn_count: usize,
}

impl<ServerConnector> State<ServerConnector>
where
    ServerConnector: super::ServerConnector + Send,
{
    pub fn new(
        server_connector: ServerConnector,
        event_tx: mpsc::UnboundedSender<Event>,
    ) -> (
        Self,
        impl Future<Output = Result<(), Never>> + Send + 'static,
    ) {
        let (conns_scope_handle, conns_scope_task) = task_scope::new_scope();

        (
            Self {
                conns: HashMap::new(),
                connecting_count: 0,
                conns_scope_handle,
                server_connector,
                event_tx,
                expected_conn_count: 0,
            },
            conns_scope_task,
        )
    }

    pub async fn on_connected(
        &mut self,
        conn_id: ConnId,
        write_handle: WriteHandle<protocol::msg::ClientMsg>,
    ) {
        self.connecting_count -= 1;
        self.conns.insert(conn_id, ConnEntry { write_handle });
    }

    pub async fn on_connection_attempt_failed(&mut self) {
        self.connecting_count -= 1;
        self.match_expected_conn_count().await;
    }

    // closed and errored connections have no differentiation to conn manager
    pub async fn on_connection_closed(&mut self, conn_id: ConnId) {
        assert!(self.conns.remove(&conn_id).is_some());

        self.match_expected_conn_count().await;
    }

    pub async fn set_expected_conn_count(&mut self, expected_conn_count: usize) {
        tracing::info!(
            expected = expected_conn_count,
            active = self.active_conn_count(),
            "set expected conn count"
        );

        self.expected_conn_count = expected_conn_count;
        self.match_expected_conn_count().await;
    }

    async fn match_expected_conn_count(&mut self) {
        tracing::info!(
            expected = self.expected_conn_count,
            active = self.active_conn_count(),
            "match expected conn count"
        );
        while self.active_conn_count() < self.expected_conn_count {
            self.create_connection_in_background().await;
        }
    }

    async fn create_connection_in_background(&mut self) {
        tracing::debug!("create new connection");

        self.connecting_count += 1;
        let connector = self.server_connector.clone();
        let mut event_tx = self.event_tx.clone();
        self.conns_scope_handle
            .run_async(async move {
                let (conn_id, conn_read, conn_write) = match connector
                    .connect()
                    .instrument(tracing::trace_span!("connecte to server"))
                    .await
                {
                    Ok(result) => result,

                    Err(err) => {
                        tracing::error!(?err, "failed to connect to server");

                        if let Err(_) = event_tx
                            .send(Event::ConnectAttemptFailed)
                            .instrument(tracing::trace_span!("connected_tx"))
                            .await
                        {
                            tracing::warn!("connected_tx is broken");
                        }

                        return Ok(());
                    }
                };

                let (lifet_task, write_handle, _) = crate::protocol_conn_lifetime::run(
                    Default::default(),
                    conn_read,
                    conn_write,
                    event_tx.clone().with_sync(Event::ServerMsg),
                );

                if let Err(_) = event_tx.send(Event::Connected(conn_id, write_handle)).await {
                    tracing::warn!("event_tx is broken, exiting");
                    return Ok(());
                }

                match lifet_task.await {
                    Ok(_) => {
                        if let Err(_) = event_tx.send(Event::Closed(conn_id)).await {
                            tracing::warn!("event_tx is broken");
                        }
                    }
                    Err(err) => {
                        tracing::error!(?err, "connection error");
                        if let Err(_) = event_tx
                            .send(Event::Errored(conn_id))
                            .instrument(tracing::trace_span!("connected_tx"))
                            .await
                        {
                            tracing::warn!("connected_tx is broken");
                        }
                    }
                };

                Ok(())
            })
            .await;
    }

    pub fn active_conn_count(&self) -> usize {
        self.conns.len() + self.connecting_count
    }
}

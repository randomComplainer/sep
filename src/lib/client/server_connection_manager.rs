use std::collections::HashMap;

use futures::channel::mpsc;
use futures::prelude::*;
use tracing::Instrument as _;

use crate::async_channel_ext::SenderExt as _;
use crate::prelude::*;
use crate::protocol::ConnId;

struct ConnEntry {}

pub enum Event<GreetedRead, GreetedWrite> {
    Connected(ConnId, GreetedRead, GreetedWrite),
    ServerMsg(protocol::msg::ServerMsg),
    Closed(ConnId),
}

pub struct State<ServerConnector>
where
    ServerConnector: super::ServerConnector,
{
    conns: HashMap<ConnId, ConnEntry>,
    conns_scope_handle: task_scope::ScopeHandle<std::io::Error>,
    connecting_count: usize,
    server_connector: ServerConnector,
    event_tx:
        mpsc::UnboundedSender<Event<ServerConnector::GreetedRead, ServerConnector::GreetedWrite>>,
    client_msg_sender_tx: async_channel::Sender<(
        ConnId,
        crate::oneshot_with_ack::RawSender<protocol::msg::ClientMsg>,
    )>,
    expected_conn_count: usize,
}

impl<ServerConnector> State<ServerConnector>
where
    ServerConnector: super::ServerConnector + Send,
{
    pub fn new(
        server_connector: ServerConnector,
        event_tx: mpsc::UnboundedSender<
            Event<ServerConnector::GreetedRead, ServerConnector::GreetedWrite>,
        >,
        client_msg_sender_tx: async_channel::Sender<(
            ConnId,
            crate::oneshot_with_ack::RawSender<protocol::msg::ClientMsg>,
        )>,
    ) -> (
        Self,
        impl Future<Output = std::io::Result<()>> + Send + 'static,
    ) {
        let (conns_scope_handle, conns_scope_task) = task_scope::new_scope::<std::io::Error>();

        (
            Self {
                conns: HashMap::new(),
                connecting_count: 0,
                conns_scope_handle,
                server_connector,
                event_tx,
                client_msg_sender_tx,
                expected_conn_count: 0,
            },
            conns_scope_task.map(Err),
        )
    }

    async fn create_connection_in_background(&mut self) {
        tracing::debug!("create new connection");

        self.connecting_count += 1;
        let connector = self.server_connector.clone();
        let mut event_tx = self.event_tx.clone();
        self.conns_scope_handle
            .run_async(
                async move {
                    let result = connector
                        .connect()
                        .instrument(tracing::trace_span!("connecte to server"))
                        .await?;

                    if let Err(_) = event_tx
                        .send(Event::Connected(result.0, result.1, result.2))
                        .instrument(tracing::trace_span!("connected_tx"))
                        .await
                    {
                        // TODO: actually exit? seperated task scope?
                        tracing::warn!("connected_tx is broken");
                    }

                    Ok(())
                }
                .instrument(tracing::trace_span!("create connection in background")),
            )
            .await;
    }

    pub async fn on_connected(
        &mut self,
        connected: std::io::Result<(
            ConnId,
            ServerConnector::GreetedRead,
            ServerConnector::GreetedWrite,
        )>,
    ) -> std::io::Result<()> {
        self.connecting_count -= 1;
        let (conn_id, server_read, server_write) = match connected {
            Ok(x) => x,
            Err(err) => {
                tracing::warn!(?err, "failed to connect to server");
                return Err(err);
            }
        };

        assert!(self.conns.get(&conn_id).is_none());

        let client_msg_sender_tx = self.client_msg_sender_tx.clone();
        let mut event_tx = self.event_tx.clone();

        let task = async move {
            let (conn_task, _) = crate::protocol_conn_lifetime::run(
                Default::default(),
                server_read,
                server_write,
                event_tx.clone().with_sync(Event::ServerMsg),
                client_msg_sender_tx.clone().with(
                    move |sender: crate::oneshot_with_ack::RawSender<protocol::msg::ClientMsg>| {
                        (conn_id, sender)
                    },
                ),
            );

            let conn_result = conn_task
                .await
                .inspect_err(|err| tracing::error!(?err, "connection error"));

            if let Err(_) = event_tx.send(Event::Closed(conn_id)).await {
                tracing::warn!("event_tx is broken");
            }

            conn_result
        }
        .instrument(tracing::trace_span!(parent: None, "conn lifetime", ?conn_id));

        self.conns_scope_handle.run_async(task).await;
        self.conns.insert(conn_id, ConnEntry {});

        tracing::info!(
            active = self.active_conn_count(),
            expected = self.expected_conn_count,
            "server connection created"
        );

        Ok(())
    }

    pub async fn on_connection_closed(&mut self, conn_id: ConnId) {
        assert!(self.conns.remove(&conn_id).is_some());

        tracing::info!(
            active = self.active_conn_count(),
            expected = self.expected_conn_count,
            "server connection closed"
        );

        self.match_expected_conn_count().await;
    }

    pub fn active_conn_count(&self) -> usize {
        self.conns.len() + self.connecting_count
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
}

use std::collections::HashMap;

use futures::Sink;
use futures::prelude::*;
use tracing::Instrument as _;

use crate::async_channel_ext::SenderExt as _;
use crate::prelude::*;
use crate::protocol::ConnId;

struct ConnEntry {}

pub struct Handle {
    expectet_conn_count_tx: tokio::sync::watch::Sender<usize>,
}

struct State<ServerConnector, EventTx, ServerMsgTx> {
    conns: HashMap<ConnId, ConnEntry>,
    conns_scope_handle: task_scope::ScopeHandle<std::io::Error>,
    connecting_count: usize,
    server_connector: ServerConnector,
    event_tx: EventTx,
    server_msg_tx: ServerMsgTx,
    client_msg_sender_tx: async_channel::Sender<(
        ConnId,
        crate::oneshot_with_ack::RawSender<protocol::msg::ClientMsg>,
    )>,
    expected_conn_count: usize,
}

enum Event<GreetedRead, GreetedWrite> {
    Connected(Result<(ConnId, GreetedRead, GreetedWrite), std::io::Error>),
    Closed(ConnId),
}

impl<ServerConnector, EventTx, EventTxErr, ServerWrite, ServerRead, ServerMsgTx, ServerMsgTxErr>
    State<ServerConnector, EventTx, ServerMsgTx>
where
    ServerConnector:
        super::ServerConnector<GreetedRead = ServerRead, GreetedWrite = ServerWrite> + Send,
    EventTx:
        Sink<Event<ServerRead, ServerWrite>, Error = EventTxErr> + Unpin + Send + Clone + 'static,
    EventTxErr: std::fmt::Debug,
    ServerWrite:
        protocol::MessageWriter<Message = protocol::msg::conn::ConnMsg<protocol::msg::ClientMsg>>,
    ServerRead:
        protocol::MessageReader<Message = protocol::msg::conn::ConnMsg<protocol::msg::ServerMsg>>,
    ServerMsgTx:
        Sink<protocol::msg::ServerMsg, Error = ServerMsgTxErr> + Unpin + Send + Clone + 'static,
    ServerMsgTxErr: std::fmt::Debug,
{
    pub fn new(
        server_connector: ServerConnector,
        conns_scope_handle: task_scope::ScopeHandle<std::io::Error>,
        event_tx: EventTx,
        server_msg_tx: ServerMsgTx,
        client_msg_sender_tx: async_channel::Sender<(
            ConnId,
            crate::oneshot_with_ack::RawSender<protocol::msg::ClientMsg>,
        )>,
    ) -> Self {
        Self {
            conns: HashMap::new(),
            connecting_count: 0,
            conns_scope_handle,
            server_connector,
            event_tx,
            server_msg_tx,
            client_msg_sender_tx,
            expected_conn_count: 0,
        }
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
                        .await;

                    if let Err(_) = event_tx
                        .send(Event::Connected(result))
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
        connected: std::io::Result<(ConnId, ServerRead, ServerWrite)>,
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

        let server_msg_tx = self.server_msg_tx.clone();
        let client_msg_sender_tx = self.client_msg_sender_tx.clone();
        let mut event_tx = self.event_tx.clone();

        let task = async move {
            let (conn_task, _) = crate::protocol_conn_lifetime_new::run(
                Default::default(),
                server_read,
                server_write,
                server_msg_tx,
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

pub fn run<ServerConnector>(
    server_connector: ServerConnector,
    client_msg_sender_tx: async_channel::Sender<(
        ConnId,
        crate::oneshot_with_ack::RawSender<protocol::msg::ClientMsg>,
    )>,
    server_msg_tx: impl Sink<protocol::msg::ServerMsg, Error = impl std::fmt::Debug>
    + Unpin
    + Send
    + Clone
    + 'static,
) -> (
    Handle,
    impl Future<Output = std::io::Result<()>> + Send + 'static,
)
where
    ServerConnector: super::ServerConnector + Send + 'static,
{
    let (conns_scope_handle, conns_scope_task) = task_scope::new_scope::<std::io::Error>();
    let (expected_conn_count_tx, mut expected_conn_count_rx) = tokio::sync::watch::channel(0);
    let (evt_tx, mut evt_rx) = futures::channel::mpsc::unbounded();
    let mut state = State::new(
        server_connector,
        conns_scope_handle,
        evt_tx,
        server_msg_tx,
        client_msg_sender_tx,
    );

    let main_loop = async move {
        loop {
            tokio::select! {
                r = expected_conn_count_rx.changed() => {
                    if let Err(_) = r {
                        tracing::warn!("expected conn count channel is broken, exiting");
                        return Ok::<_, std::io::Error>(());
                    }

                    let expected_conn_count = *expected_conn_count_rx.borrow_and_update();
                    state.set_expected_conn_count(expected_conn_count).await;
                },
                evt = evt_rx.next() => {
                    let evt = match evt {
                        Some(evt) => evt,
                        None => {
                            tracing::warn!("evt_rx is broken, exiting");
                            return Ok(());
                        }
                    };

                    match evt {
                        Event::Connected(connect_result) => {
                            state.on_connected(connect_result).await?;
                        },
                        Event::Closed(conn_id) => {
                            state.on_connection_closed(conn_id).await;
                        },
                    };

                    state.match_expected_conn_count().await;
                }
            }
        }
    }
    .instrument(tracing::trace_span!("conn manager main loop"));

    return (Handle::new(expected_conn_count_tx), async move {
        tokio::select! {
            r = main_loop => r,
            e = conns_scope_task => Err(e),
        }
    });
}

impl Handle {
    pub fn new(expectet_conn_count_tx: tokio::sync::watch::Sender<usize>) -> Self {
        Self {
            expectet_conn_count_tx,
        }
    }

    pub fn set_expected_conn_count(&mut self, count: usize) {
        if let Err(_) = self.expectet_conn_count_tx.send(count) {
            tracing::warn!("expectet_conn_count_tx is broken");
        }
    }
}

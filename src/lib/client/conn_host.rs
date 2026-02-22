use futures::prelude::*;
use tokio::sync::oneshot;
use tracing::Instrument as _;

use crate::prelude::*;
use crate::protocol::ConnId;

pub enum Event {
    ServerConnected(ConnId),
    ConnectionAttemptFailed,
    ConnectionErrored(ConnId),
    ConnectionEnded(ConnId),
    ServerMsg(ConnId, protocol::msg::ServerMsg),
    ClientMsgSenderReady(ConnId, oneshot::Sender<protocol::msg::ClientMsg>),
}

pub fn create<EvtTx, EvtTxErr, ServerConnector>(
    evt_tx: EvtTx,
    server_connector: ServerConnector,
) -> (
    impl Future<Output = Result<(), Never>> + Send,
    Handle<EvtTx, ServerConnector>,
)
where
    ServerConnector: super::ServerConnector,
    EvtTx: Sink<Event, Error = EvtTxErr> + Unpin + Send + Clone + 'static,
    EvtTxErr: std::fmt::Debug + Send,
{
    let (scope_handle, scope_task) = task_scope::new_scope();

    (
        scope_task,
        Handle {
            evt_tx,
            scope_handle,
            server_connector,
        },
    )
}

pub struct Handle<EvtTx, ServerConnector> {
    evt_tx: EvtTx,
    scope_handle: task_scope::ScopeHandle<Never>,
    server_connector: ServerConnector,
}

impl<EvtTx, EvtTxErr, ServerConnector> Handle<EvtTx, ServerConnector>
where
    ServerConnector: super::ServerConnector,
    EvtTx: Sink<Event, Error = EvtTxErr> + Unpin + Send + Clone + 'static,
    EvtTxErr: std::fmt::Debug + Send,
{
    pub async fn create_connection(&mut self) {
        tracing::debug!("create new connection");

        let connector = self.server_connector.clone();
        let mut evt_tx = self.evt_tx.clone();

        let task = async move {
            let (conn_id, conn_read, conn_write) = match connector
                .connect()
                .instrument(tracing::trace_span!("connecte to server"))
                .await
            {
                Ok(result) => result,

                Err(err) => {
                    tracing::error!(?err, "failed to connect to server");

                    if let Err(_) = evt_tx.send(Event::ConnectionAttemptFailed).await {
                        tracing::warn!("connected_tx is broken");
                    }

                    return;
                }
            };

            if let Err(_) = evt_tx.send(Event::ServerConnected(conn_id)).await {
                tracing::warn!("event_tx is broken, exiting");
                return;
            }

            let (lifetime_task, _gentle_close_sender) = crate::protocol_conn_lifetime::run(
                Default::default(),
                conn_read,
                conn_write,
                evt_tx
                    .clone()
                    .with_sync(move |server_msg| Event::ServerMsg(conn_id, server_msg)),
                evt_tx
                    .clone()
                    .with_sync(move |sender| Event::ClientMsgSenderReady(conn_id, sender)),
            );

            match lifetime_task
                .instrument(tracing::trace_span!("conn lifetime", ?conn_id))
                .await
            {
                Ok(_) => {
                    if let Err(_) = evt_tx.send(Event::ConnectionEnded(conn_id)).await {
                        tracing::warn!("event_tx is broken");
                    }
                }
                Err(err) => {
                    tracing::error!(?err, "connection error");
                    if let Err(_) = evt_tx.send(Event::ConnectionErrored(conn_id)).await {
                        tracing::warn!("connected_tx is broken");
                    }
                }
            };
        };

        self.scope_handle.run_async(task.map(|_| Ok(()))).await;
    }
}

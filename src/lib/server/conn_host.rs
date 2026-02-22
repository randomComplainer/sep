use std::ops::Add as _;

use futures::prelude::*;
use rand::Rng as _;
use tokio::sync::oneshot;

use crate::prelude::*;
use crate::protocol::ConnId;

pub enum Event {
    // NewConnection(ConnId),
    ConnectionErrored(ConnId),
    ConnectionEnded(ConnId),
    ClientMsg(ConnId, protocol::msg::ClientMsg),
    ServerMsgSenderReady(ConnId, oneshot::Sender<protocol::msg::ServerMsg>),
}

pub fn create<EvtTx>(evt_tx: EvtTx) -> (impl Future<Output = Result<(), Never>>, Handle<EvtTx>) {
    let (scope_handle, scope_task) = task_scope::new_scope();
    (
        scope_task,
        Handle {
            evt_tx,
            scope_handle,
        },
    )
}

pub struct Handle<EvtTx> {
    evt_tx: EvtTx,
    scope_handle: task_scope::ScopeHandle<Never>,
}

impl<EvtTx, EvtTxErr> Handle<EvtTx>
where
    EvtTx: Sink<Event, Error = EvtTxErr> + Unpin + Send + Clone + 'static,
    EvtTxErr: std::fmt::Debug + Send + 'static,
{
    pub async fn new_conn<ClientRead, ClientWrite>(
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
        let mut evt_tx = self.evt_tx.clone();

        let span = tracing::trace_span!("conn lifetime", ?conn_id);

        let managed_task = span.in_scope(|| {
            let (lifetime_task, gentle_close_sender) = crate::protocol_conn_lifetime::run(
                Default::default(),
                client_read,
                client_write,
                evt_tx
                    .clone()
                    .with_sync(move |client_msg| Event::ClientMsg(conn_id, client_msg)),
                evt_tx
                    .clone()
                    .with_sync(move |sender| Event::ServerMsgSenderReady(conn_id, sender)),
            );

            let duration = std::time::Duration::from_secs(10).add(std::time::Duration::from_mins(
                rand::rng().random_range(..=10u64),
            ));

            // let duration = std::time::Duration::from_secs(5).add(std::time::Duration::from_secs(
            //     rand::rng().random_range(..=5u64),
            // ));

            let timeout_task = tokio::time::sleep(duration);

            async move {
                let result =
                    match futures::future::select(Box::pin(lifetime_task), Box::pin(timeout_task))
                        .await
                    {
                        future::Either::Left((lifetime_result, _)) => lifetime_result,
                        future::Either::Right((_, lifetime_task)) => {
                            tracing::debug!(?conn_id, "end of conn lifetime reached");
                            gentle_close_sender.gentle_close();
                            lifetime_task.await
                        }
                    };

                let evt = match result {
                    Ok(_) => Event::ConnectionEnded(conn_id),
                    Err(_) => Event::ConnectionErrored(conn_id),
                };

                if let Err(_) = evt_tx.send(evt).await {
                    tracing::warn!("evt_tx is broken");
                }

                return Ok(());
            }
        });

        self.scope_handle.run_async(managed_task).await;
    }
}

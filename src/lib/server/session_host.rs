use futures::{channel::mpsc, prelude::*};
use tracing::Instrument as _;

use super::target_io;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct Config<TConnectTarget> {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
    pub connect_target: TConnectTarget,
}

impl<TConnectTarget> Into<target_io::Config<TConnectTarget>> for Config<TConnectTarget> {
    fn into(self) -> target_io::Config<TConnectTarget> {
        target_io::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
            connect_target: self.connect_target,
        }
    }
}

pub enum Event {
    SessionEnded(SessionId),
    ServerMsg(SessionId, protocol::msg::session::ServerMsg),
}

pub fn create<EvtTx, TConnectTarget>(
    config: Config<TConnectTarget>,
    evt_tx: EvtTx,
) -> (
    impl Future<Output = Result<(), Never>> + Send + 'static,
    Handle<EvtTx, TConnectTarget>,
) {
    let (scope_handle, scope_task) = task_scope::new_scope();

    (
        scope_task,
        Handle {
            config,
            evt_tx,
            scope_handle,
        },
    )
}

pub struct Handle<EvtTx, TConnectTarget> {
    config: Config<TConnectTarget>,
    evt_tx: EvtTx,
    scope_handle: task_scope::ScopeHandle<Never>,
}

impl<EvtTx, TConnectTarget> Handle<EvtTx, TConnectTarget>
where
    EvtTx: Sink<Event> + Unpin + Send + Clone + 'static,
    TConnectTarget: crate::connect_target::ConnectTarget,
{
    pub async fn new_session(
        &mut self,
        session_id: SessionId,
    ) -> mpsc::UnboundedSender<protocol::msg::session::ClientMsg> {
        let (session_client_msg_tx, session_client_msg_rx) = mpsc::unbounded();
        let mut evt_tx = self.evt_tx.clone();

        let target_io_task = target_io::run(
            session_client_msg_rx,
            evt_tx
                .clone()
                .with_sync(move |msg| Event::ServerMsg(session_id, msg)),
            self.config.clone().into(),
        );

        let session_task = async move {
            if let Err(e) = target_io_task.await {
                tracing::error!(err=?e, "session ends in error");
            }

            if let Err(_) = evt_tx.send(Event::SessionEnded(session_id)).await {
                tracing::warn!("evt_tx is broken");
            }

            Ok(())
        }
        .instrument(tracing::trace_span!("session", ?session_id));

        self.scope_handle.run_async(session_task).await;

        session_client_msg_tx
    }
}

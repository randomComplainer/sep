use futures::{channel::mpsc, prelude::*};
use tracing::Instrument as _;

use super::proxyee_io;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub max_packet_size: u16,
    pub max_bytes_ahead: u32,
}

impl Into<proxyee_io::Config> for Config {
    fn into(self) -> proxyee_io::Config {
        proxyee_io::Config {
            max_packet_size: self.max_packet_size,
            max_bytes_ahead: self.max_bytes_ahead,
        }
    }
}

pub enum Event {
    SessionEnded(SessionId),
    ClientMsg(SessionId, protocol::msg::session::ClientMsg),
}

pub fn create<EvtTx>(
    config: Config,
    evt_tx: EvtTx,
) -> (
    impl Future<Output = Result<(), Never>> + Send + 'static,
    Handle<EvtTx>,
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

pub struct Handle<EvtTx> {
    config: Config,
    evt_tx: EvtTx,
    scope_handle: task_scope::ScopeHandle<Never>,
}

impl<EvtTx> Handle<EvtTx>
where
    EvtTx: Sink<Event> + Unpin + Send + Clone + 'static,
{
    pub async fn new_session(
        &mut self,
        session_id: SessionId,
        agent: impl socks5::server_agent::Init,
    ) -> mpsc::UnboundedSender<protocol::msg::session::ServerMsg> {
        let (session_server_msg_tx, session_server_msg_rx) = mpsc::unbounded();
        let mut evt_tx = self.evt_tx.clone();

        let proxyee_io_task = proxyee_io::run(
            agent,
            session_server_msg_rx,
            evt_tx
                .clone()
                .with_sync(move |msg: protocol::msg::session::ClientMsg| {
                    Event::ClientMsg(session_id, msg)
                }),
            self.config.into(),
        );

        let session_task = async move {
            if let Err(e) = proxyee_io_task.await {
                tracing::error!(err=?e, "session ends in error");
            }

            if let Err(_) = evt_tx.send(Event::SessionEnded(session_id)).await {
                tracing::warn!("evt_tx is broken");
            }

            Ok(())
        }
        .instrument(tracing::debug_span!("session", session_id=?session_id));

        self.scope_handle.run_async(session_task).await;

        session_server_msg_tx
    }
}

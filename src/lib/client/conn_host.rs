use futures::prelude::*;
use tokio::sync::oneshot;

use crate::ok_or;
use crate::prelude::*;
use crate::protocol::ConnId;
use crate::some_or;

pub enum Event {
    ServerConnected(ConnId),
    ConnectionErrored(ConnId),
    ConnectionEnded(ConnId),
    ServerMsg(ConnId, protocol::msg::ServerMsg),
    ClientMsgSenderReady(ConnId, oneshot::Sender<protocol::msg::ClientMsg>),
}

pub fn create<EvtTx, EvtTxErr, ServerConnector>(
    mut evt_tx: EvtTx,
    server_connector: ServerConnector,
) -> (impl Future<Output = Result<(), Never>>, Handle)
where
    ServerConnector: super::ServerConnector,
    EvtTx: Sink<Event, Error = EvtTxErr> + Unpin + Send + Clone + 'static,
    EvtTxErr: std::fmt::Debug + Send + 'static,
{
    let (conn_creation_cmd_tx, conn_creation_cmd_rx) =
        tokio::sync::mpsc::unbounded_channel::<conn_creation::Cmd>();

    let mut new_conn_stream = conn_creation::run(server_connector, conn_creation_cmd_rx);

    let main_task = {
        let conn_creation_cmd_tx = conn_creation_cmd_tx.clone();
        async move {
            loop {
                let (conn_id, conn_read, conn_write) =
                    some_or!(new_conn_stream.next().await, return Ok(()));

                ok_or!(
                    evt_tx.send(Event::ServerConnected(conn_id)).await,
                    return Ok(())
                );

                let span = tracing::trace_span!("conn lifetime", ?conn_id);

                let task = span.in_scope(|| {
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

                    let mut evt_tx = evt_tx.clone();
                    let conn_creation_cmd_tx = conn_creation_cmd_tx.clone();
                    async move {
                        match lifetime_task.await {
                            Ok(_) => {
                                let _ = evt_tx.send(Event::ConnectionEnded(conn_id)).await;
                            }
                            Err(error) => {
                                tracing::error!(?error, "conn ends in error");
                                let _ = evt_tx.send(Event::ConnectionErrored(conn_id)).await;
                            }
                        };

                        let _ = conn_creation_cmd_tx.send(conn_creation::Cmd::Disconnected);
                    }
                });

                tokio::spawn(task);
            }
        }
    };

    (
        main_task,
        Handle {
            conn_creation_cmd_tx,
        },
    )
}

#[derive(Clone)]
pub struct Handle {
    conn_creation_cmd_tx: tokio::sync::mpsc::UnboundedSender<conn_creation::Cmd>,
}

impl Handle {
    pub fn expect_conn(&mut self, expected: u8) {
        tracing::debug!("create new connection");

        let _ = self
            .conn_creation_cmd_tx
            .send(conn_creation::Cmd::Expected(expected));
    }
}

mod conn_creation {
    use tracing::Instrument as _;

    use crate::prelude::*;

    pub enum Cmd {
        Expected(u8),
        Disconnected,
    }

    #[derive(Debug)]
    struct State {
        expected: u8,
        current: u8,
    }

    impl State {
        pub fn new() -> Self {
            Self {
                expected: 0,
                current: 0,
            }
        }

        pub fn satisfied(&self) -> bool {
            self.expected <= self.current
        }
    }

    pub fn run<ServerConnector>(
        connector: ServerConnector,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<Cmd>,
    ) -> impl futures::Stream<
        Item = (
            ConnId,
            ServerConnector::GreetedRead,
            ServerConnector::GreetedWrite,
        ),
    >
    + 'static
    + Unpin
    where
        ServerConnector: crate::client::server_connector::ServerConnector,
    {
        let (state_tx, state_rx) = tokio::sync::watch::channel(State::new());

        let stream = futures::stream::unfold(
            (connector, cmd_rx, state_tx, state_rx),
            async move |(connector, mut cmd_rx, state_tx, mut state_rx)| {
                let new_conn = loop {
                    tokio::select! {
                        cmd = cmd_rx.recv() => {
                            let cmd = cmd?;

                            match cmd {
                                Cmd::Expected(x) => {
                                    let _ = state_tx.send_if_modified(|old| {
                                        if old.expected < x {
                                            old.expected = x;
                                            true
                                        } else {
                                            false
                                        }
                                    });
                                }
                                Cmd::Disconnected => state_tx.send_modify(|old| old.current -= 1),
                            };
                        },
                        lock = state_rx.wait_for(|s| !s.satisfied()) => {
                            let lock =  lock.ok()?;
                            tracing::debug!(state=?*lock, "unsatisified conn count");
                            drop(lock);

                            let new_conn = loop {
                                match connector
                                    .connect()
                                    .instrument(tracing::trace_span!("connect to server"))
                                    .await
                                {
                                    Ok(result) => break result,

                                    Err(err) => {
                                        tracing::error!(?err, "failed to connect to server");

                                        // TODO: delay? retry limitation?
                                        continue;
                                    }
                                }
                            };

                            state_tx.send_modify(|old| {
                                old.current += 1;
                                if old.satisfied() {
                                    old.expected = 0;
                                }
                            });

                            break new_conn;
                        }

                    }
                };

                return Some((new_conn, (connector, cmd_rx, state_tx, state_rx)));
            },
        );

        return Box::pin(stream);
    }
}

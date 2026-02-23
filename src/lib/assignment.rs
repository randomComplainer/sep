use std::collections::{HashMap, HashSet, VecDeque};

use futures::channel::mpsc;
use futures::prelude::*;
use tokio::sync::oneshot;

use crate::prelude::*;

struct SessionEntry<IncomingMsg> {
    assigned_conns: HashSet<ConnId>,
    incomming_msg_tx: mpsc::UnboundedSender<IncomingMsg>,
    conn_count_to_keep: usize,
}

struct ConnEntry<OutgoingMsg> {
    assigned_sessions: HashSet<SessionId>,
    outgoing_msg_tx: Option<oneshot::Sender<OutgoingMsg>>,
}

pub struct State<OutgoingMsg, IncomingMsg> {
    sessions: HashMap<SessionId, SessionEntry<IncomingMsg>>,
    conns: HashMap<ConnId, ConnEntry<OutgoingMsg>>,
    conns_by_num_of_assignment: Vec<ConnId>, // keep sorted
    outgoing_queue: VecDeque<(Option<SessionId>, OutgoingMsg)>, // None session id for global msg
    pub max_conn_count_to_keep_for_session: usize,
}

#[derive(Debug)]
pub enum Action {
    UpgradeSession {
        session_id: SessionId,
        conn_count_to_keep: usize,
    },
    KillSession(SessionId),
}

impl<OutgoingMsg, IncomingMsg> State<OutgoingMsg, IncomingMsg> {
    pub fn new() -> Self {
        Self {
            sessions: Default::default(),
            conns: Default::default(),
            conns_by_num_of_assignment: Default::default(),
            outgoing_queue: Default::default(),
            max_conn_count_to_keep_for_session: 0,
        }
    }

    #[must_use]
    pub fn new_outgoing_session_msg(
        &mut self,
        session_id: &SessionId,
        mut msg: OutgoingMsg,
    ) -> Vec<Action> {
        let session = match self.sessions.get_mut(session_id) {
            Some(session) => session,
            None => {
                tracing::debug!(?session_id, "session does not exist, droping message");
                return Default::default();
            }
        };

        // try sending via an assigned connection that is available right now first
        // TODO: distribute workload to connections more evenly
        for conn_id in session.assigned_conns.iter() {
            let conn = self.conns.get_mut(conn_id).unwrap();
            match conn.outgoing_msg_tx.take() {
                Some(msg_tx) => {
                    match msg_tx.send(msg) {
                        Ok(_) => return Default::default(),
                        Err(rejected) => {
                            msg = rejected;
                            continue;
                        }
                    };
                }
                None => continue,
            };
        }

        // try assigning an existing connection that is available right now
        msg = {
            for conn_id in self
                .conns_by_num_of_assignment
                .iter()
                .filter(|conn_id| !session.assigned_conns.contains(conn_id))
            {
                let conn = self.conns.get_mut(conn_id).unwrap();

                let msg_tx = match conn.outgoing_msg_tx.take() {
                    Some(x) => x,
                    None => continue,
                };

                match msg_tx.send(msg) {
                    Ok(_) => {
                        // assign
                        assert!(conn.assigned_sessions.insert(*session_id));
                        assert!(session.assigned_conns.insert(*conn_id));

                        session.conn_count_to_keep =
                            std::cmp::max(session.conn_count_to_keep, session.assigned_conns.len());
                        self.sort_conns();
                        return Default::default();
                    }
                    Err(rejected) => {
                        msg = rejected;
                        continue;
                    }
                };
            }

            msg
        };

        // no connection available, queue the message
        // and requet new connection if conn_count_to_keep is already fulfilled
        self.outgoing_queue.push_back((Some(*session_id), msg));
        if session.conn_count_to_keep <= session.assigned_conns.len() {
            session.conn_count_to_keep += 2;
            self.max_conn_count_to_keep_for_session = std::cmp::max(
                self.max_conn_count_to_keep_for_session,
                session.conn_count_to_keep,
            );
            return vec![Action::UpgradeSession {
                session_id: *session_id,
                conn_count_to_keep: session.conn_count_to_keep,
            }];
        } else {
            return Default::default();
        }
    }

    pub fn new_outgoing_global_msg(&mut self, mut msg: OutgoingMsg) {
        for conn_id in self.conns_by_num_of_assignment.iter() {
            let conn = self.conns.get_mut(conn_id).unwrap();
            let sender = match conn.outgoing_msg_tx.take() {
                Some(sender) => sender,
                None => continue,
            };

            if let Err(rejected) = sender.send(msg) {
                msg = rejected;
                continue;
            } else {
                return Default::default();
            }
        }

        self.outgoing_queue.push_front((None, msg));

        return Default::default();
    }

    pub fn conn_ready_to_send(
        &mut self,
        conn_id: &ConnId,
        conn_msg_tx: oneshot::Sender<OutgoingMsg>,
    ) {
        let conn = match self.conns.get_mut(conn_id) {
            Some(x) => x,
            None => {
                tracing::debug!(?conn_id, "conn does not exist, droping msg sender");
                return;
            }
        };

        // send queued msg that
        // - is global message or
        // - is from sessions the connection is assigned to
        // first
        // TODO: remove/insert msg is O(msg_count_in_queue)
        if let Some((idx, session_id, msg)) = self
            .outgoing_queue
            .iter()
            .position(|(session_id_opt, _)| {
                session_id_opt
                    .map(|session_id| conn.assigned_sessions.contains(&session_id))
                    .unwrap_or(true)
            })
            .map(|idx| {
                let (session_id, msg) = self.outgoing_queue.remove(idx).unwrap();
                (idx, session_id, msg)
            })
        {
            if let Err(rejected) = conn_msg_tx.send(msg) {
                tracing::warn!(?conn_id, "failed to send message to conn");
                self.outgoing_queue.insert(idx, (session_id, rejected));
            }

            return;
        }

        // send the first msg in queue
        if let Some((session_id_opt, msg)) = self.outgoing_queue.pop_front() {
            if let Err(rejected) = conn_msg_tx.send(msg) {
                tracing::warn!(?conn_id, "failed to send message to conn");
                self.outgoing_queue.push_front((session_id_opt, rejected));
                // return here to avoid assignment of this broken conn
                return;
            }

            // and assign the connection to it,
            // if that's a session msg of a session
            if let Some((session_id, session)) = session_id_opt.and_then(|session_id| {
                self.sessions
                    .get_mut(&session_id)
                    .map(|session| (session_id, session))
            }) {
                assert!(session.assigned_conns.insert(*conn_id));
                assert!(conn.assigned_sessions.insert(session_id));

                self.sort_conns();
            };

            return;
        }

        // no message to send at the moment, store the msg sender
        assert!(conn.outgoing_msg_tx.replace(conn_msg_tx).is_none());
    }

    pub fn on_conn_created(&mut self, conn_id: ConnId) {
        let conn = ConnEntry {
            assigned_sessions: Default::default(),
            outgoing_msg_tx: None,
        };

        self.conns.insert(conn_id, conn);
        self.conns_by_num_of_assignment.insert(0, conn_id);
    }

    pub fn on_conn_closed(&mut self, conn_id: &ConnId) {
        let conn = match self.conns.remove(conn_id) {
            Some(x) => x,
            None => return,
        };

        self.conns_by_num_of_assignment.remove(
            self.conns_by_num_of_assignment
                .iter()
                .position(|entry_id| entry_id.eq(conn_id))
                .unwrap(),
        );

        for session_id in conn.assigned_sessions.iter() {
            let session = self.sessions.get_mut(session_id).unwrap();
            assert!(session.assigned_conns.remove(&conn_id));
            // if let Some(session) = self.sessions.get_mut(session_id) {
            //     session.assigned_conns.remove(&conn_id);
            // }
        }
    }

    pub fn on_conn_errored(&mut self, conn_id: &ConnId) -> Vec<Action> {
        let conn = {
            match self.conns.remove(conn_id) {
                Some(x) => x,
                None => return Default::default(),
            }
        };

        let actions = conn
            .assigned_sessions
            .iter()
            .map(|session_id| Action::KillSession(*session_id))
            .collect();

        self.conns_by_num_of_assignment.remove(
            self.conns_by_num_of_assignment
                .iter()
                .position(|entry_id| entry_id.eq(conn_id))
                .unwrap(),
        );

        self.outgoing_queue.retain(|(session_id_opt, _)| {
            session_id_opt
                .as_ref()
                .map(|msg_session_id| !conn.assigned_sessions.contains(&msg_session_id))
                .unwrap_or(true)
        });

        for session_id in conn.assigned_sessions.iter() {
            // remove session from session list &
            // remove session from assigned connections
            let session = self.sessions.remove(session_id).unwrap();
            for assigned_conn_id in session
                .assigned_conns
                .iter()
                .filter(|assigned_conn_id| !assigned_conn_id.eq(&conn_id))
            {
                let assigned_conn = self.conns.get_mut(assigned_conn_id).unwrap();
                assert!(assigned_conn.assigned_sessions.remove(session_id));
            }
        }

        self.sort_conns();

        actions
    }

    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }

    pub fn on_new_session(
        &mut self,
        session_id: SessionId,
        session_incomming_msg_tx: mpsc::UnboundedSender<IncomingMsg>,
    ) {
        self.sessions.insert(
            session_id,
            SessionEntry {
                assigned_conns: Default::default(),
                incomming_msg_tx: session_incomming_msg_tx,
                conn_count_to_keep: 2,
            }
            .into(),
        );

        self.max_conn_count_to_keep_for_session =
            std::cmp::max(2, self.max_conn_count_to_keep_for_session);
    }

    pub async fn on_msg_to_session(
        &mut self,
        conn_id: &ConnId,
        session_id: &SessionId,
        msg: IncomingMsg,
    ) {
        let session = match self.sessions.get_mut(session_id) {
            Some(x) => x,
            None => {
                tracing::warn!(?session_id, "seesion does not exist, dropping message");
                return;
            }
        };

        let _ = session.incomming_msg_tx.send(msg).await;

        if !session.assigned_conns.contains(conn_id) {
            let conn = self.conns.get_mut(conn_id).unwrap();
            assert!(conn.assigned_sessions.insert(*session_id));
            assert!(session.assigned_conns.insert(*conn_id));

            self.sort_conns();
        }
    }

    // session ending and erroring is the same in the perspective of assignment
    pub fn on_session_ended(&mut self, session_id: &SessionId) {
        let session = match self.sessions.remove(session_id) {
            Some(x) => x,
            None => return,
        };

        for conn_id in session.assigned_conns.iter() {
            assert!(
                self.conns
                    .get_mut(conn_id)
                    .unwrap()
                    .assigned_sessions
                    .remove(session_id)
            );
        }

        self.sort_conns();

        if session.conn_count_to_keep == self.max_conn_count_to_keep_for_session {
            self.max_conn_count_to_keep_for_session = self
                .sessions
                .values()
                .map(|s| s.conn_count_to_keep)
                .max()
                .unwrap_or(0);
        }
    }

    pub fn upgrade_session(&mut self, session_id: &SessionId, conn_num: usize) {
        let session = match self.sessions.get_mut(session_id) {
            Some(x) => x,
            None => return,
        };

        session.conn_count_to_keep = std::cmp::max(session.conn_count_to_keep, conn_num);
        self.max_conn_count_to_keep_for_session = std::cmp::max(
            self.max_conn_count_to_keep_for_session,
            session.conn_count_to_keep,
        );
    }

    fn sort_conns(&mut self) {
        self.conns_by_num_of_assignment.sort_by(|l, r| {
            self.conns
                .get(l)
                .unwrap()
                .assigned_sessions
                .len()
                .cmp(&self.conns.get(r).unwrap().assigned_sessions.len())
        });
    }
}

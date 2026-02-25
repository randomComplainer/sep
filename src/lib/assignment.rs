use std::collections::{HashMap, HashSet, VecDeque};

use futures::channel::mpsc;
use futures::prelude::*;
use tokio::sync::oneshot;

use crate::prelude::*;

struct SessionEntry<OutgoingMsg, IncomingMsg> {
    assigned_conns: HashSet<ConnId>,
    incomming_msg_tx: mpsc::UnboundedSender<IncomingMsg>,
    conn_count_to_keep: usize,
    outgoing_msg_queue: VecDeque<(u64, OutgoingMsg)>,
}

struct ConnEntry<OutgoingMsg> {
    assigned_sessions: HashSet<SessionId>,
    outgoing_msg_tx: Option<(u64, oneshot::Sender<OutgoingMsg>)>,
}

pub struct State<OutgoingMsg, IncomingMsg> {
    session_msg_seq: u64,
    conn_msg_sender_seq: u64,
    sessions: HashMap<SessionId, SessionEntry<OutgoingMsg, IncomingMsg>>,
    conns: HashMap<ConnId, ConnEntry<OutgoingMsg>>,
    conns_by_num_of_assignment: Vec<ConnId>, // keep sorted
    global_outgoing_queue: VecDeque<OutgoingMsg>, // None session id for global msg
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
            session_msg_seq: 0,
            conn_msg_sender_seq: 0,
            sessions: Default::default(),
            conns: Default::default(),
            conns_by_num_of_assignment: Default::default(),
            global_outgoing_queue: Default::default(),
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
        loop {
            let conn_opt = session
                .assigned_conns
                .iter()
                .fold(None, |r: Option<(ConnId, u64)>, conn_id| {
                    let conn_sender_seq = match self.conns.get(conn_id).unwrap().outgoing_msg_tx {
                        Some((x, _)) => x,
                        None => return r,
                    };

                    match &r {
                        Some((_, r_seq)) => {
                            if conn_sender_seq.lt(r_seq) {
                                Some((*conn_id, conn_sender_seq))
                            } else {
                                r
                            }
                        }
                        None => Some((*conn_id, conn_sender_seq)),
                    }
                })
                .map(|(conn_id, _)| conn_id);

            let conn_id = match conn_opt {
                Some(x) => x,
                None => break,
            };

            let conn = self.conns.get_mut(&conn_id).unwrap();
            let msg_tx = conn.outgoing_msg_tx.take().unwrap().1;
            match msg_tx.send(msg) {
                Ok(_) => return Default::default(),
                Err(rejected) => {
                    msg = rejected;
                    continue;
                }
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
                    Some(x) => x.1,
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
        session
            .outgoing_msg_queue
            .push_back((self.session_msg_seq, msg));
        self.session_msg_seq += 1;
        if session.conn_count_to_keep <= session.assigned_conns.len() {
            session.conn_count_to_keep += 4;
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
                Some(x) => x.1,
                None => continue,
            };

            if let Err(rejected) = sender.send(msg) {
                msg = rejected;
                continue;
            } else {
                return Default::default();
            }
        }

        self.global_outgoing_queue.push_front(msg);

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

        // send global msg first
        if let Some(msg) = self.global_outgoing_queue.pop_front() {
            if let Err(rejected) = conn_msg_tx.send(msg) {
                tracing::warn!(?conn_id, "failed to send message to conn");
                self.global_outgoing_queue.push_front(rejected);
            }

            return;
        }

        // send msg from assigned sessions
        if let Some((session_id, _)) =
            conn.assigned_sessions
                .iter()
                .fold(None, |r: Option<(SessionId, u64)>, session_id| {
                    let session_msg_seq = match self
                        .sessions
                        .get(session_id)
                        .unwrap()
                        .outgoing_msg_queue
                        .get(0)
                        .map(|x| x.0)
                    {
                        Some(x) => x,
                        None => return r,
                    };

                    match &r {
                        Some((_, r_session_id)) => {
                            if session_msg_seq.lt(r_session_id) {
                                Some((*session_id, session_msg_seq))
                            } else {
                                r
                            }
                        }
                        None => Some((*session_id, session_msg_seq)),
                    }
                })
        {
            let session = self.sessions.get_mut(&session_id).unwrap();

            let (seq, msg) = session.outgoing_msg_queue.pop_front().unwrap();

            if let Err(rejected) = conn_msg_tx.send(msg) {
                tracing::warn!(?conn_id, "failed to send message to conn");
                session.outgoing_msg_queue.push_front((seq, rejected));
            }

            return;
        }

        // send any queued msg from any session
        if let Some((session_id, _)) = self.sessions.iter().fold(
            None,
            |r: Option<(SessionId, u64)>, (session_id, session)| {
                if conn.assigned_sessions.contains(session_id) {
                    return r;
                }

                let session_msg_seq = match session.outgoing_msg_queue.get(0).map(|x| x.0) {
                    Some(x) => x,
                    None => return r,
                };

                match &r {
                    Some((_, r_session_id)) => {
                        if session_msg_seq.lt(r_session_id) {
                            Some((*session_id, session_msg_seq))
                        } else {
                            r
                        }
                    }
                    None => Some((*session_id, session_msg_seq)),
                }
            },
        ) {
            let session = self.sessions.get_mut(&session_id).unwrap();

            let (seq, msg) = session.outgoing_msg_queue.pop_front().unwrap();

            if let Err(rejected) = conn_msg_tx.send(msg) {
                tracing::warn!(?conn_id, "failed to send message to conn");
                session.outgoing_msg_queue.push_front((seq, rejected));
                return;
            }

            // and assign the connection to it
            assert!(session.assigned_conns.insert(*conn_id));
            assert!(conn.assigned_sessions.insert(session_id));

            self.sort_conns();

            return;
        }

        // no message to send at the moment, store the msg sender
        assert!(
            conn.outgoing_msg_tx
                .replace((self.conn_msg_sender_seq, conn_msg_tx))
                .is_none()
        );
        self.conn_msg_sender_seq += 1;
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
                outgoing_msg_queue: Default::default(),
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

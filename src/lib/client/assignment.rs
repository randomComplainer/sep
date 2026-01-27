use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
};

use crate::{prelude::*, protocol_conn_lifetime_new::WriteHandle};

use Action::*;

pub enum Action {
    Assign(SessionId, ConnId, WriteHandle<protocol::msg::ClientMsg>),
    Kill(SessionId),
}

impl Debug for Action {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Assign(session_id, conn_id, _write_handle) => f
                .debug_struct("Assign")
                .field("session_id", session_id)
                .field("conn_id", conn_id)
                .finish(),
            Action::Kill(session_id) => f.debug_tuple("Kill").field(session_id).finish(),
        }
    }
}

struct ConnEntry {
    id: ConnId,
    sessions: HashSet<SessionId>,
    write_handle: WriteHandle<protocol::msg::ClientMsg>,
}

pub struct State {
    // ordered
    conns: Vec<ConnEntry>,
    sessions: HashMap<SessionId, HashSet<ConnId>>,
    unsatified_sessions: Vec<SessionId>,
    expected_conn_per_session: usize,
}

// TODO: it's so bad
impl State {
    pub fn new(expected_conn_per_session: usize) -> Self {
        Self {
            conns: Vec::new(),
            sessions: HashMap::new(),
            unsatified_sessions: Vec::new(),
            expected_conn_per_session,
        }
    }

    #[must_use]
    pub fn new_session(&mut self, session_id: SessionId) -> Vec<Action> {
        self.sessions.insert(session_id, Default::default());
        self.unsatified_sessions.push(session_id);
        self.resolve_unsatisfied_sessions()
    }

    #[must_use]
    pub fn new_conn(
        &mut self,
        conn_id: ConnId,
        write_handle: WriteHandle<protocol::msg::ClientMsg>,
    ) -> Vec<Action> {
        self.conns.push(ConnEntry {
            id: conn_id,
            sessions: Default::default(),
            write_handle,
        });
        self.sort_conns();
        self.resolve_unsatisfied_sessions()
    }

    pub fn session_ended_or_errored(&mut self, session_id: SessionId) {
        self.remove_session(&session_id);

        self.conns
            .sort_by(|l, r| l.sessions.len().cmp(&r.sessions.len()));
    }

    #[must_use]
    pub fn close_conn(&mut self, conn_id: ConnId) -> Vec<Action> {
        if let Some(idx) = self.conns.iter().position(|x| x.id == conn_id) {
            let sessions = self.conns.swap_remove(idx).sessions;
            for session_id in sessions {
                if let Some(session_conns) = self.sessions.get_mut(&session_id) {
                    self.unsatified_sessions.push(session_id);
                    session_conns.remove(&conn_id);
                }
            }
            self.sort_conns();
            return self.resolve_unsatisfied_sessions();
        }

        Default::default()
    }

    #[must_use]
    pub fn purge_errored_conn(&mut self, conn_id: ConnId) -> Vec<Action> {
        let mut result = Vec::new();

        if let Some(conn_idx) = self.conns.iter().position(|x| x.id == conn_id) {
            let sessions = self.conns.remove(conn_idx).sessions;
            for session_id in sessions {
                self.remove_session(&session_id);
                result.push(Kill(session_id));
            }
        }

        result
    }

    // does not maker sure conns' ordered
    fn remove_session(&mut self, session_id: &SessionId) {
        let _ = self.sessions.remove(&session_id);

        self.conns.iter_mut().for_each(|conn| {
            conn.sessions.remove(&session_id);
        });

        if let Some(session_idx) = self
            .unsatified_sessions
            .iter()
            .position(|x| x == session_id)
        {
            self.unsatified_sessions.swap_remove(session_idx);
        }
    }

    #[must_use]
    fn resolve_unsatisfied_sessions(&mut self) -> Vec<Action> {
        let mut result = Vec::new();

        // tracing::trace!(
        //     unsatified_sessions=?self.unsatified_sessions,
        //     conns=?self.conns.iter().map(|x| x.id).collect::<Vec<_>>(),
        //     "resolve unsatified sessions"
        // );

        let mut idx = 0;
        while idx < self.unsatified_sessions.len() {
            let session_id = self.unsatified_sessions[idx];
            let session_conns = self.sessions.get_mut(&session_id).unwrap();
            let diff = self.expected_conn_per_session - session_conns.len();

            self.conns
                .iter_mut()
                .filter(|conn| !conn.sessions.contains(&session_id))
                .take(diff)
                .for_each(|conn| {
                    conn.sessions.insert(session_id);
                    session_conns.insert(conn.id);
                    result.push(Assign(session_id, conn.id, conn.write_handle.clone()));
                });

            if session_conns.len() == self.expected_conn_per_session {
                self.unsatified_sessions.swap_remove(idx);
            } else {
                idx += 1;
            }

            self.sort_conns();
        }

        // tracing::trace!("result: {:?}", result);

        return result;
    }

    fn sort_conns(&mut self) {
        self.conns
            .sort_by(|l, r| l.sessions.len().cmp(&r.sessions.len()));
    }
}

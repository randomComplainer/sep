use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;
use tokio::sync::SetOnce;

mod evict_queue;

pub use evict_queue::EvictQueue;
use tokio::time::Instant;

#[derive(Clone)]
pub struct Cache {
    shared: Arc<Mutex<Shared>>,
}

impl Cache {
    pub fn new(evict_queue: EvictQueue) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            entries_map: HashMap::with_capacity(
                evict_queue.bucket_initial_capacity * evict_queue.buckets.len() / 2,
            ),
            evict_queue,
        }));

        Self { shared }
    }

    pub fn query(
        &self,
        domain: String,
        port: u16,
        now: Instant,
    ) -> impl Future<Output = Result<Arc<Vec<SocketAddr>>, ()>> + Send {
        async move {
            // be very careful here
            // drop this lock before awaiting
            let mut lock = self.shared.lock().await;

            for key in lock.evict_queue.evict(now) {
                lock.entries_map.remove(&key);
            }

            // also be careful to not move the allocation of domain string parameter
            // if it's already in the map
            let query_key = (domain, port);
            if let Some(entry) = lock.entries_map.get(&query_key) {
                match entry {
                    EntryState::Querying(set_once) => {
                        let set_once = set_once.clone();
                        drop(lock);
                        set_once.wait().await.clone()
                    }
                    EntryState::Cached(socket_addrs) => {
                        let result = socket_addrs.clone();
                        drop(lock);
                        Ok(result)
                    }
                }
            } else {
                let set_once = SetOnce::new();
                let key = query_key.into();
                lock.entries_map
                    .insert(Arc::clone(&key), EntryState::Querying(set_once.clone()));
                drop(lock);

                // TODO: inject lookup_host function
                let r = tokio::net::lookup_host((key.0.as_str(), port))
                    .await
                    .map(|addrs| Arc::new(addrs.collect::<Vec<_>>()))
                    .map_err(|err| {
                        tracing::warn!(?err, "failed to resolve domain");
                        ()
                    });

                let mut lock = self.shared.lock().await;
                if let Ok(addrs) = &r {
                    // let domain = domain.into();
                    lock.entries_map
                        .insert(Arc::clone(&key), EntryState::Cached(addrs.clone()));
                    lock.evict_queue.insert(key);
                } else {
                    lock.entries_map.remove(&key);
                }

                set_once.set(r.clone()).unwrap();
                drop(lock);
                r
            }
        }
    }
}

enum EntryState {
    Querying(SetOnce<Result<Arc<Vec<SocketAddr>>, ()>>),
    Cached(Arc<Vec<SocketAddr>>),
}

struct Shared {
    entries_map: HashMap<Arc<(String, u16)>, EntryState>,
    evict_queue: evict_queue::EvictQueue,
}

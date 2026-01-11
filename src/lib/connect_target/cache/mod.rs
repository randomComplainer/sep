use std::cmp::Ordering;
use std::pin::Pin;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;
use tokio::sync::SetOnce;

mod evict_queue;

pub use evict_queue::EvictQueue;
use tokio::time::Instant;

type BoxPin<T> = Pin<Box<T>>;

// AKA async fn(domain: &str, port: u16) -> Result<Vec<SocketAddr>, ()>
type LookupFn = Box<
    dyn for<'a> Fn(
            &'a str,
            u16,
        )
            -> BoxPin<dyn Future<Output = Result<Vec<SocketAddr>, ()>> + Send + Sync + 'a>
        + Sync
        + Send
        + 'static,
>;

#[derive(Clone)]
pub struct Cache {
    // There are absolutely no need to have 2 distinct Arcs,
    // but it's just for the sake of simplicity
    shared: Arc<Mutex<Shared>>,
    lookup_fn: Arc<LookupFn>,
}

impl Cache {
    pub fn new(evict_queue: EvictQueue, lookup_fn: LookupFn) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            entries_map: HashMap::with_capacity(
                evict_queue.bucket_initial_capacity * evict_queue.buckets.len() / 2,
            ),
            evict_queue,
        }));

        Self {
            shared,
            lookup_fn: Arc::new(lookup_fn),
        }
    }

    pub fn query(
        &self,
        domain: String,
        port: u16,
        now: Instant,
    ) -> impl Future<Output = Result<Arc<Vec<SocketAddr>>, ()>> + Send {
        async move {
            // TDOD: could distinct locks for evict_queue and entries be better?

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

                let lookup_reslt = (self.lookup_fn)(key.0.as_str(), port)
                    .await
                    .map(|mut addrs| {
                        addrs.sort_by(|l, r| match (l.is_ipv6(), r.is_ipv6()) {
                            (true, true) => Ordering::Equal,
                            (false, false) => Ordering::Equal,
                            (true, false) => Ordering::Less,
                            (false, true) => Ordering::Greater,
                        });
                        addrs
                    })
                    .map(Arc::new);

                let mut lock = self.shared.lock().await;
                if let Ok(addrs) = &lookup_reslt {
                    lock.entries_map
                        .insert(Arc::clone(&key), EntryState::Cached(addrs.clone()));
                    lock.evict_queue.insert(key);
                } else {
                    lock.entries_map.remove(&key);
                }

                set_once.set(lookup_reslt.clone()).unwrap();
                drop(lock);
                lookup_reslt
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

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use super::*;

    fn make_mock(
        entries: Arc<Mutex<HashMap<(String, u16), Result<Vec<SocketAddr>, ()>>>>,
    ) -> LookupFn {
        Box::new(move |domain, port| {
            let entries = entries.clone();
            Box::pin(async move {
                let mut lock = entries.lock().await;
                match lock.remove(&(domain.to_string(), port)) {
                    Some(r) => r,
                    None => {
                        panic!("unexpected dns request to {}:{}", domain, port);
                    }
                }
            })
        })
    }

    #[tokio::test]
    async fn happy_path() {
        let start_time = Instant::now();
        let delta = Duration::from_mins(1) + Duration::from_secs(1);
        let entries = Arc::new(Mutex::new(HashMap::new()));

        let cache = Cache::new(
            EvictQueue::new(Duration::from_mins(1), 3, start_time, 8),
            make_mock(Arc::clone(&entries)),
        );

        let domain = "example.com";
        let port = 80;

        let mut lock = entries.lock().await;
        lock.insert(
            (domain.to_string(), port),
            Ok(vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                180,
            )]),
        );
        drop(lock);

        let addrs = cache.query(domain.into(), port, start_time).await.unwrap();
        assert_eq!(1, addrs.len());
        assert_eq!("127.0.0.1:180", addrs[0].to_string());

        // should not yet be evicted
        let time = start_time + delta + delta;
        let addrs = cache.query(domain.into(), port, time).await.unwrap();
        assert_eq!(1, addrs.len());
        assert_eq!("127.0.0.1:180", addrs[0].to_string());

        // should be evicted
        let time = time + delta;
        let e = tokio::spawn(async move {
            let addrs = cache.query(domain.into(), port, time).await.unwrap();
            unreachable!("should not be reached, but got {:?}", addrs);
        })
        .await
        .unwrap_err();
        assert!(e.is_panic());
    }
}

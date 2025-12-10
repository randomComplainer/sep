use std::{sync::Arc, time::Duration};

use tokio::time::Instant;

pub struct EvictQueue {
    pub(super) buckets: Box<[Vec<Arc<(String, u16)>>]>,
    evict_interval: Duration,
    last_evict: Instant,
    head: usize,
    pub(super) bucket_initial_capacity: usize,
}

impl EvictQueue {
    pub fn new(
        evict_interval: Duration,
        capacity: usize,
        start_time: Instant,
        bucket_initial_capacity: usize,
    ) -> Self {
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(Vec::with_capacity(bucket_initial_capacity));
        }

        Self {
            buckets: buckets.into_boxed_slice(),
            evict_interval,
            last_evict: start_time,
            head: 0,
            bucket_initial_capacity,
        }
    }

    fn next_bucket_idx(&self, idx: usize) -> usize {
        let v = idx + 1;
        if v >= self.buckets.len() { 0 } else { v }
    }

    pub fn insert(&mut self, entry: Arc<(String, u16)>) {
        self.buckets[self.head].push(entry);
    }

    pub fn evict(&mut self, now: Instant) -> impl Iterator<Item = Arc<(String, u16)>> + 'static {
        let capacity = self.buckets.len();
        let mut result = Vec::with_capacity(capacity);
        let mut counter = 0;
        let mut time_cursor = self.last_evict;

        while now - time_cursor > self.evict_interval {
            let next_head = self.next_bucket_idx(self.head);
            result.push(Vec::with_capacity(self.bucket_initial_capacity));
            std::mem::swap(&mut self.buckets[next_head], &mut result[counter]);
            self.head = next_head;

            time_cursor += self.evict_interval;
            counter += 1;
            // no point doing more rounds then capacity
            if counter == capacity {
                break;
            }
        }

        // it's not rocket science, Okay?
        if counter > 0 {
            self.last_evict = now;
        }

        result.into_iter().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path() {
        let start_time = Instant::now();
        let delta = Duration::from_mins(1) + Duration::from_secs(1);

        let mut queue = EvictQueue::new(Duration::from_mins(1), 3, start_time, 8);

        queue.insert(("a".into(), 0).into());
        queue.insert(("b".into(), 0).into());
        let new_time = start_time + delta;
        let evicted = queue.evict(new_time);
        assert_eq!(0, evicted.count());

        queue.insert(("c".into(), 0).into());
        let new_time = new_time + delta;
        let evicted = queue.evict(new_time);
        assert_eq!(0, evicted.count());

        queue.insert(("d".into(), 0).into());
        let new_time = new_time + delta;
        let evicted = queue.evict(new_time).collect::<Vec<_>>();
        assert_eq!(2, evicted.len());
        assert_eq!("a", evicted[0].0);
        assert_eq!("b", evicted[1].0);

        let new_time = new_time + delta;
        let evicted = queue.evict(new_time);
        let evicted = evicted.collect::<Vec<_>>();
        assert_eq!(1, evicted.len());
        assert_eq!("c", evicted[0].0);
    }

    #[test]
    fn evict_accross_multiple_intervals() {
        let start_time = Instant::now();
        let delta = Duration::from_mins(1) + Duration::from_secs(1);

        let mut queue = EvictQueue::new(Duration::from_mins(1), 3, start_time, 8);

        queue.insert(("a".into(), 0).into());
        let new_time = start_time + delta;
        let _ = queue.evict(new_time);

        queue.insert(("b".into(), 0).into());
        let new_time = new_time + delta;
        let _ = queue.evict(new_time);

        queue.insert(("c".into(), 0).into());
        let new_time = new_time + delta + delta;
        let evicted = queue.evict(new_time).collect::<Vec<_>>();
        assert_eq!(2, evicted.len());
        assert_eq!("a", evicted[0].0);
        assert_eq!("b", evicted[1].0);
    }
}

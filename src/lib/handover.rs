use std::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::FutureExt;
use tokio::sync::{Notify, watch};

// simple 1-to-1 channel
// both sender and receiver cannot be cloned
// no buffering, which means send() will be pending until item been consumed via recv()
// in case of receiver dropped, send() will return Err(item), so no data loss

pub fn channel<T>() -> (Sender<T>, Receiver<T>)
where
    T: Send,
{
    let shared = Arc::new(Shared {
        sender_drop_notify: Notify::new(),
        sender_dropped: AtomicBool::new(false),
        receiver_drop_notify: Notify::new(),
        receiver_dropped: AtomicBool::new(false),
        item: UnsafeCell::new(None),
    });

    let (item_watch_tx, item_watch_rx) = watch::channel(false);
    item_watch_tx.send(false).unwrap();

    let sender = Sender {
        shared: shared.clone(),
        item_watch_rx: item_watch_rx.clone(),
        item_watch_tx: item_watch_tx.clone(),
    };
    let receiver = Receiver {
        shared,
        item_watch_rx,
        item_watch_tx,
    };

    (sender, receiver)
}

pub struct Sender<T>
where
    T: Send,
{
    shared: Arc<Shared<T>>,
    item_watch_rx: watch::Receiver<bool>,
    item_watch_tx: watch::Sender<bool>,
}

impl<T> Sender<T>
where
    T: Send,
{
    // Not safe to cancel, YOU WILL DEADLOCK
    // cause item_watch is not in correct state(empty=false)
    // and you lose the item anyway
    pub async fn send(&mut self, t: T) -> Result<(), T> {
        if self.shared.receiver_dropped.load(Ordering::SeqCst) {
            return Err(t);
        }

        unsafe {
            debug_assert!(!*self.item_watch_rx.borrow());
            debug_assert!((&*self.shared.item.get()).is_none());
            (&mut *self.shared.item.get()).replace(t);
        }

        let _ = self.item_watch_tx.send_replace(true);

        match futures::future::select(
            self.shared.receiver_drop_notify.notified().boxed(),
            self.item_watch_rx.wait_for(|x| !x).boxed(),
        )
        .await
        {
            // receiver droped
            futures::future::Either::Left(x) => {
                drop(x);
                unsafe {
                    debug_assert!((&*self.shared.item.get()).is_some());
                    let _ = self.item_watch_tx.send_replace(false);
                    Err((&mut *self.shared.item.get()).take().unwrap())
                }
            }
            // consumed
            futures::future::Either::Right(_) => Ok(()),
        }
    }
}

impl<T> Drop for Sender<T>
where
    T: Send,
{
    fn drop(&mut self) {
        self.shared.sender_dropped.store(true, Ordering::SeqCst);
        self.shared.sender_drop_notify.notify_one();
    }
}

pub struct Receiver<T>
where
    T: Send,
{
    shared: Arc<Shared<T>>,
    item_watch_rx: watch::Receiver<bool>,
    item_watch_tx: watch::Sender<bool>,
}

impl<T> Receiver<T>
where
    T: Send,
{
    pub async fn recv(&mut self) -> Option<T> {
        if self.shared.sender_dropped.load(Ordering::SeqCst) {
            return None;
        }

        match futures::future::select(
            self.shared.sender_drop_notify.notified().boxed(),
            self.item_watch_rx.wait_for(|x| *x).boxed(),
        )
        .await
        {
            futures::future::Either::Left(_) => None,
            futures::future::Either::Right(x) => {
                // drop borrowed item form wait_for
                drop(x);
                unsafe {
                    debug_assert!((&*self.shared.item.get()).is_some());
                    let result = (&mut *self.shared.item.get()).take();
                    let _ = self.item_watch_tx.send_replace(false);
                    result
                }
            }
        }
    }
}

impl<T> Drop for Receiver<T>
where
    T: Send,
{
    fn drop(&mut self) {
        self.shared.receiver_drop_notify.notify_one();
        self.shared.receiver_dropped.store(true, Ordering::SeqCst);
    }
}

struct Shared<T> {
    pub sender_drop_notify: Notify,
    pub sender_dropped: AtomicBool,
    pub receiver_drop_notify: Notify,
    pub receiver_dropped: AtomicBool,
    pub item: UnsafeCell<Option<T>>,
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::task::Poll;

    use super::*;

    #[test]
    fn send_blocking() {
        let (mut tx, mut rx) = channel::<u8>();

        let mut sending = tokio_test::task::spawn(tx.send(1));
        assert!(sending.poll().is_pending());

        let mut receiving = tokio_test::task::spawn(rx.recv());
        assert_matches!(receiving.poll(), Poll::Ready(Some(1)));

        assert!(sending.poll().is_ready());
    }

    #[test]
    fn recv_end_of_stream() {
        let (tx, mut rx) = channel::<u8>();
        let mut receiving = tokio_test::task::spawn(rx.recv());
        assert_eq!(receiving.poll().is_pending(), true);
        drop(tx);
        assert_matches!(receiving.poll(), Poll::Ready(None));
    }

    #[test]
    fn send_closed() {
        let (mut tx, rx) = channel::<u8>();
        drop(rx);
        assert_matches!(
            tokio_test::task::spawn(tx.send(1)).poll(),
            Poll::Ready(Err(1))
        );
    }

    #[test]
    fn recv_cancel_safty() {
        let (mut tx, mut rx) = channel::<u8>();
        let mut receiving = tokio_test::task::spawn(rx.recv());
        let mut sending = tokio_test::task::spawn(tx.send(1));

        assert!(receiving.poll().is_pending());
        assert!(sending.poll().is_pending());
        drop(receiving);

        let mut receiving = tokio_test::task::spawn(rx.recv());
        assert_matches!(receiving.poll(), Poll::Ready(Some(1)));
        assert_matches!(sending.poll(), Poll::Ready(Ok(())));
    }
}

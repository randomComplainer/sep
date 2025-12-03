use std::{cell::UnsafeCell, ptr::NonNull, sync::Arc};

use futures::FutureExt;
use tokio::sync::watch;

// simple 1-to-1 channel
// both sender and receiver cannot be cloned
// no buffering, which means send() will be pending until item been consumed via recv()
// in case of receiver dropped, send() will return Err(item), so no data loss

pub fn channel<T>() -> (Sender<T>, Receiver<T>)
where
    T: Send,
{
    let shared = Arc::new(Shared {
        item: UnsafeCell::new(None),
    });

    let (item_watch_tx, item_watch_rx) = watch::channel(false);

    let (sender_closed_tx, sender_closed_rx) = watch::channel(false);
    let (receiver_closed_tx, receiver_closed_rx) = watch::channel(false);

    let sender = Sender {
        shared: shared.clone(),
        item_watch_rx: item_watch_rx.clone(),
        item_watch_tx: item_watch_tx.clone(),
        sender_closed_tx,
        receiver_closed_rx,
    };
    let receiver = Receiver {
        shared,
        item_watch_rx,
        item_watch_tx,
        receiver_closed_tx,
        sender_closed_rx,
    };

    (sender, receiver)
}

pub trait Channel<T: Send> {
    fn shared(&self) -> &Arc<Shared<T>>;
}

pub trait ChannelExt<T: Send> {
    fn is_connected_to<Other: Channel<T>>(&self, other: &Other) -> bool;
    fn create_channel_ref(&self) -> ChannelRef<T>;
}

impl<T: Send, C: Channel<T>> ChannelExt<T> for C {
    fn is_connected_to<Other: Channel<T>>(&self, other: &Other) -> bool {
        Arc::ptr_eq(&self.shared(), &other.shared())
    }

    fn create_channel_ref(&self) -> ChannelRef<T> {
        ChannelRef::new(self.shared().clone())
    }
}

pub struct Sender<T>
where
    T: Send,
{
    shared: Arc<Shared<T>>,
    item_watch_rx: watch::Receiver<bool>,
    item_watch_tx: watch::Sender<bool>,
    sender_closed_tx: watch::Sender<bool>,
    receiver_closed_rx: watch::Receiver<bool>,
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}

impl<T> Sender<T>
where
    T: Send,
{
    // Err(Some) when receiver closed without consuming item
    // Err(None) when receiver closed and item consumed
    // Not safe to cancel, YOU WILL DEADLOCK
    // cause item_watch is not in correct state(empty=false)
    // and you lose the item anyway
    pub async fn send(&mut self, t: T) -> Result<(), Option<T>> {
        if *self.receiver_closed_rx.borrow() {
            return Err(Some(t));
        }

        unsafe {
            assert!(!*self.item_watch_rx.borrow());
            assert!((&*self.shared.item.get()).is_none());
            (*self.shared.item.get()) = NonNull::new(Box::leak(Box::new(t)));
        }

        let _ = self.item_watch_tx.send_replace(true);

        match futures::future::select(
            self.receiver_closed_rx.wait_for(|x| *x).boxed(),
            self.item_watch_rx.wait_for(|x| !x).boxed(),
        )
        .await
        {
            // receiver droped
            futures::future::Either::Left(x) => {
                drop(x);
                let _ = self.item_watch_tx.send_replace(false);

                unsafe { Err((*self.shared.item.get()).map(|p| *Box::from_raw(p.as_ptr()))) }
            }
            // consumed
            futures::future::Either::Right(_) => Ok(()),
        }
    }

    pub fn close(&self) {
        self.sender_closed_tx.send_replace(true);
    }

    pub fn is_connected_to(&self, receiver: &Receiver<T>) -> bool {
        Arc::ptr_eq(&self.shared, &receiver.shared)
    }
}

impl<T> Drop for Sender<T>
where
    T: Send,
{
    fn drop(&mut self) {
        self.close();
    }
}

impl<T: Send> Channel<T> for Sender<T> {
    fn shared(&self) -> &Arc<Shared<T>> {
        &self.shared
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

pub struct Receiver<T>
where
    T: Send,
{
    shared: Arc<Shared<T>>,
    item_watch_rx: watch::Receiver<bool>,
    item_watch_tx: watch::Sender<bool>,
    receiver_closed_tx: watch::Sender<bool>,
    sender_closed_rx: watch::Receiver<bool>,
}

impl<T> Receiver<T>
where
    T: Send,
{
    pub async fn recv(&mut self) -> Option<T> {
        if *self.sender_closed_rx.borrow() {
            return None;
        }

        match futures::future::select(
            self.sender_closed_rx.wait_for(|x| *x).boxed(),
            self.item_watch_rx.wait_for(|x| *x).boxed(),
        )
        .await
        {
            futures::future::Either::Left(_) => None,
            futures::future::Either::Right(x) => {
                // drop borrowed item form wait_for
                drop(x);
                unsafe {
                    assert!((&*self.shared.item.get()).is_some());
                    let result = (*self.shared.item.get())
                        .take()
                        .map(|p| *Box::from_raw(p.as_ptr()));
                    let _ = self.item_watch_tx.send_replace(false);
                    result
                }
            }
        }
    }

    pub fn close(&self) {
        self.receiver_closed_tx.send_replace(true);
    }

    pub fn is_connected_to(&self, sender: &Sender<T>) -> bool {
        sender.is_connected_to(self)
    }
}

impl<T> Drop for Receiver<T>
where
    T: Send,
{
    fn drop(&mut self) {
        self.close();
    }
}

impl<T: Send> Channel<T> for Receiver<T> {
    fn shared(&self) -> &Arc<Shared<T>> {
        &self.shared
    }
}

pub struct Shared<T> {
    pub item: UnsafeCell<Option<NonNull<T>>>,
}

pub struct ChannelRef<T: Send> {
    shared: Arc<Shared<T>>,
}

impl<T: Send> Clone for ChannelRef<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T: Send> ChannelRef<T> {
    pub fn new(shared: Arc<Shared<T>>) -> Self {
        Self { shared }
    }
}

unsafe impl<T: Send> Send for ChannelRef<T> {}

impl<T: Send> Channel<T> for ChannelRef<T> {
    fn shared(&self) -> &Arc<Shared<T>> {
        &self.shared
    }
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
            Poll::Ready(Err(Some(1)))
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

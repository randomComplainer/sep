use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use tokio::sync::mpsc;

pub fn pair<T>() -> (Supplier<T>, Warehose<T>) {
    let (sender, reciver) = mpsc::unbounded_channel();
    let count = Arc::new(AtomicU8::new(0));

    (
        Supplier {
            sender: sender.clone(),
            count: Arc::clone(&count),
        },
        Warehose {
            sender,
            reciver,
            count,
        },
    )
}

pub struct Warehose<T> {
    sender: mpsc::UnboundedSender<T>,
    reciver: mpsc::UnboundedReceiver<T>,
    count: Arc<AtomicU8>,
}

impl<T> Warehose<T> {
    pub async fn next(&mut self) -> Option<Recyle<T>> {
        let inner = match self.reciver.recv().await {
            Some(x) => x,
            None => return None,
        };

        Some(Recyle {
            sender: self.sender.clone(),
            inner: Some(inner),
        })
    }

    pub fn register(&mut self, item: T) -> Recyle<T> {
        self.count.fetch_and(1, Ordering::Relaxed);

        Recyle {
            inner: Some(item),
            sender: self.sender.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Supplier<T> {
    sender: mpsc::UnboundedSender<T>,
    count: Arc<AtomicU8>,
}

impl<T> Supplier<T> {
    pub fn supply(&mut self, item: T) -> Result<(), mpsc::error::SendError<T>> {
        let result = self.sender.send(item);

        if result.is_ok() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    pub fn get_count(&self) -> u8 {
        self.count.load(Ordering::Relaxed)
    }
}

pub struct Recyle<T> {
    inner: Option<T>,
    // inner: Option<NonNull,
    sender: mpsc::UnboundedSender<T>,
}

impl<T> Drop for Recyle<T> {
    fn drop(&mut self) {
        let inner = self.inner.take().unwrap();

        let _ = self.sender.send(inner);
    }
}

impl<T> PartialEq for Recyle<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> Eq for Recyle<T> where T: Eq {}

impl<T> AsRef<T> for Recyle<T> {
    fn as_ref(&self) -> &T {
        self.inner.as_ref().unwrap()
    }
}

impl<T> AsMut<T> for Recyle<T> {
    fn as_mut(&mut self) -> &mut T {
        self.inner.as_mut().unwrap()
    }
}

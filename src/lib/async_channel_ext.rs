use futures::TryFutureExt;

pub trait Sender<T>
where
    Self: Unpin + Send + Clone + 'static,
    T: Unpin + Send + 'static,
{
    fn send(&self, item: T) -> impl Future<Output = Result<(), ()>> + Send;
}

impl<T> Sender<T> for async_channel::Sender<T>
where
    T: Unpin + Send + 'static,
{
    fn send(&self, item: T) -> impl Future<Output = Result<(), ()>> + Send {
        async_channel::Sender::<T>::send(self, item).map_err(|_| ())
    }
}

#[derive(Clone)]
pub struct With<Inner, F> {
    inner: Inner,
    f: F,
}

impl<T, Inner, F, R> Sender<T> for With<Inner, F>
where
    T: Unpin + Send + 'static,
    R: Unpin + Send + 'static,
    Inner: Sender<R> + Unpin + Send + Clone + 'static,
    F: (Fn(T) -> R) + Clone + Unpin + Send + 'static,
{
    fn send(&self, item: T) -> impl Future<Output = Result<(), ()>> + Send {
        let r = (self.f.clone())(item);
        self.inner.send(r)
    }
}

pub trait SenderExt<T>
where
    Self: Sized,
{
    fn with<F>(self, f: F) -> With<Self, F>;
}

impl<T, Orig> SenderExt<T> for Orig
where
    T: Unpin + Send + 'static,
    Orig: Sender<T> + Unpin + Send + Clone + 'static,
{
    fn with<F>(self, f: F) -> With<Self, F> {
        With { inner: self, f }
    }
}

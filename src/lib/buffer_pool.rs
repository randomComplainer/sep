use bytes::BytesMut;
use tokio::sync::{mpsc, oneshot};

pub struct Config {
    pub buf_size: u16,
    pub pool_size: usize,
}

#[derive(Clone)]
pub struct BufferPool {
    req_tx: mpsc::UnboundedSender<oneshot::Sender<Recycle<BytesMut>>>,
}

impl BufferPool {
    pub fn create(config: Config) -> (impl Future<Output = ()>, BufferPool) {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel::<oneshot::Sender<Recycle<BytesMut>>>();
        let (recyle_tx, mut recyle_rx) = mpsc::unbounded_channel::<BytesMut>();

        for _ in 0..config.pool_size {
            let buf = BytesMut::with_capacity(config.buf_size as usize);
            recyle_tx.send(buf).unwrap();
        }

        let fut = async move {
            loop {
                let req = match req_rx.recv().await {
                    Some(x) => x,
                    None => return,
                };

                let buf = match recyle_rx.recv().await {
                    Some(mut buf) => {
                        buf.clear();
                        buf
                    }
                    None => return,
                };

                let _ = req.send(Recycle::new(buf, recyle_tx.clone()));
            }
        };

        (fut, BufferPool { req_tx })
    }

    pub async fn request_one(&self) -> Result<Recycle<BytesMut>, ()> {
        let (buf_tx, buf_rx) = oneshot::channel();

        if let Err(_) = self.req_tx.send(buf_tx) {
            return Err(());
        }

        buf_rx.await.map_err(|_| ())
    }
}

pub struct Recycle<T> {
    inner: Option<T>,
    sender: mpsc::UnboundedSender<T>,
}

impl<T> Recycle<T> {
    pub fn new(inner: T, sender: mpsc::UnboundedSender<T>) -> Self {
        Self {
            inner: Some(inner),
            sender,
        }
    }

    pub fn ref_inner(&self) -> &T {
        self.inner.as_ref().unwrap()
    }
}

impl<T> Drop for Recycle<T> {
    fn drop(&mut self) {
        let inner = self.inner.take().unwrap();

        let _ = self.sender.send(inner);
    }
}

impl<T> PartialEq for Recycle<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> Eq for Recycle<T> where T: Eq {}

impl<T> AsRef<T> for Recycle<T> {
    fn as_ref(&self) -> &T {
        self.inner.as_ref().unwrap()
    }
}

impl<T> AsMut<T> for Recycle<T> {
    fn as_mut(&mut self) -> &mut T {
        self.inner.as_mut().unwrap()
    }
}

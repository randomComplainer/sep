use std::future::Future;
use std::pin::Pin;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::stream::FuturesUnordered;

use drop_guard::DropGuard;

type Fut<E> = Pin<Box<dyn futures::Future<Output = Result<(), E>>>>;

#[derive(Clone)]
pub struct ScopeHandle<E> {
    task_tx: mpsc::Sender<Fut<E>>,
}

impl<E> ScopeHandle<E> {
    pub fn new(task_tx: mpsc::Sender<Fut<E>>) -> Self {
        Self { task_tx }
    }
}

impl<E> ScopeHandle<E> {
    pub async fn run_async<F>(&mut self, future: F) -> Result<(), mpsc::SendError>
    where
        F: Future<Output = Result<(), E>> + 'static,
    {
        self.task_tx.send(Box::pin(future)).await
    }

    pub async fn spawn(
        &mut self,
        future: impl Future<Output = Result<(), E>> + Send + 'static,
    ) -> Result<(), mpsc::SendError>
    where
        E: Send + 'static,
    {
        self.task_tx.send(Box::pin(DropGuard::new(future))).await
    }
}

async fn main_loop<E>(mut task_rx: mpsc::Receiver<Fut<E>>) -> E {
    let mut list: FuturesUnordered<Fut<E>> = FuturesUnordered::new();
    loop {
        match futures::future::select(task_rx.next(), list.next()).await {
            future::Either::Left((task, x)) => {
                drop(x);
                list.push(task.unwrap());
            }
            future::Either::Right((result_opt, x)) => {
                drop(x);
                match result_opt {
                    Some(result) => match result {
                        Ok(()) => continue,
                        Err(e) => return e,
                    },
                    // FuturesUnordered::next() returns None when the
                    // internal queue is empty
                    // but we want to wait for more tasks to be pushed
                    None => {
                        list.push(task_rx.next().await.unwrap());
                    }
                }
            }
        }
    }
}

pub fn scope<E>() -> (ScopeHandle<E>, impl Future<Output = E>) {
    let (task_tx, task_rx) = mpsc::channel(1);
    (ScopeHandle::new(task_tx), main_loop(task_rx))
}

mod drop_guard {
    use std::future::Future;
    use std::pin::Pin;

    use tokio::task::{AbortHandle, JoinHandle};

    // wrap around a Future
    // original Future is run with tokio::spawn
    // when DropGuard is dropped, the original Future is aborted
    // DropGuard implements Future, polling behavior is forwarded to the original Future
    pub struct DropGuard<E>(JoinHandle<Result<(), E>>, AbortHandle);

    impl<E> Future for DropGuard<E>
    where
        E: Send + 'static,
    {
        type Output = Result<(), E>;

        fn poll(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            match unsafe { self.map_unchecked_mut(|s| &mut s.0).poll(cx) } {
                std::task::Poll::Pending => std::task::Poll::Pending,
                std::task::Poll::Ready(join_result) => match join_result {
                    Ok(task_result) => std::task::Poll::Ready(task_result),
                    Err(join_err) => {
                        if join_err.is_cancelled() {
                            std::task::Poll::Ready(Ok(()))
                        } else {
                            std::panic::resume_unwind(join_err.into_panic());
                        }
                    }
                },
            }
        }
    }

    impl<E> DropGuard<E>
    where
        E: Send + 'static,
    {
        pub fn new<RawF>(future: RawF) -> Self
        where
            RawF: Future<Output = Result<(), E>> + Send + 'static,
        {
            let task = tokio::spawn(future);
            let abort_handle = task.abort_handle();
            Self(task, abort_handle)
        }
    }

    impl<E> Drop for DropGuard<E> {
        fn drop(&mut self) {
            self.1.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicBool};

    use super::*;

    #[test]
    fn initial_state() {
        let (handle, task) = scope::<usize>();
        let mut task = tokio_test::task::spawn(task);
        assert_eq!(std::task::Poll::Pending, task.poll());
        drop(handle);
    }

    #[tokio::test]
    async fn send_err() {
        let (mut handle, task) = scope::<usize>();
        handle.run_async(async move { Err(3) }).await.unwrap();
        assert_eq!(3, task.await);
    }

    #[test]
    fn send_ok() {
        let sied_effect = Arc::new(AtomicBool::new(false));
        let (mut handle, task) = scope::<usize>();
        let mut task = tokio_test::task::spawn(task);

        assert_eq!(
            std::task::Poll::Ready(Ok(())),
            tokio_test::task::spawn(handle.run_async({
                let sied_effect = sied_effect.clone();
                async move {
                    sied_effect.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            }))
            .poll(),
        );

        assert_eq!(std::task::Poll::Pending, task.poll());

        assert!(sied_effect.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn async_spawn() {
        let (mut handle, scpoe_task) = scope::<usize>();

        let lock = Arc::new(tokio::sync::Notify::new());
        let task = {
            let lock = lock.clone();
            async move {
                lock.notify_one();
                Err::<(), usize>(3)
            }
        };

        handle.spawn(task).await.unwrap();

        assert_eq!((), lock.notified().await);
        assert_eq!(3, scpoe_task.await);
    }

    #[tokio::test]
    async fn cancel_on_drop() {
        let counter = Arc::new(());
        let (mut handle, scpoe_task) = scope::<usize>();

        let task = {
            let counter = counter.clone();
            async move {
                std::future::pending::<()>().await;
                dbg!(counter);
                Err::<(), usize>(3)
            }
        };

        handle.spawn(task).await.unwrap();

        let mut scpoe_task = tokio_test::task::spawn(scpoe_task);
        assert_eq!(std::task::Poll::Pending, scpoe_task.poll());
        assert_eq!(2, Arc::strong_count(&counter));
        drop(scpoe_task);
        drop(handle);
        tokio::task::yield_now().await;
        assert_eq!(1, Arc::strong_count(&counter));
    }
}


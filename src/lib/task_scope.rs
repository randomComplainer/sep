use std::future::Future;
use std::pin::Pin;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::stream::FuturesUnordered;

use drop_guard::DropGuard;

// TODO: revisit these (Sync + Send + 'static) bounds

type Fut<E> = Pin<Box<dyn futures::Future<Output = Result<(), E>> + Send + 'static>>;

#[derive(Debug)]
pub struct ScopeHandle<E> {
    task_tx: mpsc::Sender<Fut<E>>,
}

impl<E> Clone for ScopeHandle<E> {
    fn clone(&self) -> Self {
        Self {
            task_tx: self.task_tx.clone(),
        }
    }
}

impl<E> ScopeHandle<E> {
    pub fn new(task_tx: mpsc::Sender<Fut<E>>) -> Self {
        Self { task_tx }
    }
}

impl<E> ScopeHandle<E> {
    pub fn run_async<F>(
        &mut self,
        future: F,
    ) -> impl std::future::Future<Output = Result<(), mpsc::SendError>> + Send
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
    {
        self.task_tx.send(Box::pin(future))
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

async fn main_loop<E: Send>(mut task_rx: mpsc::Receiver<Fut<E>>) -> E {
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
                        let next_task = task_rx.next().await.unwrap();
                        list.push(next_task);
                    }
                }
            }
        }
    }
}

pub fn new_scope<E: Sync + Send>() -> (ScopeHandle<E>, impl Future<Output = E> + Send) {
    // TODO: unbounded
    let (task_tx, task_rx) = mpsc::channel(64);
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
        let (handle, task) = new_scope::<usize>();
        let mut task = tokio_test::task::spawn(task);
        assert_eq!(std::task::Poll::Pending, task.poll());
        drop(handle);
    }

    #[tokio::test]
    async fn send_err() {
        let (mut handle, task) = new_scope::<usize>();
        handle.run_async(async move { Err(3) }).await.unwrap();
        assert_eq!(3, task.await);
    }

    #[test]
    fn send_ok() {
        let sied_effect = Arc::new(AtomicBool::new(false));
        let (mut handle, task) = new_scope::<usize>();
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
        let (mut handle, scpoe_task) = new_scope::<usize>();

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
        let (mut handle, scpoe_task) = new_scope::<usize>();

        let task = {
            let counter = counter.clone();
            async move {
                std::future::pending::<()>().await;
                drop(counter);
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

    #[tokio::test]
    async fn drop_finished_task() {
        struct SideEffectOnDrop(Arc<()>, Option<tokio::sync::oneshot::Sender<()>>);
        impl std::future::Future for SideEffectOnDrop {
            type Output = ();
            fn poll(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                std::task::Poll::Ready(())
            }
        }
        impl Drop for SideEffectOnDrop {
            fn drop(&mut self) {
                self.1.take().unwrap().send(()).unwrap();
            }
        }

        let counter = Arc::new(());
        let (task_end_tx, task_end_rx) = tokio::sync::oneshot::channel();
        let (mut handle, scpoe_task) = new_scope::<usize>();

        assert_eq!(1, Arc::strong_count(&counter));

        let task = {
            let counter = counter.clone();
            async move {
                let owner = SideEffectOnDrop(counter, Some(task_end_tx));
                Ok::<(), usize>(owner.await)
            }
        };

        assert_eq!(2, Arc::strong_count(&counter));
        handle.spawn(task).await.unwrap();
        let scpoe_task = tokio::spawn(scpoe_task);

        task_end_rx.await.unwrap();
        assert_eq!(1, Arc::strong_count(&counter));
        tokio_test::assert_pending!(tokio_test::task::spawn(scpoe_task).poll());
    }
}

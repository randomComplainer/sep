use std::future::Future;
use std::pin::Pin;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::stream::FuturesUnordered;

// TODO: revisit these (Sync + Send + 'static) bounds

type Fut<E> = Pin<Box<dyn futures::Future<Output = Result<(), E>> + Send + 'static>>;

#[derive(Debug)]
pub struct ScopeHandle<E> {
    task_tx: mpsc::UnboundedSender<Fut<E>>,
}

impl<E> Clone for ScopeHandle<E> {
    fn clone(&self) -> Self {
        Self {
            task_tx: self.task_tx.clone(),
        }
    }
}

impl<E> ScopeHandle<E> {
    pub fn new(task_tx: mpsc::UnboundedSender<Fut<E>>) -> Self {
        Self { task_tx }
    }
}

impl<E> ScopeHandle<E> {
    pub fn run_async<F>(&mut self, future: F) -> impl std::future::Future<Output = ()> + Send
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
    {
        self.task_tx.send(Box::pin(future)).map(|x| x.unwrap())
    }
}

async fn main_loop<E: Send + 'static>(
    mut task_rx: mpsc::UnboundedReceiver<Fut<E>>,
) -> Result<(), E> {
    let mut list: FuturesUnordered<Fut<E>> = FuturesUnordered::new();

    let (end_of_task_tx, end_of_task_rx) = tokio::sync::oneshot::channel::<()>();

    list.push(Box::pin(async move {
        end_of_task_rx.await.unwrap();
        Ok::<(), E>(())
    }));

    loop {
        match futures::future::select(task_rx.next(), list.next()).await {
            future::Either::Left((task, _)) => {
                match task {
                    Some(task) => {
                        list.push(task);
                    }
                    None => {
                        end_of_task_tx.send(()).unwrap();
                        break;
                    }
                };
            }
            future::Either::Right((result, _)) => {
                match result {
                    Some(result) => result?,
                    None => unreachable!(),
                };
            }
        };
    }

    while let Some(r) = list.next().await {
        r?
    }

    Ok(())
}

pub fn new_scope<E: Sync + Send + 'static>()
-> (ScopeHandle<E>, impl Future<Output = Result<(), E>> + Send) {
    let (task_tx, task_rx) = mpsc::unbounded();
    (ScopeHandle::new(task_tx), main_loop(task_rx))
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
        handle.run_async(async move { Err(3) }).await;
        assert_eq!(Err(3), task.await);
    }

    #[test]
    fn send_ok() {
        let sied_effect = Arc::new(AtomicBool::new(false));
        let (mut handle, task) = new_scope::<usize>();
        let mut task = tokio_test::task::spawn(task);

        assert_eq!(
            std::task::Poll::Ready(()),
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
    async fn usable_after_all_tasks_finished() {
        let (mut handle, task) = new_scope::<usize>();
        let (lock_tx, lock_rx) = tokio::sync::oneshot::channel::<()>();

        handle
            .run_async(async move {
                lock_tx.send(()).unwrap();
                Ok(())
            })
            .await;

        let task = match futures::future::select(lock_rx, task.boxed()).await {
            futures::future::Either::Left((_, task)) => task,
            futures::future::Either::Right(_) => {
                unreachable!()
            }
        };

        handle.run_async(std::future::ready(Err(1))).await;
        assert_eq!(Err(1), task.await);
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

        handle.run_async(task).await;

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
        handle.run_async(task).await;
        let scpoe_task = tokio::spawn(scpoe_task);

        task_end_rx.await.unwrap();
        assert_eq!(1, Arc::strong_count(&counter));
        tokio_test::assert_pending!(tokio_test::task::spawn(scpoe_task).poll());
    }
}

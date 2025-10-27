use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ReadyOr<TResult, TL, TR>
where
    TL: Future<Output = TResult>,
    TR: Future<Output = TResult>,
{
    l: Option<TL>,
    r: TR,
}

impl<TResult, TL, TR> ReadyOr<TResult, TL, TR>
where
    TL: Future<Output = TResult>,
    TR: Future<Output = TResult>,
{
    pub fn new(l: TL, r: TR) -> Self {
        Self { l: Some(l), r }
    }
}

impl<TResult, TL, TR> Future for ReadyOr<TResult, TL, TR>
where
    TL: Future<Output = TResult>,
    TR: Future<Output = TResult>,
{
    type Output = TResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(l) = this.l.as_mut() {
            let l = unsafe { Pin::new_unchecked(l) };
            match l.poll(cx) {
                Poll::Ready(r) => {
                    return Poll::Ready(r);
                }
                Poll::Pending => {
                    this.l = None;
                }
            }
        }

        let r = unsafe { Pin::new_unchecked(&mut this.r) };
        r.poll(cx)
    }
}

pub trait FutureExt<TResult>
where
    Self: Future<Output = TResult> + Sized,
{
    fn ready_or<TR>(self, r: TR) -> ReadyOr<TResult, Self, TR>
    where
        TR: Future<Output = TResult>;

    fn poll_once(self) -> impl Future<Output = Option<Self::Output>>;
}

impl<TResult, TFuture> FutureExt<TResult> for TFuture
where
    TFuture: Future<Output = TResult> + Sized,
{
    fn ready_or<TR>(self, r: TR) -> ReadyOr<TResult, Self, TR>
    where
        TR: Future<Output = TResult>,
    {
        ReadyOr::new(self, r)
    }

    fn poll_once(mut self) -> impl Future<Output = Option<Self::Output>> {
        std::future::poll_fn(move |ctx| {
            let this = unsafe { Pin::new_unchecked(&mut self) };
            match this.poll(ctx) {
                Poll::Ready(x) => Poll::Ready(Some(x)),
                Poll::Pending => Poll::Ready(None),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt as _;

    use super::*;

    #[test]
    fn l_ready() {
        let mut task =
            tokio_test::task::spawn(std::future::ready(1).ready_or(std::future::ready(2)));

        let result = tokio_test::assert_ready!(task.poll());
        assert_eq!(result, 1);
    }

    #[test]
    fn l_pending_r_ready() {
        let mut task = tokio_test::task::spawn(
            std::future::pending::<usize>().ready_or(std::future::ready(2)),
        );

        let result = tokio_test::assert_ready!(task.poll());
        assert_eq!(result, 2);
    }

    #[test]
    fn l_pending_r_pending() {
        let (r_sender, r) = tokio::sync::oneshot::channel::<usize>();
        let mut task = tokio_test::task::spawn(
            std::future::pending::<usize>().ready_or(r.map(|x| x.unwrap())),
        );

        tokio_test::assert_pending!(task.poll());
        r_sender.send(1).unwrap();
        let result = tokio_test::assert_ready!(task.poll());
        assert_eq!(result, 1);
    }
}

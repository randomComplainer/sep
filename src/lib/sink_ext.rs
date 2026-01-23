use futures::Sink;

pub trait SinkExt<Item>: Sink<Item>
where
    Self: Sized,
{
    fn with_sync<F>(self, f: F) -> with_sync::WithSync<Self, F>;

    fn inspect<F>(self, f: F) -> inspect::Inspect<Self, F>
    where
        F: Fn(&Item);
}

impl<T, Item> SinkExt<Item> for T
where
    T: Sink<Item> + Sized,
{
    fn with_sync<F>(self, f: F) -> with_sync::WithSync<Self, F> {
        with_sync::WithSync::new(self, f)
    }

    fn inspect<F>(self, f: F) -> inspect::Inspect<Self, F>
    where
        F: Fn(&Item),
    {
        inspect::Inspect::new(self, f)
    }
}

pub mod with_sync {
    use futures::Sink;
    use std::{pin::Pin, task::Context};

    // Because futures::sink::With requires the Item to implement Clone to be cloneable (why?).
    // So here we are
    #[derive(Clone)]
    pub struct WithSync<Inner, F> {
        inner: Inner,
        f: F,
    }

    impl<Inner, F> WithSync<Inner, F> {
        pub fn new(inner: Inner, f: F) -> Self {
            Self { inner, f }
        }
    }

    impl<Inner, F, InnerItem, Item> Sink<Item> for WithSync<Inner, F>
    where
        Inner: Sink<InnerItem> + Unpin,
        F: Fn(Item) -> InnerItem,
    {
        type Error = Inner::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_ready(cx)
        }

        fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
            let mapped = (self.f)(item);
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.start_send(mapped)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_flush(cx)
        }

        fn poll_close(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_close(cx)
        }
    }
}

pub mod inspect {
    use futures::Sink;
    use std::{pin::Pin, task::Context};

    #[derive(Clone)]
    pub struct Inspect<Inner, F> {
        inner: Inner,
        f: F,
    }

    impl<Inner, F> Inspect<Inner, F> {
        pub fn new(inner: Inner, f: F) -> Self {
            Self { inner, f }
        }
    }

    impl<Inner, F, Item> Sink<Item> for Inspect<Inner, F>
    where
        Inner: Sink<Item> + Unpin,
        F: Fn(&Item),
    {
        type Error = Inner::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_ready(cx)
        }

        fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
            (self.f)(&item);
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.start_send(item)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_flush(cx)
        }

        fn poll_close(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_close(cx)
        }
    }
}

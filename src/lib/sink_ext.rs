use std::{pin::Pin, task::Context};

use futures::Sink;

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
    Inner: Sink<InnerItem>,
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

pub trait SinkExt<Item>: Sink<Item>
where
    Self: Sized,
{
    fn with_sync<F>(self, f: F) -> WithSync<Self, F> {
        WithSync::new(self, f)
    }
}

impl<T, Item> SinkExt<Item> for T
where
    T: Sink<Item> + Sized,
{
    fn with_sync<F>(self, f: F) -> WithSync<Self, F> {
        WithSync::new(self, f)
    }
}

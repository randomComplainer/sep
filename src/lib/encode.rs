use bytes::{BufMut, BytesMut};

use crate::protocol::msg::session::Buf;

pub trait Encode
where
    Self: Sized,
{
    fn encode(self, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>);
}

pub trait Encoder<T> {
    fn encode(&self, item: T, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>);
}

impl<T, F> Encoder<T> for F
where
    F: Fn(T, &mut BytesMut, &mut Vec<Buf>) -> (),
{
    fn encode(&self, item: T, main_buf: &mut BytesMut, side_bufs: &mut Vec<Buf>) {
        (self)(item, main_buf, side_bufs)
    }
}

pub const fn wrap<T>(f: impl Fn(T, &mut BytesMut, &mut Vec<Buf>) -> ()) -> impl Encoder<T> {
    f
}

pub const fn u8_encoder() -> impl Encoder<u8> {
    wrap(|item, main, _side| {
        main.put_u8(item);
    })
}

pub const fn u16_encoder() -> impl Encoder<u16> {
    wrap(|item, main, _side| {
        main.put_u16(item);
    })
}

pub const fn u32_encoder() -> impl Encoder<u32> {
    wrap(|item, main, _side| {
        main.put_u32(item);
    })
}

pub const fn u64_encoder() -> impl Encoder<u64> {
    wrap(|item, main, _side| {
        main.put_u64(item);
    })
}

// pub const fn sli


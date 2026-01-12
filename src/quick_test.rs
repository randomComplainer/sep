#![feature(assert_matches)]
use std::{cmp::Ordering, collections::HashMap};
#[allow(unused_variables)]
#[allow(dead_code)]
#[allow(unused_imports)]
use std::hash::{Hash, Hasher};
use std::net::ToSocketAddrs as _;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, duplex};

use sep_lib::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let a = 123u16;
    let b = 33333u64;

    let mut bytes = [1u8; 2+8];

    bytes[0..2].copy_from_slice(&a.to_be_bytes());
    bytes[2..10].copy_from_slice(&b.to_be_bytes());

    dbg!(bytes);
}

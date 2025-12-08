#![feature(assert_matches)]
#[allow(unused_variables)]
#[allow(dead_code)]
#[allow(unused_imports)]
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, duplex};

use sep_lib::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::pause();

    let before = tokio::time::Instant::now();
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let after = tokio::time::Instant::now();

    println!("sleep took {:?}", after.duration_since(before));

    dbg!("hello");
}

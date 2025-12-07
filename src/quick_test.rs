#![feature(assert_matches)]
#[allow(unused_variables)]
#[allow(dead_code)]
#[allow(unused_imports)]
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, duplex};

use sep_lib::*;

#[tokio::main]
async fn main() {
    let mut timeout_l = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(10)));

    loop {
        tokio::select! {
            _ = timeout_l.as_mut() => {
                break;
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                println!("tick");
                continue;
            }
        }
    }
}

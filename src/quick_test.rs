#![feature(assert_matches)]
use std::collections::HashMap;
#[allow(unused_variables)]
#[allow(dead_code)]
#[allow(unused_imports)]
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::net::ToSocketAddrs as _;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, duplex};

use sep_lib::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let addrs = "[::]:1081".to_socket_addrs().unwrap().collect::<Vec<_>>();

    dbg!(addrs);
}

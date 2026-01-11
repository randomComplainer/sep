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
    let domain = std::env::args().nth(1).unwrap();

    let mut addrs = tokio::net::lookup_host(domain)
        .await
        .unwrap()
        .collect::<Vec<_>>();

    addrs.sort_by(|l, r| match (l.is_ipv6(), r.is_ipv6()) {
        (true, true) => Ordering::Equal,
        (false, false) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
    });
    dbg!(addrs);
}

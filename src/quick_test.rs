#![feature(assert_matches)]
#[allow(unused_variables)]
#[allow(dead_code)]
#[allow(unused_imports)]
use std::hash::{Hash, Hasher};
use std::net::ToSocketAddrs as _;
use std::sync::Arc;
use std::{cmp::Ordering, collections::HashMap};

use bytes::{BufMut, Bytes, BytesMut};
use sep_lib::cli_parameters::LogFormatExt as _;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, duplex};

use sep_lib::*;
use tracing::Instrument as _;
use tracing_subscriber::fmt::format::{FmtSpan, Json};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use sep_lib::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let domain = "www.reddit.com".to_string();
    let addrs = tokio::net::lookup_host((domain, 443))
        .await
        .unwrap()
        .collect::<Vec<_>>();

    for addr in addrs {
        dbg!(addr);
    }
}

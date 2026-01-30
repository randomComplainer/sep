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
    let layer = tracing_subscriber::fmt::layer()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .json()
        .with_ansi(true);

    let subscriber = tracing_subscriber::registry().with(layer);
    subscriber.init();

    // let branch = async move {
    //     tracing::info!("inside try join");
    //     Ok::<_, ()>(())
    // }
    // .instrument(tracing::info_span!("branch"));
    //
    // let fut = async move {
    //     tracing::info!("outside try join");
    //     tokio::try_join! {
    //         async move { Ok::<_, ()>(()) },
    //         branch,
    //     }
    //     .map(|_| ())
    // };
    // let fut = fut.instrument(tracing::info_span!("try join"));
    // fut.await.unwrap();

    test1().instrument(tracing::info_span!("test1")).await;
}

async fn test1() {
    let inner = async move {
        tracing::info!("hi");
        Ok::<_, ()>(())
    }
    .instrument(tracing::info_span!("inner"));

    let outer = async move {
        tracing::info!("hi");

        inner.await
    }
    .instrument(tracing::info_span!("outer"));

    let _ = outer.await;
}

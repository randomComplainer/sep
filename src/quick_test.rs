#[allow(unused_variables)]
#[allow(dead_code)]
#[allow(unused_imports)]
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, duplex};

use sep_lib::*;

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let key: Box<[u8; 32]> = vec![0u8; 32].try_into().unwrap();
    let nonce: Box<[u8; 12]> = vec![1u8; 12].try_into().unwrap();

    let (client_steam, server_stream) = duplex(8 * 1024);

    let client_agent = protocol::client_agent::Init::new(key.clone(), nonce, client_steam);
    let server_agent = protocol::server_agent::Init::new(key, server_stream);

    tokio::time::pause();
    client_agent.send_greeting(12).await.unwrap();

    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    server_agent.recv_greeting(12).await.unwrap();
}

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

    let client_agent = client_agent.send_greeting(12).await.unwrap();
    let server_agent = server_agent.recv_greeting(12).await.unwrap();

    let mut addr_buf = BytesMut::new();
    addr_buf.put_u8(1);
    addr_buf.put_slice(&std::net::Ipv4Addr::new(127, 0, 0, 1).octets());
    addr_buf.put_u16(1234);

    let mut client_agent = client_agent.send_request(&mut addr_buf).await.unwrap();

    let req = server_agent.recv_request().await.unwrap();

    dbg!(req.addr());
    dbg!(req.port());
}

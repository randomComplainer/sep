use std::net::SocketAddr;

use sep_lib::protocol;
use sep_lib::socks5;

#[tokio::main]
async fn main() {
    println!("Hello world from client");

    let listener = socks5::agent::Socks5Listener::bind("127.0.0.1:1080".parse().unwrap())
        .await
        .unwrap();

    println!("Listening...");

    while let Ok((agent, addr)) = listener.accept().await {
        tokio::spawn(async move {
            handle_proxyee(agent, addr).await;
        });
    }
}

async fn handle_proxyee<Stream>(proxyee: socks5::agent::Init<Stream>, _: SocketAddr)
where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (msg, proxyee) = proxyee.receive_greeting_message().await.unwrap();

    dbg!(msg.ver());
    dbg!(msg.methods());

    let (mut proxyee_msg, proxyee) = proxyee.send_method_selection_message(0).await.unwrap();

    dbg!(proxyee_msg.ver());
    dbg!(proxyee_msg.cmd());
    dbg!(proxyee_msg.rsv());
    dbg!(proxyee_msg.addr());

    let server = protocol::client_agent::Init::new(
        [0u8; 32].into(),
        [0u8; 12].into(),
        tokio::net::TcpStream::connect("127.0.0.1:1081")
            .await
            .unwrap(),
    );

    let server = server
        .send_greeting(protocol::get_timestamp())
        .await
        .unwrap();

    dbg!(proxyee_msg.0.ver.offset);

    let server_bound_addr = server
        .send_request(proxyee_msg.addr_bytes_mut())
        .await
        .unwrap();

    dbg!(server_bound_addr);

    let _ = proxyee.reply(&server_bound_addr).await.unwrap();
}

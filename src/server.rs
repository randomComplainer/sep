use std::net::SocketAddr;

use sep_lib::protocol;

#[tokio::main]
async fn main() {
    println!("Hello world from server");

    let listener = protocol::server_agent::TcpListener::bind(
        "127.0.0.1:1081".parse().unwrap(),
        [0u8; 32].into(),
    )
    .await
    .unwrap();

    println!("Listening...");

    while let Ok((agent, addr)) = listener.accept().await {
        tokio::spawn(async move {
            handle_client(agent, addr).await;
        });
    }
}

pub async fn handle_client<Stream>(agent: protocol::server_agent::Init<Stream>, addr: SocketAddr)
where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let agent = agent
        .recv_greeting(protocol::get_timestamp())
        .await
        .unwrap();

    let (msg, agent) = agent.recv_request().await.unwrap();

    dbg!(msg.addr());
    dbg!(msg.port());

    let _ = agent
        .reply("128.0.0.1:1234".parse().unwrap())
        .await
        .unwrap();
}

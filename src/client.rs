use sep_lib::socks5;

#[tokio::main]
async fn main() {
    println!("Hello world from client");

    let listener = socks5::agent::Socks5Listener::bind("127.0.0.1:1080".parse().unwrap())
        .await
        .unwrap();

    println!("Listening...");

    let agent = listener.accept().await.unwrap();

    let (msg, agent) = agent.receive_greeting_message().await.unwrap();

    dbg!(msg.ver());
    dbg!(msg.methods());

    let (msg, agent) = agent.send_method_selection_message(0).await.unwrap();

    dbg!(msg.ver());
    dbg!(msg.cmd());
    dbg!(msg.rsv());
    dbg!(msg.addr());

    agent.reply(std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::new(127, 0, 0, 1),
        1080,
    )));
}

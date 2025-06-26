use sep_lib::socks5;

#[tokio::main]
async fn main() {
    println!("Hello world from client");

    let listener = socks5::agent::Socks5Listener::bind("127.0.0.1:1080".parse().unwrap())
        .await
        .unwrap();

    println!("Listening...");

    let agent = listener.accept().await.unwrap();

    let (greeting_msg, msg_bytes, agent) = agent.receive_greeting_message().await.unwrap();

    // dbg!(greeting_msg.ver.read(msg_bytes.as_ref()));
    // dbg!(greeting_msg.methods.read(msg_bytes.as_ref()));

    let (req_msg, msg_bytes, agent) = agent.send_method_selection_message(0).await.unwrap();

    dbg!(req_msg.ver.read(msg_bytes.as_ref()));
    dbg!(req_msg.cmd.read(msg_bytes.as_ref()));
    dbg!(req_msg.rsv.read(msg_bytes.as_ref()));
    dbg!(req_msg.addr.format(msg_bytes.as_ref()));
}

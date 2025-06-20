use sep_lib::socks5;

#[tokio::main]
async fn main() {
    println!("Hello world from client");

    let listener = socks5::agent::Socks5Listener::bind("127.0.0.1:1080".parse().unwrap())
        .await
        .unwrap();

    println!("Listening...");

    let agent = listener.accept().await.unwrap();

    let (method_selection_message, method_sent) =
        agent.receive_method_selection_message().await.unwrap();

    println!("method selection message: {:?}", method_selection_message);
}

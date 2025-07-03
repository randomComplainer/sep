use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use sep_lib::protocol;
use sep_lib::socks5;

#[tokio::main]
async fn main() {
    println!("Hello world from client");

    let listener = socks5::agent::Socks5Listener::bind("127.0.0.1:1080".parse().unwrap())
        .await
        .unwrap();

    println!("Listening...");

    let key: Arc<protocol::Key> = protocol::key_from_string("password").into();

    while let Ok((agent, addr)) = listener.accept().await {
        tokio::spawn({
            let key = key.clone();
            async move {
                handle_proxyee(key, agent, addr).await;
            }
        });
    }
}

async fn handle_proxyee<Stream>(
    key: Arc<protocol::Key>,
    proxyee: socks5::agent::Init<Stream>,
    _: SocketAddr,
) where
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
        key,
        protocol::rand_nonce(),
        tokio::net::TcpStream::connect("127.0.0.1:1081")
            .await
            .unwrap(),
    );

    let server = server
        .send_greeting(protocol::get_timestamp())
        .await
        .unwrap();

    dbg!(proxyee_msg.0.ver.offset);

    let (server_bound_addr, (server_read, mut server_write)) = server
        .send_request(proxyee_msg.addr_bytes_mut())
        .await
        .unwrap();

    let (server_read_buf, mut server_read) = server_read.into_parts();

    dbg!(server_bound_addr);

    let (proxyee_read, mut proxyee_write) = proxyee.reply(&server_bound_addr).await.unwrap();
    let (mut proxyee_read_buf, mut proxyee_read) = proxyee_read.into_parts();

    let proxyee_to_server = async move {
        server_write.write_all(proxyee_read_buf.as_mut()).await?;
        // TODO: magic size
        let mut buf = vec![0u8; 1024 * 4];
        loop {
            let n = proxyee_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            server_write.write_all(&mut buf[..n]).await?;
        }

        Ok::<_, std::io::Error>(())
    };

    let server_to_proxyee = async move {
        proxyee_write.write_all(server_read_buf.as_ref()).await?;
        tokio::io::copy(&mut server_read, &mut proxyee_write).await?;

        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(proxyee_to_server, server_to_proxyee).unwrap();
}

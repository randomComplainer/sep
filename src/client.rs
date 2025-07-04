use std::net::SocketAddr;
use std::sync::Arc;

use rand::Rng;
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
    let (msg, proxyee) = match proxyee.receive_greeting_message().await {
        Ok(x) => x,
        Err(err) => {
            dbg!(err);
            dbg!("failed to receive greeting message");
            let wait_time = rand::rng().random_range(30..61);
            tokio::time::sleep(std::time::Duration::from_secs(wait_time)).await;
            return;
        }
    };

    dbg!(msg.ver());
    dbg!(msg.methods());

    let (mut proxyee_msg, proxyee) = match proxyee.send_method_selection_message(0).await {
        Ok(x) => x,
        Err(err) => {
            dbg!(err);
            dbg!("failed to send method selection message");
            return;
        }
    };

    dbg!(proxyee_msg.ver());
    dbg!(proxyee_msg.cmd());
    dbg!(proxyee_msg.rsv());
    dbg!(proxyee_msg.addr());

    let server = protocol::client_agent::Init::new(
        key,
        protocol::rand_nonce(),
        match tokio::net::TcpStream::connect("149.28.61.13:1081").await {
            Ok(stream) => stream,
            Err(err) => {
                dbg!(err);
                dbg!("failed to connect to server");
                return;
            }
        },
    );

    let server = match server.send_greeting(protocol::get_timestamp()).await {
        Ok(server) => server,
        Err(err) => {
            dbg!(err);
            dbg!("failed to send greeting");
            return;
        }
    };

    dbg!(proxyee_msg.0.ver.offset);

    let (server_bound_addr, (server_read, mut server_write)) =
        match server.send_request(proxyee_msg.addr_bytes_mut()).await {
            Ok(x) => x,
            Err(err) => {
                dbg!(err);
                dbg!("failed to send request");
                return;
            }
        };

    let (server_read_buf, mut server_read) = server_read.into_parts();

    dbg!(server_bound_addr);

    let (proxyee_read, mut proxyee_write) = match proxyee.reply(&server_bound_addr).await {
        Ok(x) => x,
        Err(err) => {
            dbg!(err);
            dbg!("failed to reply");
            return;
        }
    };
    let (mut proxyee_read_buf, mut proxyee_read) = proxyee_read.into_parts();

    let proxyee_to_server = async move {
        let mut bytes_forwarede_to_server = 0;
        let mut log_bytes_sent = |n: usize| {
            bytes_forwarede_to_server += n;
            dbg!(bytes_forwarede_to_server);
        };
        server_write.write_all(proxyee_read_buf.as_mut()).await?;
        log_bytes_sent(proxyee_read_buf.len());

        // TODO: magic size
        let mut buf = vec![0u8; 1024 * 4];
        loop {
            let n = proxyee_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            log_bytes_sent(n);
            server_write.write_all(&mut buf[..n]).await?;
        }

        Ok::<_, std::io::Error>(())
    };

    let server_to_proxyee = async move {
        let mut bytes_forwarede_to_proxyee = 0;
        let mut log_bytes_forwarede_to_proxyee = |n: usize| {
            bytes_forwarede_to_proxyee += n;
            dbg!(bytes_forwarede_to_proxyee);
        };
        proxyee_write.write_all(server_read_buf.as_ref()).await?;
        log_bytes_forwarede_to_proxyee(server_read_buf.len());

        // tokio::io::copy(&mut server_read, &mut proxyee_write).await?;

        let mut buf = vec![0u8; 1024 * 4];
        loop {
            let n = server_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            proxyee_write.write_all(&mut buf[..n]).await?;
            log_bytes_forwarede_to_proxyee(n);
        }

        Ok::<_, std::io::Error>(())
    };

    match tokio::try_join!(proxyee_to_server, server_to_proxyee) {
        Ok(_) => {}
        Err(err) => {
            dbg!(err);
            dbg!("error while forwarding data");
        }
    }
}

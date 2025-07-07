use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use sep_lib::protocol;
use sep_lib::socks5;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    key: String,

    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))]
    bound_addr: IpAddr,

    #[arg(short, long, default_value_t = 1080)]
    port: u16,

    #[arg(long = "server")]
    server_addr: SocketAddr,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    dbg!(&args);
    println!("Hello world from client");

    let bound_addr = SocketAddr::new(args.bound_addr, args.port);

    let listener = socks5::agent::Socks5Listener::bind(bound_addr)
        .await
        .unwrap();

    println!("Listening at {}...", bound_addr);

    let key: Arc<protocol::Key> = protocol::key_from_string(&args.key).into();
    let server_addr = Arc::new(args.server_addr);

    while let Ok((agent, local_addr)) = listener.accept().await {
        tokio::spawn({
            let key = key.clone();
            let server_addr = server_addr.clone();
            handle_proxyee(key, agent, server_addr, local_addr)
        });
    }
}

async fn handle_proxyee<Stream>(
    key: Arc<protocol::Key>,
    proxyee: socks5::agent::Init<Stream>,
    server_addr: Arc<SocketAddr>,
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
        match tokio::net::TcpStream::connect(server_addr.as_ref()).await {
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
    //
    // let (server_bound_addr, (server_read, mut server_write)) =
    //     match server.send_request(proxyee_msg.addr_bytes_mut()).await {
    //         Ok(x) => x,
    //         Err(err) => {
    //             dbg!(err);
    //             dbg!("failed to send request");
    //             return;
    //         }
    //     };
    //
    // let (server_read_buf, mut server_read) = server_read.into_parts();
    //
    // dbg!(server_bound_addr);
    //
    // let (proxyee_read, mut proxyee_write) = match proxyee.reply(&server_bound_addr).await {
    //     Ok(x) => x,
    //     Err(err) => {
    //         dbg!(err);
    //         dbg!("failed to reply");
    //         return;
    //     }
    // };
    // let (mut proxyee_read_buf, mut proxyee_read) = proxyee_read.into_parts();
    //
    // let proxyee_to_server = async move {
    //     let mut bytes_forwarede_to_server = 0;
    //     let mut log_bytes_sent = |n: usize| {
    //         bytes_forwarede_to_server += n;
    //         dbg!(bytes_forwarede_to_server);
    //     };
    //     server_write.write_all(proxyee_read_buf.as_mut()).await?;
    //     log_bytes_sent(proxyee_read_buf.len());
    //
    //     // TODO: magic size
    //     let mut buf = vec![0u8; 1024 * 4];
    //     loop {
    //         let n = proxyee_read.read(&mut buf).await?;
    //         if n == 0 {
    //             break;
    //         }
    //         log_bytes_sent(n);
    //         server_write.write_all(&mut buf[..n]).await?;
    //     }
    //
    //     Ok::<_, std::io::Error>(())
    // };
    //
    // let server_to_proxyee = async move {
    //     let mut bytes_forwarede_to_proxyee = 0;
    //     let mut log_bytes_forwarede_to_proxyee = |n: usize| {
    //         bytes_forwarede_to_proxyee += n;
    //         dbg!(bytes_forwarede_to_proxyee);
    //     };
    //     proxyee_write.write_all(server_read_buf.as_ref()).await?;
    //     log_bytes_forwarede_to_proxyee(server_read_buf.len());
    //
    //     // tokio::io::copy(&mut server_read, &mut proxyee_write).await?;
    //
    //     let mut buf = vec![0u8; 1024 * 4];
    //     loop {
    //         let n = server_read.read(&mut buf).await?;
    //         if n == 0 {
    //             break;
    //         }
    //         proxyee_write.write_all(&mut buf[..n]).await?;
    //         log_bytes_forwarede_to_proxyee(n);
    //     }
    //
    //     Ok::<_, std::io::Error>(())
    // };
    //
    // match tokio::try_join!(proxyee_to_server, server_to_proxyee) {
    //     Ok(_) => {}
    //     Err(err) => {
    //         dbg!(err);
    //         dbg!("error while forwarding data");
    //     }
    // }
}

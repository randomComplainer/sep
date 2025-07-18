use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use futures::prelude::*;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use sep_lib::prelude::*;

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

    let (mut new_proxee_tx, new_proxee_rx) = futures::channel::mpsc::channel(4);
    let channeling_new_proxee = async move {
        loop {
            let (agent, socket_addr) = listener.accept().await.unwrap();
            new_proxee_tx
                .send((socket_addr.port(), agent))
                .await
                .unwrap();
        }

        Ok::<_, std::io::Error>(())
    };

    let main_task = sep_lib::proxyee_conn_group::run_task(new_proxee_rx, move || {
        let key = key.clone();
        let server_addr = server_addr.clone();

        Box::pin(async move {
            let server = protocol::client_agent::Init::new(
                key,
                protocol::rand_nonce(),
                tokio::net::TcpStream::connect(server_addr.as_ref())
                    .await
                    .unwrap(),
            );

            let conn = server
                .send_greeting(protocol::get_timestamp())
                .await
                .unwrap();

            Ok::<_, std::io::Error>(conn)
        })
    });

    tokio::try_join! {
        main_task,
        channeling_new_proxee,
    }
    .map(|_| ())
    .unwrap();
}

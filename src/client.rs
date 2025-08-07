#![feature(impl_trait_in_bindings)]

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use futures::prelude::*;
use rand::RngCore as _;
use sep_lib::client_main_task;
use tracing::*;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

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
    // TODO: configurable pretty
    let layer = tracing_subscriber::fmt::layer()
        .pretty()
        // .json()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr);
    let subscriber = tracing_subscriber::Registry::default().with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args = Args::parse();
    info!(?args, "starting client");

    let bound_addr = SocketAddr::new(args.bound_addr, args.port);

    let mut client_id: Box<[u8; 16]> = [0u8; 16].into();
    rand::rng().fill_bytes(client_id.as_mut());
    let client_id: Arc<[u8; 16]> = client_id.into();

    let key: Arc<protocol::Key> = protocol::key_from_string(&args.key).into();
    let server_addr = Arc::new(args.server_addr);

    loop {
        let key = key.clone();
        let client_id = client_id.clone();
        let server_addr = server_addr.clone();

        let listener = socks5::agent::Socks5Listener::bind(bound_addr)
            .await
            .unwrap();
        println!("Listening at {}...", bound_addr);

        let (mut new_proxee_tx, new_proxee_rx) = futures::channel::mpsc::channel(4);
        let channeling_new_proxee: impl Future<Output = Result<(), client_main_task::ClientError>> = async move {
            loop {
                let (agent, socket_addr) = listener.accept().await.unwrap();
                new_proxee_tx
                    .send((socket_addr.port(), agent))
                    .await
                    .unwrap();
            }
        };

        let main_task = sep_lib::client_main_task::run(
            new_proxee_rx,
            move || {
                let key = key.clone();
                let server_addr = server_addr.clone();

                Box::pin({
                    let client_id = client_id.clone();
                    async move {
                        let stream = tokio::net::TcpStream::connect(server_addr.as_ref())
                            .await
                            .unwrap();

                        let server = protocol::client_agent::Init::new(
                            client_id,
                            stream.local_addr().unwrap().port(),
                            key,
                            protocol::rand_nonce(),
                            stream,
                        );

                        let conn = server
                            .send_greeting(protocol::get_timestamp())
                            .await
                            .unwrap();

                        Ok::<_, std::io::Error>(conn)
                    }
                })
            },
            4,
        );

        match tokio::try_join! {
            main_task,
            channeling_new_proxee,
        } {
            Ok(_) => unreachable!(),
            Err(err) => match err {
                client_main_task::ClientError::SessionProtocol(session_id, err) => {
                    // protocol error means bug, exit
                    println!("session protocol error: session id {}: {}", session_id, err);
                    return;
                }
                client_main_task::ClientError::LostConnection => {
                    println!("lost connection to server");
                    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                    continue;
                }
            },
        }
    }
}

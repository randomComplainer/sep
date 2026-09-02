#![feature(impl_trait_in_bindings)]

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use futures::prelude::*;
use rand::RngCore as _;
use tracing::*;

use sep_lib::prelude::*;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    key: String,

    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 1080))]
    bound_addr: SocketAddr,

    #[arg(long = "server")]
    server_addr: SocketAddr,

    #[command(flatten)]
    log_parameters: sep_lib::cli_parameters::LogParameter,
}

fn main() {
    let args = Args::parse();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main(args));
}

async fn async_main(args: Args) {
    info!(?args, "starting client");
    args.log_parameters.setup_subscriber();

    let key: Arc<protocol::Key> = protocol::key_from_string(&args.key).into();
    let server_addr = Arc::new(args.server_addr);
    let mut retry = RetryState::default();

    loop {
        let key = key.clone();
        let server_addr = server_addr.clone();

        let mut client_id: Box<[u8; 16]> = [0u8; 16].into();
        rand::rng().fill_bytes(client_id.as_mut());
        let client_id: Arc<[u8; 16]> = client_id.into();

        let listener = proxy_interface::Listener::bind(args.bound_addr)
            .await
            .unwrap();

        info!("Listening at {}...", args.bound_addr);

        let (mut new_proxee_tx, new_proxee_rx) = futures::channel::mpsc::channel(4);
        let channeling_new_proxee: impl Future<Output = Result<(), std::io::Error>> = async move {
            let mut session_id = 0u64;
            loop {
                let (agent, _remote_addr) = match listener.accept().await {
                    Ok(x) => x,
                    Err(err) => {
                        warn!(?err, "failed to accept connection");
                        continue;
                    }
                };
                new_proxee_tx.send((session_id, agent)).await.unwrap();
                session_id += 1;
            }
        };

        let main_task = sep_lib::client::main_task::run(
            new_proxee_rx,
            move |conn_id| {
                let key = key.clone();
                let server_addr = server_addr.clone();

                Box::pin({
                    let client_id = client_id.clone();
                    async move {
                        let socket = match server_addr.as_ref() {
                            std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
                            std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
                        };

                        socket.set_nodelay(true)?;
                        let stream = socket.connect(server_addr.as_ref().clone()).await?;

                        let server = protocol::client_agent::implementation::Init::new(
                            client_id,
                            key,
                            protocol::rand_nonce(),
                            stream,
                        );

                        let conn = server
                            .send_greeting(conn_id, protocol::get_timestamp())
                            .await?;

                        Ok::<_, std::io::Error>(conn)
                    }
                })
            },
            sep_lib::client::main_task::Config {
                max_packet_size: protocol::DATA_BUFF_SIZE,
                max_bytes_ahead: protocol::MAX_BYTES_AHEAD,
                max_server_conn: 16,
                max_conn_per_session: protocol::MAX_CONN_PER_SESSION,
            },
        );

        match tokio::try_join! {
            main_task,
            channeling_new_proxee,
        } {
            Ok(_) => unreachable!(),
            Err(err) => {
                // lost connection to server
                // retry later
                error!("lost connection to server: {}", err);
                // tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                retry.wait().await;
                continue;
            }
        }
    }
}

#[derive(Debug, Default)]
struct RetryState {
    history: Option<std::time::Instant>,
}

impl RetryState {
    pub async fn wait(&mut self) {
        let now = std::time::Instant::now();

        // no attempt in 5 seconds -> instant retry
        // else wait for 10 seconds

        if self
            .history
            .map(|h| now - h >= std::time::Duration::from_secs(5))
            .unwrap_or(true)
        {
            self.history = Some(now);
            return;
        } else {
            self.history = Some(now);
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    }
}

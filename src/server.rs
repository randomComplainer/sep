#![feature(impl_trait_in_bindings)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use futures::prelude::*;
use rand::{Rng as _, SeedableRng as _};
use tracing::*;

use sep_lib::prelude::*;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    key: String,

    #[arg(short, long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 1081))]
    bound_addr: SocketAddr,

    #[command(flatten)]
    log_parameters: sep_lib::cli_parameters::LogParameter,
}

fn main() {
    let args = Args::parse();
    args.log_parameters.setup_subscriber();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main(args));
}

async fn async_main(args: Args) {
    info!(?args, "starting server");

    let listener = protocol::server_agent::implementation::TcpListener::bind(
        &args.bound_addr,
        protocol::key_from_string(&args.key).into(),
    )
    .await
    .unwrap();

    // info!("Listening at {}...", bound_addr);

    let (new_client_conn_tx, new_client_conn_rx) = futures::channel::mpsc::channel(4);

    let channeling_new_client: impl Future<Output = Result<(), std::io::Error>> = async move {
        loop {
            let agent = listener.accept().await?;
            tokio::spawn({
                let mut new_client_conn_tx = new_client_conn_tx.clone();
                async move {
                    let (client_id, conn_id, client_read, client_write) =
                        match agent.recv_greeting(protocol::get_timestamp()).await {
                            Ok(agent) => agent,
                            Err(err) => {
                                debug!("failed to receive greeting: {}", err);
                                tokio::time::sleep(std::time::Duration::from_secs(
                                    rand::rngs::StdRng::from_os_rng().random_range(0..=30u64),
                                ))
                                .await;
                                return;
                            }
                        };

                    new_client_conn_tx
                        .send((client_id, conn_id, client_read, client_write))
                        .await
                        .unwrap();
                }
            });
        }
    }
    .instrument(info_span!("channeling new client"));

    let main_task = sep_lib::server::main_task::run(
        new_client_conn_rx,
        sep_lib::server::main_task::Config {
            max_packet_size: protocol::DATA_BUFF_SIZE,
            max_bytes_ahead: protocol::MAX_BYTES_AHEAD,
        },
    );

    tokio::try_join! {
        main_task,
        channeling_new_client,
    }
    .map(|_| ())
    .unwrap();
}

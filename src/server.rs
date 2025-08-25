use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use futures::prelude::*;
use tracing::*;

use sep_lib::prelude::*;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    key: String,

    #[arg(short, long="addr", default_value_t = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))]
    bound_addr: IpAddr,

    #[arg(short, long, default_value_t = 1081)]
    port: u16,

    #[command(flatten)]
    log_parameters: sep_lib::cli_parameters::LogParameter,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    args.log_parameters.setup_subscriber();
    info!(?args, "starting server");

    let bound_addr = SocketAddr::new(args.bound_addr, args.port);

    let listener = protocol::server_agent::TcpListener::bind(
        bound_addr,
        protocol::key_from_string(&args.key).into(),
    )
    .await
    .unwrap();

    info!("Listening at {}...", bound_addr);

    let (new_client_conn_tx, new_client_conn_rx) = futures::channel::mpsc::channel(4);

    let channeling_new_client = async move {
        loop {
            let (agent, _socket_addr) = listener.accept().await.unwrap();
            tokio::spawn({
                let mut new_client_conn_tx = new_client_conn_tx.clone();
                async move {
                    let (client_id, client_read, client_write) =
                        match agent.recv_greeting(protocol::get_timestamp()).await {
                            Ok(agent) => agent,
                            Err(err) => {
                                debug!("failed to receive greeting: {}", err);
                                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                                return;
                            }
                        };

                    new_client_conn_tx
                        .send((client_id, client_read, client_write))
                        .await
                        .unwrap();
                }
            });
        }

        Ok::<_, std::io::Error>(())
    };

    let main_task = sep_lib::server_main_task::run(new_client_conn_rx);

    tokio::try_join! {
        main_task,
        channeling_new_client,
    }
    .map(|_| ())
    .unwrap();
}

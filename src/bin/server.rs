use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use clap::Parser;
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use sep_lib::protocol;
use tokio::io::AsyncWriteExt;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(long = "bound-addr", default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9998))]
    bound_addr: SocketAddr,

    #[arg(long)]
    key: PathBuf,

    #[arg(long)]
    cert: PathBuf,

    #[arg(long = "trusted-dir")]
    trusted_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();

    let args = Args::parse();
    dbg!(&args);

    let server_config = config_server(&args.key, &args.cert, &args.trusted_dir);

    let endpoint = Endpoint::server(server_config, args.bound_addr).unwrap();

    println!("listening");

    loop {
        let incoming_conn = endpoint.accept().await.unwrap();

        tokio::spawn(async move {
            let conn = incoming_conn.await.unwrap();

            println!(
                "[server] connection accepted: addr={}",
                conn.remote_address()
            );

            handle_conn(conn).await.unwrap();
        });
    }
}

fn config_server(
    server_priv_key_path: &Path,
    server_cert_path: &Path,
    trusted_dir: &Path,
) -> ServerConfig {
    let server_priv_key =
        rustls::pki_types::PrivatePkcs8KeyDer::from_pem_file(server_priv_key_path).unwrap();

    let server_cert = rustls::pki_types::CertificateDer::from_pem_file(server_cert_path).unwrap();

    let mut trusted_certs = rustls::RootCertStore::empty();
    for dir_entry in std::fs::read_dir(trusted_dir).unwrap() {
        let file = dir_entry.unwrap();
        let cert = rustls::pki_types::CertificateDer::from_pem_file(file.path()).unwrap();
        trusted_certs.add(CertificateDer::from(cert)).unwrap();
    }

    // trusted_certs.add(server_cert.clone()).unwrap();

    let tls_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(trusted_certs))
                .build()
                .unwrap(),
        )
        .with_single_cert(vec![server_cert], server_priv_key.into())
        .unwrap();

    let quinn_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config).unwrap(),
    ));

    quinn_config
}

async fn handle_conn(conn: quinn::Connection) -> Result<(), std::io::Error> {
    loop {
        let (client_write, client_read) = conn.accept_bi().await?;
        tokio::spawn(async move {
            handle_stream(client_write, client_read).await.unwrap();
        });
    }
}

async fn handle_stream(
    mut client_write: quinn::SendStream,
    client_read: quinn::RecvStream,
) -> Result<(), std::io::Error> {
    let mut client_read = sep_lib::BufReader::new(client_read);

    let req = client_read
        .read_framed(sep_lib::protocol::msg::request_peeker())
        .await?;
    dbg!(&req);

    // let mut buf = [0u8; 4];
    // client_read.read_exact(buf.as_mut_slice()).await.unwrap();
    // println!("[server] read: {:?}", buf);

    let target_port = req.port;

    let target_ip = match req.addr {
        sep_lib::protocol::msg::RequestAddr::Ipv4(ip) => IpAddr::V4(ip),
        sep_lib::protocol::msg::RequestAddr::Ipv6(ip) => IpAddr::V6(ip),
        sep_lib::protocol::msg::RequestAddr::Domain(buf) => {
            let domain = match str::from_utf8(&buf) {
                Ok(x) => x,
                Err(e) => {
                    dbg!(e);
                    return Ok(());
                }
            };
            let mut a = tokio::net::lookup_host((domain, target_port)).await?;

            match a.next() {
                Some(addr) => addr.ip(),
                None => {
                    dbg!("cannot reslove domain name");
                    return Ok(());
                }
            }
        }
    };

    dbg!(&target_ip);

    let target_socket = match &target_ip {
        IpAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        IpAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };
    target_socket.set_nodelay(true)?;
    target_socket.set_reuseaddr(true)?;
    let target_stream = target_socket
        .connect(SocketAddr::new(target_ip, target_port))
        .await?;

    let local_addr = target_stream.local_addr()?;
    let reply = protocol::msg::Reply {
        bound_addr: local_addr,
    };

    let mut buf = BytesMut::with_capacity(64);
    match &reply.bound_addr {
        std::net::SocketAddr::V4(addr) => {
            buf.put_u8(0x01);
            buf.put_u32(addr.ip().to_bits());
        }
        std::net::SocketAddr::V6(addr) => {
            buf.put_u8(0x04);
            buf.put_slice(&addr.ip().octets());
        }
    };
    buf.put_u16(reply.bound_addr.port());

    client_write.write_all(&mut buf).await?;

    let (mut target_read, mut target_write) = tokio::io::split(target_stream);
    let (mut client_read, client_read_buffed) = client_read.unpack();

    let target_to_client = async move {
        tokio::io::copy(&mut target_read, &mut client_write).await?;

        client_write.finish()?;
        client_write.stopped().await?;

        Ok::<_, std::io::Error>(())
    };

    let client_to_target = async move {
        target_write.write_all(&client_read_buffed).await?;
        tokio::io::copy(&mut client_read, &mut target_write).await?;

        Ok::<_, std::io::Error>(())
    };

    let _ = tokio::try_join!(client_to_target, target_to_client)?;

    Ok(())
}

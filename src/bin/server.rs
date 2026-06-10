use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use sep_lib::protocol;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();

    let mut cwd = std::env::current_dir().unwrap();
    cwd.push("test_assets");

    let server_priv_key_path = {
        let mut tmp = cwd.clone();
        tmp.push("server.key.pem");
        tmp
    };

    let server_cert_path = {
        let mut tmp = cwd.clone();
        tmp.push("server.cert.pem");
        tmp
    };

    cwd.push("trusted");
    let trusted_dir = cwd;

    let server_config = config_server(&server_priv_key_path, &server_cert_path, &trusted_dir);

    let endpoint = Endpoint::server(
        server_config,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9998),
    )
    .unwrap();

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

    println!("[server] start wait");
    endpoint.wait_idle().await;
    println!("[server] exit");
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
    let (mut client_write, client_read) = conn.accept_bi().await.unwrap();

    let mut client_read = sep_lib::BufReader::new(client_read);

    let req = client_read
        .read_framed(sep_lib::protocol::msg::request_peeker())
        .await
        .unwrap();
    dbg!(&req);

    // let mut buf = [0u8; 4];
    // client_read.read_exact(buf.as_mut_slice()).await.unwrap();
    // println!("[server] read: {:?}", buf);

    let target_port = req.port;

    let target_ip = match req.addr {
        sep_lib::protocol::msg::RequestAddr::Ipv4(ip) => IpAddr::V4(ip),
        sep_lib::protocol::msg::RequestAddr::Ipv6(ip) => IpAddr::V6(ip),
        sep_lib::protocol::msg::RequestAddr::Domain(buf) => {
            let domain = str::from_utf8(&buf).unwrap();
            let mut a = tokio::net::lookup_host((domain, target_port))
                .await
                .unwrap();
            a.next().unwrap().ip()
        }
    };

    let target_socket = tokio::net::TcpSocket::new_v4().unwrap();
    target_socket.set_nodelay(true).unwrap();
    target_socket.set_reuseaddr(true).unwrap();
    let target_stream = target_socket
        .connect(SocketAddr::new(target_ip, target_port))
        .await
        .unwrap();

    let local_addr = target_stream.local_addr().unwrap();
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

    client_write.write_all(&mut buf).await.unwrap();

    let (mut target_read, mut target_write) = tokio::io::split(target_stream);
    let (mut client_read, client_read_buffed) = client_read.unpack();

    let target_to_client = async move {
        tokio::io::copy(&mut target_read, &mut client_write).await?;

        client_write.finish().unwrap();
        client_write.stopped().await.unwrap();

        Ok::<_, std::io::Error>(())
    };

    let client_to_target = async move {
        target_write.write_all(&client_read_buffed).await?;
        tokio::io::copy(&mut client_read, &mut target_write).await?;

        Ok::<_, std::io::Error>(())
    };

    let _ = tokio::try_join!(client_to_target, target_to_client).unwrap();

    conn.closed().await;

    Ok(())
}

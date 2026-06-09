use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

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

    let incoming_conn = endpoint.accept().await.unwrap();
    let conn = incoming_conn.await.unwrap();

    println!(
        "[server] connection accepted: addr={}",
        conn.remote_address()
    );

    let (mut send_stream, mut recv_stream) = conn.accept_bi().await.unwrap();

    let mut buf = [0u8; 4];
    recv_stream.read_exact(buf.as_mut_slice()).await.unwrap();
    println!("[server] read: {:?}", buf);

    send_stream.write(&[4, 3, 2, 1]).await.unwrap();
    send_stream.finish().unwrap();

    let _ = recv_stream.read_to_end(10).await.unwrap();

    conn.closed().await;

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

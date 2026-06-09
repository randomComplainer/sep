use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
};

use quinn::Endpoint;
use rustls::pki_types::{CertificateDer, pem::PemObject as _};
use tokio::io::AsyncReadExt as _;

#[tokio::main]
async fn main() {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();

    let mut cwd = std::env::current_dir().unwrap();
    cwd.push("test_assets");

    let server_cert_path = {
        let mut tmp = cwd.clone();
        tmp.push("server.cert.pem");
        tmp
    };

    let client_cert_path = {
        let mut tmp = cwd.clone();
        tmp.push("client.cert.pem");
        tmp
    };

    let client_priv_key_path = {
        let mut tmp = cwd.clone();
        tmp.push("client.key.pem");
        tmp
    };

    let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9998);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9999);

    let client_config = config_client(&client_cert_path, &client_priv_key_path, &server_cert_path);

    let mut endpoint = Endpoint::client(addr).unwrap();
    endpoint.set_default_client_config(client_config);

    let connection = endpoint
        .connect(server_addr, "server")
        .unwrap()
        .await
        .unwrap();

    println!("[client] connected: addr={}", connection.remote_address());

    let (mut send_stream, mut recv_stream) = connection.open_bi().await.unwrap();

    send_stream.write(&[1, 2, 3, 4]).await.unwrap();
    let _ = send_stream.finish();

    let mut buf = [0u8; 4];
    recv_stream.read_exact(buf.as_mut_slice()).await.unwrap();
    println!("[client] read: {:?}", buf);

    let _ = recv_stream.read_to_end(10).await.unwrap();

    connection.close(quinn::VarInt::from_u64(0u64).unwrap(), &[]);

    println!("[client] start wait");
    endpoint.wait_idle().await;
    println!("[client] exit");
    // listen(addr).await.unwrap();
}

fn config_client(
    client_cert_path: &Path,
    client_priv_key_path: &Path,
    server_cert_path: &Path,
) -> quinn::ClientConfig {
    let client_priv_key =
        rustls::pki_types::PrivatePkcs8KeyDer::from_pem_file(client_priv_key_path).unwrap();
    let client_cert = rustls::pki_types::CertificateDer::from_pem_file(client_cert_path).unwrap();

    let mut trusted_certs = rustls::RootCertStore::empty();
    let server_cert = rustls::pki_types::CertificateDer::from_pem_file(server_cert_path).unwrap();
    trusted_certs
        .add(CertificateDer::from(server_cert))
        .unwrap();

    let tls_config = rustls::ClientConfig::builder()
        .with_webpki_verifier(
            rustls::client::WebPkiServerVerifier::builder(Arc::new(trusted_certs))
                .build()
                .unwrap(),
        )
        .with_client_auth_cert(vec![client_cert], client_priv_key.into())
        .unwrap();

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config).unwrap();

    let quinn_config = quinn::ClientConfig::new(Arc::new(quic_crypto));

    quinn_config
}

async fn listen(addr: SocketAddr) -> std::io::Result<()> {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_nodelay(true)?;
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(addr.port().into())?;

    while let (proxyee_stream, _proxyee_addr) = listener.accept().await? {
        let mut buf = [0u8];

        proxyee_stream.peek(buf.as_mut_slice()).await?;

        match buf[0] {
            b'C' => serve_http(proxyee_stream).await?,
            x => {
                println!("unexpected byte: $[{x}]");
                break;
            }
        };
    }

    Ok(())
}

async fn serve_http(
    mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
) -> std::io::Result<()> {
    let mut buf = bytes::BytesMut::with_capacity(1024);

    let (target_domain, header_len) = loop {
        let n = stream.read_buf(&mut buf).await?;

        let mut headers = [httparse::EMPTY_HEADER; 8];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf).unwrap() {
            httparse::Status::Complete(header_len) => {
                break (req.path.unwrap().to_owned(), header_len);
            }
            httparse::Status::Partial => {
                if n == 0 {
                    panic!("unexpected end of stream")
                };
                continue;
            }
        };
    };

    dbg!(target_domain);
    dbg!(header_len);

    Ok(())
}

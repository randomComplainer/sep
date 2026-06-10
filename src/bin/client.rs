use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    str::FromStr,
    sync::Arc,
};

use bytes::{BufMut as _, BytesMut};
use http::uri::Authority;
use quinn::Endpoint;
use rustls::pki_types::{CertificateDer, pem::PemObject as _};

use sep_lib::{BufReader, protocol};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9999);

    listen(addr).await.unwrap();
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

    while let (source_stream, _source_addr) = listener.accept().await? {
        let mut buf = [0u8];
        source_stream.peek(buf.as_mut_slice()).await?;
        let (source_read, source_write) = tokio::io::split(source_stream);
        let source_read = BufReader::new(source_read);

        match buf[0] {
            b'C' => tokio::spawn(async move {
                serve_http(source_read, source_write).await.unwrap();
            }),
            x => {
                println!("unexpected byte: $[{x}]");
                break;
            }
        };
    }

    Ok(())
}

async fn serve_http(
    mut source_read: BufReader<impl tokio::io::AsyncRead + Send + Sync + Unpin + 'static>,
    mut source_write: impl tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
) -> std::io::Result<()> {
    let (target_domain, header_len) = loop {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(source_read.get_buf()).unwrap() {
            httparse::Status::Complete(header_len) => {
                break (req.path.unwrap().to_owned(), header_len);
            }
            httparse::Status::Partial => {
                let n = source_read.read_ahead().await?;
                if n == 0 {
                    panic!("unexpected end of stream")
                };
                continue;
            }
        };
    };

    dbg!(&target_domain);
    dbg!(&header_len);

    source_read.skip(header_len).await?;

    let authority: Authority = target_domain.parse().unwrap();

    let host = authority.host();
    let port = authority.port_u16().unwrap_or(443);

    let req_addr = match std::net::IpAddr::from_str(host) {
        Ok(std::net::IpAddr::V4(ip)) => protocol::msg::RequestAddr::Ipv4(ip),
        Ok(std::net::IpAddr::V6(ip)) => protocol::msg::RequestAddr::Ipv6(ip),
        Err(_) => {
            let buf = host.as_bytes().into();
            protocol::msg::RequestAddr::Domain(buf)
        }
    };

    let req = protocol::msg::Request {
        addr: req_addr,
        port,
    };

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
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();

    let client_config = config_client(&client_cert_path, &client_priv_key_path, &server_cert_path);

    endpoint.set_default_client_config(client_config);

    // TODO: stream per source instead of connection per source
    let connection = endpoint
        .connect(server_addr, "server")
        .unwrap()
        .await
        .unwrap();

    println!("[client] connected: addr={}", connection.remote_address());

    let (mut server_write, server_read) = connection.open_bi().await.unwrap();
    let mut server_read = BufReader::new(server_read);

    let mut buf = BytesMut::with_capacity(64);
    match &req.addr {
        protocol::msg::RequestAddr::Ipv4(addr) => {
            buf.put_u8(0x01);
            buf.put_slice(&addr.octets());
        }
        protocol::msg::RequestAddr::Ipv6(addr) => {
            buf.put_u8(0x04);
            buf.put_slice(&addr.octets());
        }
        protocol::msg::RequestAddr::Domain(domain) => {
            buf.put_u8(0x03);
            buf.put_u8(domain.len() as u8);
            // TODO: no copy?
            buf.put_slice(domain.as_ref());
        }
    }
    buf.put_u16(req.port);
    server_write.write_all(&mut buf).await?;

    let reply = server_read
        .read_framed(protocol::msg::reply_peeker())
        .await?;
    dbg!(&reply);

    let http_response = b"HTTP/1.1 200 Connection Established\r\n\r\n";

    source_write.write_all(http_response.as_ref()).await?;

    let (mut server_read, server_read_buffed) = server_read.unpack();
    let (mut source_read, source_read_buffed) = source_read.unpack();

    let source_to_server = async move {
        server_write.write_all(&source_read_buffed).await?;
        tokio::io::copy(&mut source_read, &mut server_write).await?;

        server_write.finish()?;
        server_write.stopped().await?;

        Ok::<_, std::io::Error>(())
    };

    let server_to_source = async move {
        source_write.write_all(&server_read_buffed).await?;
        tokio::io::copy(&mut server_read, &mut source_write).await?;

        // server_read.read_to_end(0).await.unwrap();

        Ok::<_, std::io::Error>(())
    };

    let _ = tokio::try_join!(source_to_server, server_to_source,)?;

    connection.close(quinn::VarInt::from_u64(0u64).unwrap(), &[]);
    connection.closed().await;

    println!("[client] start wait");
    endpoint.wait_idle().await;
    println!("[client] exit");

    Ok(())
}

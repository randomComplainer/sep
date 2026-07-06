use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use bytes::{BufMut as _, BytesMut};
use clap::Parser;
use futures::TryFutureExt;
use quinn::{Endpoint, VarInt};
use rustls::pki_types::{CertificateDer, pem::PemObject as _};

use sep_lib::{BufReader, protocol};
use tokio::io::AsyncWriteExt;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(long = "bound-addr", default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9999))]
    bound_addr: SocketAddr,

    #[arg(long = "server-addr")]
    server_addr: SocketAddr,

    #[arg(long = "server-cert")]
    server_cert: PathBuf,

    #[arg(long)]
    cert: PathBuf,

    #[arg(long)]
    key: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();

    let args = Args::parse();
    dbg!(&args);

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();

    let client_config = config_client(&args.cert, &args.key, &args.server_cert);

    endpoint.set_default_client_config(client_config);

    let connection = endpoint
        .connect(args.server_addr, "server")
        .unwrap()
        .await
        .unwrap();

    println!("[client] connected: addr={}", connection.remote_address());

    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_nodelay(true)?;
    socket.set_reuseaddr(true)?;
    socket.bind(args.bound_addr)?;
    let listener = socket.listen(args.bound_addr.port().into())?;

    loop {
        let (source_stream, _source_addr) = listener.accept().await?;

        let connection = connection.clone();
        tokio::spawn(async move { handle_source(source_stream, connection).await.unwrap() });
    }
}

async fn handle_source(
    source_stream: tokio::net::TcpStream,
    server_conn: quinn::Connection,
) -> Result<(), std::io::Error> {
    let mut buf = [0u8];
    source_stream.peek(buf.as_mut_slice()).await?;
    let (source_read, source_write) = tokio::io::split(source_stream);
    let source_read = BufReader::new(source_read);

    match buf[0] {
        b'C' => serve_https(server_conn, source_read, source_write).await?,
        b'G' => serve_http(server_conn, source_read, source_write).await?,
        x => println!("unexpected byte: $[{x}]"),
    };

    Ok(())
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

    let mut quinn_config = quinn::ClientConfig::new(Arc::new(quic_crypto));

    let mut transport_config = quinn::TransportConfig::default();
    transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(25)));
    transport_config.send_window(1024 * 1024 * 64);
    transport_config.receive_window(VarInt::from_u64(1024 * 1024 * 64).unwrap());
    transport_config.stream_receive_window(VarInt::from_u64(1024 * 1024 * 16).unwrap());
    quinn_config.transport_config(Arc::new(transport_config));

    quinn_config
}

async fn serve_http(
    // TODO: accept the future that opens stream instead
    connection: quinn::Connection,
    mut source_read: BufReader<impl tokio::io::AsyncRead + Send + Sync + Unpin + 'static>,
    mut source_write: impl tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
) -> std::io::Result<()> {
    let (header_len, host, port, req_to_forward) = loop {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);

        let parsed = match req.parse(source_read.get_buf()) {
            Ok(x) => x,
            Err(e) => {
                dbg!(&e);
                return Ok(());
            }
        };

        match parsed {
            httparse::Status::Complete(header_len) => {
                let target_url = req.path.unwrap().to_owned();
                let target_url: http::Uri = match target_url.parse() {
                    Ok(x) => x,
                    Err(e) => {
                        dbg!(&e);
                        return Ok(());
                    }
                };

                dbg!(&target_url);

                let host = target_url.host().unwrap().to_owned();
                let port = target_url.port().map(|x| x.as_u16()).unwrap_or(80u16);

                let mut req_to_forward = bytes::BytesMut::new();

                let req_headline = format!(
                    "{} {} HTTP/1.{}\r\n",
                    req.method.unwrap(),
                    target_url.path_and_query().unwrap(),
                    req.version.unwrap()
                );

                req_to_forward.put_slice(req_headline.as_bytes());

                for header in headers.iter() {
                    if header.name == "" {
                        break;
                    }

                    if header.name == "Proxy-Connection" {
                        continue;
                    }

                    let header_str = format!(
                        "{}: {}\r\n",
                        header.name,
                        str::from_utf8(header.value).unwrap()
                    );

                    dbg!(&header_str);

                    req_to_forward.put_slice(header_str.as_bytes());
                }
                req_to_forward.put_slice("\r\n".as_bytes());

                break (header_len, host, port, req_to_forward);
            }
            httparse::Status::Partial => {
                let n = source_read.read_ahead().await?;
                if n == 0 {
                    dbg!("unexpected end of stream");
                    return Ok(());
                };
                continue;
            }
        };
    };

    source_read.skip(header_len).await?;

    let req_addr = match std::net::IpAddr::from_str(&host) {
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

    let (mut server_write, server_read) = connection.open_bi().await?;
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

    let connected = match server_read
        .read_framed(protocol::msg::reply_peeker())
        .await?
    {
        Ok(x) => x,
        Err(()) => {
            dbg!("cannot connect to target");
            return Ok(());
        }
    };
    dbg!(&connected);

    let http_response = b"HTTP/1.1 200 Connection Established\r\n\r\n";
    source_write.write_all(http_response.as_ref()).await?;

    server_write.write_all(&req_to_forward).await?;

    let (mut server_read, server_read_buffed) = server_read.unpack();
    let (mut source_read, source_read_buffed) = source_read.unpack();

    let source_to_server = async move {
        server_write.write_all(&source_read_buffed).await?;
        tokio::io::copy(&mut source_read, &mut server_write).await?;

        server_write.finish()?;
        server_write.stopped().await?;

        Ok::<_, std::io::Error>(())
    }
    .inspect_err(|e| {
        dbg!("error in forwarding from source to server");
        dbg!(e);
    });

    let server_to_source = async move {
        source_write.write_all(&server_read_buffed).await?;
        tokio::io::copy(&mut server_read, &mut source_write).await?;

        Ok::<_, std::io::Error>(())
    }
    .inspect_err(|e| {
        dbg!("error in forwarding from server to source");
        dbg!(e);
    });

    let _ = tokio::try_join!(source_to_server, server_to_source,)?;

    Ok(())
}
async fn serve_https(
    // TODO: accept the future that opens stream instead
    connection: quinn::Connection,
    mut source_read: BufReader<impl tokio::io::AsyncRead + Send + Sync + Unpin + 'static>,
    mut source_write: impl tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
) -> std::io::Result<()> {
    let (target_url, header_len) = loop {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);

        let parsed = match req.parse(source_read.get_buf()) {
            Ok(x) => x,
            Err(e) => {
                dbg!(&e);
                return Ok(());
            }
        };

        match parsed {
            httparse::Status::Complete(header_len) => {
                break (req.path.unwrap().to_owned(), header_len);
            }
            httparse::Status::Partial => {
                let n = source_read.read_ahead().await?;
                if n == 0 {
                    dbg!("unexpected end of stream");
                    return Ok(());
                };
                continue;
            }
        };
    };

    dbg!(&target_url);
    dbg!(&header_len);

    source_read.skip(header_len).await?;

    let uri: http::Uri = match target_url.parse() {
        Ok(x) => x,
        Err(e) => {
            dbg!(&e);
            return Ok(());
        }
    };

    let host = uri.host().unwrap();
    let port = uri.port().map(|x| x.as_u16()).unwrap_or(443u16);

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

    let (mut server_write, server_read) = connection.open_bi().await?;
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

    let connected = match server_read
        .read_framed(protocol::msg::reply_peeker())
        .await?
    {
        Ok(x) => x,
        Err(()) => {
            dbg!("cannot connect to target");
            return Ok(());
        }
    };
    dbg!(&connected);

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
    }
    .inspect_err(|e| {
        dbg!("error in forwarding from source to server");
        dbg!(e);
    });

    let server_to_source = async move {
        source_write.write_all(&server_read_buffed).await?;
        tokio::io::copy(&mut server_read, &mut source_write).await?;

        Ok::<_, std::io::Error>(())
    }
    .inspect_err(|e| {
        dbg!("error in forwarding from server to source");
        dbg!(e);
    });

    let _ = tokio::try_join!(source_to_server, server_to_source,)?;

    Ok(())
}

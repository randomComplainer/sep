use std::{future::Future, net::SocketAddr};

use crate::decode::ReadRequestAddr;

pub trait ConnectTarget: Send + 'static + Clone {
    fn connect(
        &self,
        addr: ReadRequestAddr,
        port: u16,
    ) -> impl Future<
        Output = Result<
            (
                impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
                SocketAddr,
            ),
            std::io::Error,
        >,
    > + Send
    + 'static;
}

#[derive(Clone)]
pub struct ConnectTargetImpl;

impl ConnectTargetImpl {
    async fn resolve_addrs(
        addr: ReadRequestAddr,
        port: u16,
    ) -> Result<Vec<SocketAddr>, std::io::Error> {
        let addrs = match addr {
            ReadRequestAddr::Ipv4(addr) => {
                vec![SocketAddr::new(addr.into(), port)]
            }
            ReadRequestAddr::Ipv6(addr) => {
                vec![SocketAddr::new(addr.into(), port)]
            }
            ReadRequestAddr::Domain(addr) => {
                tokio::net::lookup_host((str::from_utf8(addr.as_ref()).unwrap(), port))
                    .await?
                    .collect()
            }
        };

        Ok(addrs)
    }
}

impl ConnectTarget for ConnectTargetImpl {
    fn connect(
        &self,
        addr: ReadRequestAddr,
        port: u16,
    ) -> impl Future<
        Output = Result<
            (
                impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
                SocketAddr,
            ),
            std::io::Error,
        >,
    > + Send
    + 'static {
        async move {
            // TODO: DNS cache
            let addrs = Self::resolve_addrs(addr, port).await?;
            for addr in addrs {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(stream) => {
                        let addr = stream.local_addr()?;
                        return Ok((stream, addr));
                    }
                    // TODO: collect errors?
                    Err(_err) => {
                        continue;
                    }
                }
            }

            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "none of resolved addresses is reachable",
            ))
        }
    }
}

#[cfg(test)]
pub fn make_mock(
    entries: impl IntoIterator<
        Item = (
            (ReadRequestAddr, u16),
            Result<(tokio_test::io::Mock, SocketAddr), std::io::Error>,
        ),
    >,
) -> impl ConnectTarget {
    mock::MockConnectTarget::new(entries)
}

#[cfg(test)]
mod mock {
    use tokio::sync::Mutex;

    use super::*;

    use std::{collections::HashMap, sync::Arc};

    #[derive(Clone)]
    pub(super) struct MockConnectTarget(
        Arc<
            Mutex<
                HashMap<
                    (ReadRequestAddr, u16),
                    Result<(tokio_test::io::Mock, SocketAddr), std::io::Error>,
                >,
            >,
        >,
    );

    impl MockConnectTarget {
        pub fn new(
            entries: impl IntoIterator<
                Item = (
                    (ReadRequestAddr, u16),
                    Result<(tokio_test::io::Mock, SocketAddr), std::io::Error>,
                ),
            >,
        ) -> Self {
            Self(Arc::new(Mutex::new(entries.into_iter().collect())))
        }
    }

    impl ConnectTarget for MockConnectTarget {
        fn connect(
            &self,
            addr: ReadRequestAddr,
            port: u16,
        ) -> impl Future<
            Output = Result<
                (
                    impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
                    SocketAddr,
                ),
                std::io::Error,
            >,
        > + Send
        + 'static {
            let entries = self.0.clone();

            async move {
                let mut entries = entries.lock().await;
                match entries.remove(&(addr, port)) {
                    Some(result) => result,
                    None => Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "no entry",
                    )),
                }
            }
        }
    }
}

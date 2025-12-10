use std::{future::Future, net::SocketAddr, sync::Arc};

use crate::decode::ReadRequestAddr;

pub mod cache;

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
    > + Send;
}

#[derive(Clone)]
pub struct ConnectTargetImpl(pub cache::Cache);

impl ConnectTargetImpl {
    async fn resolve_addrs(
        &self,
        addr: ReadRequestAddr,
        port: u16,
    ) -> Result<Arc<Vec<SocketAddr>>, ()> {
        let addrs = match addr {
            ReadRequestAddr::Ipv4(addr) => Arc::new(vec![SocketAddr::new(addr.into(), port)]),
            ReadRequestAddr::Ipv6(addr) => Arc::new(vec![SocketAddr::new(addr.into(), port)]),
            ReadRequestAddr::Domain(addr) => {
                let domain = String::from_utf8(addr.into()).unwrap();
                self.0
                    .query(domain, port, tokio::time::Instant::now())
                    .await?
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
    > + Send {
        async move {
            // TODO: DNS cache
            let addrs = self.resolve_addrs(addr, port).await.map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "failed to resolve domain")
            })?;

            for addr in addrs.as_ref() {
                match tokio::net::TcpStream::connect(&addr).await {
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
pub fn make_mock<TStream>(
    entries: impl IntoIterator<
        Item = (
            (ReadRequestAddr, u16),
            Result<(TStream, SocketAddr), std::io::Error>,
        ),
    >,
) -> impl ConnectTarget
where
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    mock::MockConnectTarget::new(entries)
}

#[cfg(test)]
mod mock {
    use tokio::sync::Mutex;

    use super::*;

    use std::{collections::HashMap, sync::Arc};

    pub(super) struct MockConnectTarget<TStream>(
        Arc<Mutex<HashMap<(ReadRequestAddr, u16), Result<(TStream, SocketAddr), std::io::Error>>>>,
    );

    impl<TStream> Clone for MockConnectTarget<TStream> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<TStream> MockConnectTarget<TStream> {
        pub fn new(
            entries: impl IntoIterator<
                Item = (
                    (ReadRequestAddr, u16),
                    Result<(TStream, SocketAddr), std::io::Error>,
                ),
            >,
        ) -> Self {
            Self(Arc::new(Mutex::new(entries.into_iter().collect())))
        }
    }

    impl<TStream> ConnectTarget for MockConnectTarget<TStream>
    where
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
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
        > + Send {
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

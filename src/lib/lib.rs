#![feature(ip_from)]
#![feature(trait_alias)]

#[macro_use]
pub mod decode;
pub mod encrypt;

pub mod protocol;
pub mod session;
pub mod socks5;

pub mod prelude {
    pub use crate::decode::*;
    pub use crate::encrypt::{EncryptedRead, EncryptedWrite};
    pub use crate::{decode, protocol, session, socks5};
}

pub mod client_conn_group {
    use std::sync::Arc;

    use chacha20::cipher::StreamCipher;
    use dashmap::DashMap;
    use futures::prelude::*;

    use crate::prelude::*;

    pub async fn run_task<ClientStream, Cipher>(
        mut new_conn_rx: impl Stream<
            Item = (
                u16,
                protocol::server_agent::GreetedRead<ClientStream, Cipher>,
                protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
            ),
        > + Unpin,
    ) -> Result<(), std::io::Error>
    where
        ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        Cipher: StreamCipher + Unpin + Send + 'static,
    {
        let session_client_msg_senders = Arc::new(DashMap::<
            u16,
            futures::channel::mpsc::Sender<session::msg::ClientMsg>,
        >::new());

        let (server_msg_tx, mut server_msg_rx) =
            futures::channel::mpsc::channel::<(u16, session::msg::ServerMsg)>(4);

        let (client_write_tx, mut client_write_rx) = futures::channel::mpsc::channel::<
            protocol::server_agent::GreetedWrite<ClientStream, Cipher>,
        >(16);

        let accept_client_msg = {
            let server_msg_tx = server_msg_tx.clone();
            let session_client_msg_senders = session_client_msg_senders.clone();
            async move |session_id: u16, client_msg: session::msg::ClientMsg| {
                match &client_msg {
                    session::msg::ClientMsg::Request(_) => {
                        // TODO: cache size
                        let (client_msg_tx, client_msg_rx) = futures::channel::mpsc::channel(4);
                        let session_task = session::server::create_task(
                            client_msg_rx,
                            server_msg_tx
                                .clone()
                                .with(move |msg| std::future::ready(Ok((session_id, msg)))),
                        );

                        // It's fine if session fails,
                        // log and continue
                        tokio::spawn(async move {
                            match session_task.await {
                                Err(err) => {
                                    dbg!(err);
                                }
                                _ => (),
                            }
                        });

                        if session_client_msg_senders.contains_key(&session_id) {
                            panic!("session already exists");
                        }

                        session_client_msg_senders.insert(session_id, client_msg_tx);
                    }
                    _ => (),
                };

                // it's fine if send fails
                // session could be ended due to target closed or something
                if let Some(mut client_msg_tx) = session_client_msg_senders.get_mut(&session_id) {
                    client_msg_tx.send(client_msg).await.unwrap();
                }

                ()
            }
        };

        let accept_client_msg = Arc::new(accept_client_msg);

        let accepting_client_conns = {
            let mut client_write_tx = client_write_tx.clone();
            let accept_client_msg = Arc::clone(&accept_client_msg);
            async move {
                while let Some((_, mut client_read, client_write)) = new_conn_rx.next().await {
                    client_write_tx.send(client_write).await.unwrap();
                    let reciving_msg_from_conn = {
                        let accept_client_msg = Arc::clone(&accept_client_msg);
                        async move {
                            // TODO: if client_write closed/errored, do something
                            while let Some(agent_msg) = client_read.recv_msg().await? {
                                dbg!(format!("message from client: {:?}", &agent_msg));
                                // TODO: To/From
                                let (session_id, session_msg) = match agent_msg {
                                    protocol::msg::ClientMsg::SessionMsg(
                                        session_id,
                                        session_msg,
                                    ) => (session_id, session_msg),
                                };

                                accept_client_msg(session_id, session_msg).await;
                            }

                            Ok::<_, std::io::Error>(())
                        }
                    };

                    tokio::spawn(async move {
                        match reciving_msg_from_conn.await {
                            Ok(_) => {}
                            Err(err) => {
                                dbg!(err);
                            }
                        }
                    });
                }

                Ok::<_, std::io::Error>(())
            }
        };

        let sending_msg_to_client = async move {
            while let Some((session_id, server_msg)) = server_msg_rx.next().await {
                let mut client_write = client_write_rx.next().await.unwrap();
                tokio::spawn({
                    let mut client_write_tx = client_write_tx.clone();
                    async move {
                        dbg!(format!("message to client: {:?}", &server_msg));

                        client_write
                            .send_msg(server_msg.with_session_id(session_id))
                            .await
                            .unwrap();
                        // TODO: do I care about error?
                        let _ = client_write_tx.send(client_write).await.unwrap();
                    }
                });
            }
            Ok::<_, std::io::Error>(())
        };

        tokio::try_join! {
            accepting_client_conns,
            sending_msg_to_client,
        }
        .map(|_| ())
    }
}

pub mod proxyee_conn_group {
    use std::sync::Arc;

    use chacha20::cipher::StreamCipher;
    use dashmap::DashMap;
    use futures::prelude::*;

    use crate::prelude::*;

    pub async fn run_task<ProxyeeStream, ServerStream, Cipher, ConnectServerFut>(
        mut new_proxee_rx: impl Stream<Item = (u16, socks5::agent::Init<ProxyeeStream>)> + Unpin,
        connect_to_server: impl Fn() -> ConnectServerFut + Unpin,
    ) -> Result<(), std::io::Error>
    where
        ProxyeeStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        ServerStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        Cipher: StreamCipher + Unpin + Send + 'static,
        ConnectServerFut: std::future::Future<
                Output = Result<
                    (
                        protocol::client_agent::GreetedWrite<ServerStream, Cipher>,
                        protocol::client_agent::GreetedRead<ServerStream, Cipher>,
                    ),
                    std::io::Error,
                >,
            > + Unpin,
    {
        // let (server_msg_tx, mut server_msg_rx) =
        //     futures::channel::mpsc::channel::<(u16, session::msg::ServerMsg)>(4);

        let (client_msg_tx, mut client_msg_rx) =
            futures::channel::mpsc::channel::<(u16, session::msg::ClientMsg)>(8);

        let (server_write_tx, mut server_write_rx) = futures::channel::mpsc::channel::<
            protocol::client_agent::GreetedWrite<ServerStream, Cipher>,
        >(16);

        let session_server_msg_senders = Arc::new(DashMap::<
            u16,
            futures::channel::mpsc::Sender<session::msg::ServerMsg>,
        >::new());

        let accepting_proxyee = {
            let client_msg_tx = client_msg_tx.clone();
            let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
            async move {
                while let Some((session_id, proxyee)) = new_proxee_rx.next().await {
                    let (session_server_msg_tx, session_server_msg_rx) =
                        futures::channel::mpsc::channel(4);
                    let session_client_msg_tx = client_msg_tx
                        .clone()
                        .with(move |msg| std::future::ready(Ok((session_id, msg))));
                    session_server_msg_senders.insert(session_id, session_server_msg_tx);

                    let session_task = session::client::run_task(
                        proxyee,
                        session_server_msg_rx,
                        session_client_msg_tx,
                    );

                    tokio::spawn(async move {
                        match session_task.await {
                            Ok(_) => {}
                            Err(err) => {
                                dbg!(err);
                            }
                        }
                    });
                }

                Ok::<_, std::io::Error>(())
            }
        };

        let sending_msg_to_server = {
            let server_write_tx = server_write_tx.clone();
            async move {
                while let Some((session_id, client_msg)) = client_msg_rx.next().await {
                    let mut server_write = server_write_rx.next().await.unwrap();
                    dbg!(format!("message to server: {:?}", &client_msg));

                    tokio::spawn({
                        let mut server_write_tx = server_write_tx.clone();

                        async move {
                            server_write
                                .send_msg(client_msg.with_session_id(session_id))
                                .await
                                .unwrap();
                            // TODO: do I care about error?
                            let _ = server_write_tx.send(server_write).await;
                        }
                    });
                }

                Ok::<_, std::io::Error>(())
            }
        };

        let connect_to_server = {
            let mut server_write_tx = server_write_tx.clone();
            let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
            async move {
                let mut i = 0;
                while i < 4 {
                    let session_server_msg_senders = Arc::clone(&session_server_msg_senders);
                    let (server_write, mut server_read) = connect_to_server().await?;

                    let reciving_msg_from_server = {
                        async move {
                            while let Some(agent_msg) = server_read.recv_msg().await? {
                                dbg!(format!("message from server: {:?}", &agent_msg));
                                let (session_id, session_msg): (u16, session::msg::ServerMsg) =
                                    match agent_msg {
                                        protocol::msg::ServerMsg::SessionMsg(
                                            session_id,
                                            session_msg,
                                        ) => (session_id, session_msg),
                                    };

                                if let Some(mut session_server_msg_sender) =
                                    session_server_msg_senders.get_mut(&session_id)
                                {
                                    let _ = session_server_msg_sender.send(session_msg).await;
                                }
                            }

                            Ok::<_, std::io::Error>(())
                        }
                    };

                    tokio::spawn(async move {
                        match reciving_msg_from_server.await {
                            Ok(_) => {}
                            Err(err) => {
                                dbg!(err);
                            }
                        }
                    });

                    server_write_tx.send(server_write).await.unwrap();
                    i += 1;
                }

                Ok::<_, std::io::Error>(())
            }
        };

        tokio::try_join! {
            accepting_proxyee,
            sending_msg_to_server,
            connect_to_server,
        }
        .map(|_| ())
    }
}

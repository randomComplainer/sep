use std::sync::Arc;

use tokio::io::{DuplexStream, duplex};

use sep_lib::prelude::*;

fn create_pair() -> (
    protocol::client_agent::Init<DuplexStream>,
    protocol::server_agent::implementation::Init<DuplexStream>,
) {
    let key: Arc<protocol::Key> = protocol::key_from_string("000").into();
    let nonce: Box<protocol::Nonce> = vec![1u8; 12].try_into().unwrap();
    let client_id: Arc<[u8; 16]> = [1u8; 16].into();

    let (client_steam, server_stream) = duplex(8 * 1024);

    let client_agent =
        protocol::client_agent::Init::new(client_id, 0, key.clone(), nonce, client_steam);
    let server_agent = protocol::server_agent::implementation::Init::new(key, server_stream);

    (client_agent, server_agent)
}

#[tokio::test]
async fn happy_path() {
    let (client_agent, server_agent) = create_pair();

    client_agent.send_greeting(12).await.unwrap();
    server_agent.recv_greeting(12).await.unwrap();
}

#[tokio::test]
async fn wrong_key() {
    let key1: Arc<protocol::Key> = protocol::key_from_string("000").into();
    let key2: Arc<protocol::Key> = protocol::key_from_string("111").into();
    let nonce: Box<protocol::Nonce> = vec![1u8; 12].try_into().unwrap();

    let (client_steam, server_stream) = duplex(8 * 1024);
    let client_agent =
        protocol::client_agent::Init::new([1u8; 16].into(), 0, key1.clone(), nonce, client_steam);
    let server_agent = protocol::server_agent::implementation::Init::new(key2, server_stream);

    client_agent.send_greeting(12).await.unwrap();
    assert!(server_agent.recv_greeting(12).await.is_err());
}

#[tokio::test]
async fn wrong_timestamp() {
    let (client_agent, server_agent) = create_pair();

    client_agent.send_greeting(12).await.unwrap();
    assert!(server_agent.recv_greeting(43).await.is_err());
}

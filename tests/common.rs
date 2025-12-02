use std::sync::Arc;

use chacha20::ChaCha20;
use tokio::io::{DuplexStream, duplex};

use protocol::{client_agent, server_agent};
use sep_lib::prelude::*;

pub fn create_init_pair() -> (
    protocol::client_agent::implementation::Init<DuplexStream>,
    protocol::server_agent::implementation::Init<DuplexStream>,
) {
    let key: Arc<protocol::Key> = protocol::key_from_string("000").into();
    let nonce: Box<protocol::Nonce> = vec![1u8; 12].try_into().unwrap();
    let client_id: Arc<protocol::ClientId> = [1u8; 16].into();

    let (client_steam, server_stream) = duplex(8 * 1024);

    let client_agent = protocol::client_agent::implementation::Init::new(
        client_id,
        0,
        key.clone(),
        nonce,
        client_steam,
    );
    let server_agent = protocol::server_agent::implementation::Init::new(key, server_stream);

    (client_agent, server_agent)
}

pub async fn create_greeted_pair() -> (
    (
        client_agent::implementation::GreetedRead<DuplexStream, ChaCha20>,
        client_agent::implementation::GreetedWrite<DuplexStream, ChaCha20>,
    ),
    (
        Box<protocol::ClientId>,
        server_agent::implementation::GreetedRead<DuplexStream, ChaCha20>,
        server_agent::implementation::GreetedWrite<DuplexStream, ChaCha20>,
    ),
) {
    let (client_agent, server_agent) = create_init_pair();
    let client_agent = client_agent.send_greeting(12).await.unwrap();
    let server_agent = server_agent.recv_greeting(12).await.unwrap();

    (client_agent, server_agent)
}

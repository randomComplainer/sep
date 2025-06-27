// Client -> Server: Greeting
//
// Server dose not send anything back,
// close connection after random delay if Greeting is invalid
//
// Client -> Server: Request
// Server -> Client: Response
// Client <-> Server: Data
// Client <-> Server: EOF (for proxied connection)

pub mod msg {
    pub struct Greeting {
        // plaintext
        pub nonce: Box<[u8; 12]>,
        // encrypted
        pub timestamp: u64,
        // and some random bytes, size is derived from key, nonce and timestamp
    }

    pub fn test() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

fn cal_rand_byte_len(key: &[u8; 32], nonce: &[u8; 12], timestamp: u64) -> usize {
    let key_head = u64::from_be_bytes(key[0..8].try_into().unwrap());
    let nonce_head = u64::from_be_bytes(nonce[0..8].try_into().unwrap());

    let len: usize = ((key_head ^ nonce_head ^ timestamp) % 1024)
        .try_into()
        .unwrap();
    len
}

pub mod client_agent {
    use bytes::{Buf, BufMut, BytesMut};
    use chacha20::ChaCha20;
    use chacha20::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

    pub struct Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        key: Box<[u8; 32]>,
        nonce: Box<[u8; 12]>,
        stream: Stream,
    }

    impl<Stream> Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(key: Box<[u8; 32]>, nonce: Box<[u8; 12]>, stream: Stream) -> Self {
            Self { key, nonce, stream }
        }

        pub async fn send_greeting(mut self, timestamp: u64) -> Result<(), std::io::Error> {
            let mut cipher =
                ChaCha20::new(self.key.as_slice().into(), self.nonce.as_slice().into());

            let rand_byte_len = super::cal_rand_byte_len(&self.key, &self.nonce, timestamp);

            dbg!(rand_byte_len);

            // TODO: actually random generating
            let rand_bytes = vec![0; rand_byte_len];

            let buf_size = 12 // nonce
                + 8 // timestamp
                + rand_byte_len;

            let mut buf = BytesMut::with_capacity(buf_size);
            buf.put_slice(self.nonce.as_slice());

            buf.put_u64(timestamp);
            buf.put_slice(&rand_bytes);

            cipher.apply_keystream(&mut buf.as_mut()[12..]);

            self.stream.write_all(buf.as_ref()).await?;

            Ok(())
        }
    }
}

pub mod server_agent {
    use bytes::{Buf, BufMut, BytesMut};
    use chacha20::ChaCha20;
    use chacha20::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    pub struct Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        key: Box<[u8; 32]>,
        stream: Stream,
    }

    impl<Stream> Init<Stream>
    where
        Stream: AsyncRead + AsyncWrite + 'static + Unpin,
    {
        pub fn new(key: Box<[u8; 32]>, stream: Stream) -> Self {
            Self { key, stream }
        }

        pub async fn recv_greeting(mut self) -> Result<(), std::io::Error> {
            let mut nonce: Box<[u8; 12]> = vec![0u8; 12].try_into().unwrap();

            self.stream.read_exact(nonce.as_mut()).await?;

            let mut cipher = ChaCha20::new(self.key.as_slice().into(), nonce.as_slice().into());

            dbg!(nonce.as_slice());

            let mut timestamp_buf = vec![0u8; 8];

            self.stream.read_exact(&mut timestamp_buf).await?;

            cipher.apply_keystream(&mut timestamp_buf);

            let timestamp = u64::from_be_bytes(timestamp_buf.try_into().unwrap());
            dbg!(timestamp);

            let rand_byte_len = super::cal_rand_byte_len(&self.key, &nonce, timestamp);
            dbg!(rand_byte_len);

            Ok(())
        }
    }
}

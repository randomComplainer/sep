pub use encrypted_read::EncryptedRead;
pub use encrypted_write::EncryptedWrite;

mod encrypted_read {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use chacha20::cipher::StreamCipher;
    use tokio::io::{AsyncRead, ReadBuf};

    pub struct EncryptedRead<Stream, Cipher> {
        inner: Stream,
        cipher: Cipher,
    }

    impl<Stream, Cipher> EncryptedRead<Stream, Cipher> {
        pub fn new(inner: Stream, cipher: Cipher) -> Self {
            Self { inner, cipher }
        }
    }

    impl<Stream, Cipher> AsyncRead for EncryptedRead<Stream, Cipher>
    where
        Stream: AsyncRead + Unpin,
        Cipher: StreamCipher + Unpin,
    {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let before = buf.filled().len();
            let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = poll {
                let after = buf.filled().len();
                let data = &mut buf.filled_mut()[before..after];
                self.cipher.apply_keystream(data);
            }
            poll
        }
    }
}

mod encrypted_write {
    use chacha20::cipher::StreamCipher;
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    pub struct EncryptedWrite<Stream, Cipher> {
        cipher: Cipher,
        inner: Stream,
    }

    impl<Stream, Cipher> EncryptedWrite<Stream, Cipher>
    where
        Cipher: StreamCipher + Unpin,
        Stream: AsyncWrite + Unpin,
    {
        pub fn new(inner: Stream, cipher: Cipher) -> Self {
            Self { cipher, inner }
        }

        pub async fn write_all(&mut self, buf: &mut [u8]) -> tokio::io::Result<()> {
            self.cipher.apply_keystream(buf);
            self.inner.write_all(buf).await
        }
    }
}

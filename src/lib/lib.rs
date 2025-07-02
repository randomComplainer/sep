#[macro_use]
pub mod decode;
pub mod encrypt;

pub mod protocol;
pub mod socks5;

pub mod prelude {
    pub use crate::decode::Peek;
    pub use crate::encrypt::{EncryptedRead, EncryptedWrite};
    pub use crate::{decode, protocol, socks5};
}

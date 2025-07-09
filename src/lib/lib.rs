#![feature(ip_from)]

#[macro_use]
pub mod decode;
pub mod encrypt;

pub mod protocol;
pub mod socks5;

pub mod prelude {
    pub use crate::decode::{Peek as _, PeekFn as _};
    pub use crate::encrypt::{EncryptedRead, EncryptedWrite};
    pub use crate::{decode, protocol, socks5};
}

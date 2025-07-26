#![feature(duration_constructors_lite)]
#![feature(ip_from)]
#![feature(trait_alias)]
#![feature(assert_matches)]

pub mod handover;

#[macro_use]
pub mod decode;
pub mod encrypt;

pub mod protocol;
pub mod session;
pub mod socks5;

pub mod client_conn_group;
pub mod client_main_task;

pub mod prelude {
    pub use crate::decode::*;
    pub use crate::encrypt::{EncryptedRead, EncryptedWrite};
    pub use crate::{decode, protocol, session, socks5};
}

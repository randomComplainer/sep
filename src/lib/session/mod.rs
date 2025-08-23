pub mod client;
pub mod msg;
pub mod sequence;
pub mod server;

// TODO: magic number, IDK, maybe do some benchmark to find out
const DATA_BUFF_SIZE: usize = 1024 * 8;
const MAX_DATA_AHEAD: u16 = 24;

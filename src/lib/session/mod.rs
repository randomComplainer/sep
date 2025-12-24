mod sequenced_to_stream;
mod stream_to_sequenced;

pub mod client;
pub mod msg;
pub mod sequence;
pub mod server;

// TODO: magic number, IDK, maybe do some benchmark to find out
pub const DATA_BUFF_SIZE: u16 = 1024 * 16;
pub const MAX_DATA_AHEAD: u16 = 8;

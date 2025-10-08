mod sequenced_to_stream;
mod stream_to_sequenced;

pub mod client;
pub mod msg;
pub mod sequence;
pub mod server;

// TODO: magic number, IDK, maybe do some benchmark to find out
const DATA_BUFF_SIZE: usize = 1024 * 8;
const MAX_DATA_AHEAD: u16 = 24;

use derive_more::From;
#[derive(Debug, From)]
pub(self) enum SequencingError {
    SequencedBroken,
    StreamBroken(#[from] std::io::Error),
}

// Error type for session termination
// that doesn't be exposed to outside of session.
// Since only broken protocol channel is considered as a session error,
#[derive(Debug)]
pub(self) enum TerminationError {
    TargetIo,
    ProxyeeIo,
    BrokenProtocolChannel,
}

impl TerminationError {
    fn to_session_result(orig: Result<(), TerminationError>) -> Result<(), ()> {
        match orig {
            Ok(t) => Ok(t),
            Err(TerminationError::BrokenProtocolChannel) => Err(()),
            Err(_) => Ok(()),
        }
    }
}

impl From<SequencingError> for TerminationError {
    fn from(err: SequencingError) -> Self {
        match err {
            SequencingError::SequencedBroken => TerminationError::BrokenProtocolChannel,
            SequencingError::StreamBroken(_) => TerminationError::TargetIo,
        }
    }
}

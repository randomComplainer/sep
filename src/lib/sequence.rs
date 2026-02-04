use bytes::BytesMut;

pub enum StreamEntryValue {
    Data(BytesMut),
    Eof,
}

pub struct StreamEntry(pub u16, pub StreamEntryValue);

impl StreamEntry {
    pub fn data(seq: u16, data: BytesMut) -> Self {
        Self(seq, StreamEntryValue::Data(data))
    }

    pub fn eof(seq: u16) -> Self {
        Self(seq, StreamEntryValue::Eof)
    }
}

impl PartialEq for StreamEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for StreamEntry {}

impl std::cmp::PartialOrd for StreamEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl std::cmp::Ord for StreamEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

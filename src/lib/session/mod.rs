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

mod stream_to_sequenced {
    use bytes::BytesMut;
    use derive_more::From;
    use futures::prelude::*;
    use tokio::io::{AsyncRead, AsyncReadExt as _};
    use tracing::*;

    use super::SequencingError;
    use super::msg;

    #[derive(Debug, From)]
    pub enum Command {
        Ack(#[from] msg::Ack),
    }

    #[derive(Debug, From)]
    pub enum Event {
        Data(#[from] msg::Data),
        Eof(#[from] msg::Eof),
    }

    async fn read_from_stream(
        seq: u16,
        stream_to_read: &mut (impl AsyncRead + Unpin + Send + 'static),
    ) -> Result<Event, std::io::Error> {
        // TODO: reuse buf
        let mut buf = bytes::BytesMut::with_capacity(crate::session::DATA_BUFF_SIZE);
        let n = stream_to_read.read_buf(&mut buf).await?;

        if n == 0 {
            return Ok(msg::Eof { seq }.into());
        } else {
            Ok(msg::Data { seq, data: buf }.into())
        }
    }

    // sends eof on stream error
    pub async fn run(
        mut cmd_rx: impl Stream<Item = Command> + Unpin,
        mut event_tx: impl Sink<Event, Error = futures::channel::mpsc::SendError> + Unpin,
        mut stream_to_read: impl AsyncRead + Unpin + Send + 'static,
        first_pack: Option<BytesMut>,
    ) -> Result<(), SequencingError> {
        {
            let mut seq = 0;

            // stored value is actual max acked + 1
            // 0 is reserved for no ack yet
            let (max_acked_tx, mut max_acked_rx) = tokio::sync::watch::channel(0u16);

            macro_rules! try_emit_evt {
            ($evt:ident) => {
                let sp = info_span!("emit event:", evt = ?$evt);
                match event_tx
                    .send($evt)
                    .instrument(sp)
                    .await
                    {
                        Ok(_) => {}
                        Err(_) => {
                            error!("sequenced channel is broken");
                            return Err(SequencingError::SequencedBroken);
                        }
                    }
                };
            }

            if let Some(first_pack) = first_pack {
                let evt = msg::Data {
                    seq: 0,
                    data: first_pack,
                }
                .into();
                try_emit_evt!(evt);
                seq += 1;
            }

            let receving_cmds = async move {
                loop {
                    if let Some(cmd) = cmd_rx
                        .next()
                        .instrument(debug_span!("receive command"))
                        .await
                    {
                        match cmd {
                            Command::Ack(ack) => {
                                max_acked_tx.send_if_modified(|old| {
                                    let new = ack.seq + 1;
                                    if new > *old {
                                        *old = new;
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                        }
                    } else {
                        error!("sequenced channel is broken");
                        return Err::<(), _>(SequencingError::SequencedBroken);
                    }
                }
            };

            let stream_reading = async move {
                loop {
                    max_acked_rx
                        .wait_for(|max| seq - max < crate::session::MAX_DATA_AHEAD + 1)
                        .await
                        .expect("command receiving task is aborted");

                    let evt = read_from_stream(seq, &mut stream_to_read)
                        .instrument(info_span!("read from stream", seq))
                        .await?;

                    match &evt {
                        Event::Data(_) => {
                            try_emit_evt!(evt);
                        }
                        Event::Eof(_) => {
                            try_emit_evt!(evt);
                            info!(seq = seq, "eof reached, streaming exits");

                            // TODO: do I need to wait for remaining acks before exiting?
                            return Ok(());
                        }
                    };

                    seq += 1;
                }
            };

            tokio::try_join!(receving_cmds, stream_reading).map(|_| ())
        }
    }
}

mod sequenced_to_stream {
    use derive_more::From;
    use futures::prelude::*;
    use tokio::io::{AsyncWrite, AsyncWriteExt as _};
    use tracing::*;

    use crate::session::sequence::{StreamEntry, StreamEntryValue};

    use super::SequencingError;
    use super::msg;

    #[derive(Debug, From)]
    pub enum Command {
        Data(#[from] msg::Data),
        Eof(#[from] msg::Eof),
    }

    #[derive(Debug, From)]
    pub enum Event {
        Ack(#[from] msg::Ack),
    }

    pub async fn run(
        mut cmd_rx: impl Stream<Item = Command> + Unpin,
        mut event_tx: impl Sink<Event, Error = futures::channel::mpsc::SendError> + Unpin,
        mut stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    ) -> Result<(), SequencingError> {
        let mut heap =
            std::collections::BinaryHeap::<std::cmp::Reverse<StreamEntry>>::with_capacity(
                super::MAX_DATA_AHEAD as usize,
            );

        let mut next_seq = 0u16;

        macro_rules! try_emit_evt {
            ($evt:ident) => {
                let sp = info_span!("emit event:", evt = ?$evt);
                match event_tx
                    .send($evt)
                    .instrument(sp)
                    .await
                    {
                        Ok(_) => {}
                        Err(_) => {
                            error!("sequenced channel is broken");
                            return Err(SequencingError::SequencedBroken);
                    }
                }
            };
        }

        while let Some(cmd) = {
            cmd_rx
                .next()
                .instrument(debug_span!("receive command"))
                .await
        } {
            match cmd {
                Command::Data(data) => {
                    info!(
                        seq = data.seq,
                        len = data.data.len(),
                        "data command received"
                    );
                    if heap.len() == heap.capacity() {
                        panic!("too many data cached");
                    }

                    heap.push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));
                }
                Command::Eof(eof) => {
                    info!(seq = eof.seq, "eof command received");
                    if heap.len() == heap.capacity() {
                        panic!("too many data cached");
                    }

                    heap.push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
                }
            };

            while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                // safe unwrap because of ↑
                let StreamEntry(seq, entry) = heap.pop().unwrap().0;

                match entry {
                    StreamEntryValue::Data(data) => {
                        stream_to_write
                            .write_all(data.as_ref())
                            .instrument(info_span!("write to stream", seq, len = data.len()))
                            .await?;

                        let evt = msg::Ack { seq }.into();
                        try_emit_evt!(evt);
                    }
                    StreamEntryValue::Eof => {
                        info!(seq = seq, "eof reached, streaming exits");
                        return Ok(());
                    }
                }

                next_seq += 1;
            }
        }

        info!("end of commands, streaming exits");
        Ok(())
    }
}

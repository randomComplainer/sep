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
                    let evt = msg::Ack { seq }.into();
                    try_emit_evt!(evt);

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


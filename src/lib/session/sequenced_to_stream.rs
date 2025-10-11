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

async fn try_emit_evt(
    evt_tx: &mut (impl Sink<Event, Error = futures::channel::mpsc::SendError> + Unpin),
    evt: Event,
) -> Result<(), SequencingError> {
    let sp = info_span!("emit event:", evt = ?evt);
    evt_tx
        .send(evt)
        .instrument(sp)
        .await
        .inspect_err(|_| error!("sequenced channel is broken"))
        .map_err(|_| SequencingError::SequencedBroken)
}

pub struct Config {
    pub max_data_ahead: u16,
}

async fn streaming_loop(
    mut stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    mut data_rx: impl Stream<Item = StreamEntry> + Unpin,
    mut evt_tx: impl Sink<Event, Error = futures::channel::mpsc::SendError> + Unpin,
) -> Result<(), SequencingError> {
    while let Some(StreamEntry(seq, entry)) = data_rx
        .next()
        .instrument(debug_span!("receive local stream entry"))
        .await
    {
        match entry {
            StreamEntryValue::Data(data) => {
                stream_to_write
                    .write_all(data.as_ref())
                    .instrument(info_span!("write to stream", seq, len = data.len()))
                    .await?;

                let evt = msg::Ack { seq }.into();
                try_emit_evt(&mut evt_tx, evt).await?;
            }
            StreamEntryValue::Eof => {
                let evt = msg::Ack { seq }.into();
                try_emit_evt(&mut evt_tx, evt).await?;
                info!(seq = seq, "eof reached, streaming exits");
                return Ok(());
            }
        }
    }

    Ok(())
}

pub async fn run(
    mut cmd_rx: impl Stream<Item = Command> + Unpin,
    mut event_tx: impl Sink<Event, Error = futures::channel::mpsc::SendError> + Unpin,
    stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    config: Config,
) -> Result<(), SequencingError> {
    // stream interactions are on separate task to avoid blocking command sender

    let mut next_seq = 0u16;
    let mut buffed_count = 0u16;
    let (mut data_tx, data_rx) = futures::channel::mpsc::channel(config.max_data_ahead as usize);
    let (local_evt_tx, mut local_evt_rx) =
        futures::channel::mpsc::channel(config.max_data_ahead as usize);

    let mut heap = std::collections::BinaryHeap::<std::cmp::Reverse<StreamEntry>>::with_capacity(
        super::MAX_DATA_AHEAD as usize,
    );

    let main_loop = async move {
        loop {
            tokio::select! {
                    cmd = cmd_rx.next().instrument(debug_span!("receive command")) => {
                        let cmd = match cmd {
                            Some(cmd) => cmd,
                            None => {
                                error!("command channel is broken");
                                return Ok::<_, SequencingError>(());
                            }
                        };

                        info!(cmd = ?cmd, "command received");

                        buffed_count += 1;
                        if buffed_count > super::MAX_DATA_AHEAD {
                            panic!("too many data cached");
                        }

                        match cmd {
                            Command::Data(data) => {
                                heap.push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));
                            }
                            Command::Eof(eof) => {
                                heap.push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));
                            }
                        };

                        while heap.peek().map(|e| e.0.0 == next_seq).unwrap_or(false) {
                            if let Err(_) = data_tx
                                .send(heap.pop().unwrap().0)
                                    .instrument(debug_span!("send data"))
                                    .await
                            {
                                error!("local stream channel is broken");
                                return Ok(());
                            }

                            next_seq += 1;
                        }
                    },
                    evt = local_evt_rx.next().instrument(debug_span!("receive local event")) => {
                        let evt = match evt {
                            Some(evt) => evt,
                            None => {
                                debug!("end of local event channel, exiting");
                                return Ok::<_, SequencingError>(());
                            }
                        };

                        match &evt {
                            Event::Ack(_) => {
                                buffed_count -= 1;
                            }
                        };

                        info!(evt = ?evt, "local event received");
                        try_emit_evt(&mut event_tx, evt).await?;
                    }
            }
        }
    };

    tokio::try_join!(
        main_loop.instrument(debug_span!("main loop")),
        streaming_loop(stream_to_write, data_rx, local_evt_tx,)
            .instrument(debug_span!("streaming loop")),
    )
    .map(|_| ())
}

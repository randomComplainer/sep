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

#[derive(Debug)]
struct State {
    // stored value is actual max acked + 1
    // 0 is reserved for no ack yet
    max_acked: u16,

    eof_seq: Option<u16>,
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

        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State {
            max_acked: 0,
            eof_seq: None,
        });

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

        let receving_acks = {
            let state_tx = state_tx.clone();
            let mut state_rx = state_rx.clone();
            async move {
                loop {
                    match futures::future::select(
                        cmd_rx.next(),
                        state_rx
                            .wait_for(|state| {
                                state
                                    .eof_seq
                                    .map(|eof_seq| eof_seq + 1 == state.max_acked)
                                    .unwrap_or(false)
                            })
                            .boxed(),
                    )
                    .await
                    {
                        future::Either::Left((cmd, _)) => {
                            if let Some(cmd) = cmd {
                                match cmd {
                                    Command::Ack(ack) => {
                                        state_tx.send_if_modified(|old| {
                                            let new = ack.seq + 1;
                                            if new > old.max_acked {
                                                old.max_acked = new;
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
                        future::Either::Right(_) => {
                            debug!("ack of eof reached");
                            return Ok(());
                        }
                    };
                }
            }
        }
        .instrument(info_span!("receive acks"));

        let stream_reading = {
            let mut state_rx = state_rx.clone();
            async move {
                loop {
                    state_rx
                        .wait_for(|state| {
                            seq - state.max_acked < crate::session::MAX_DATA_AHEAD + 1
                        })
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
                            state_tx.send_modify(|old| {
                                old.eof_seq = Some(seq);
                            });
                            info!(seq = seq, "eof reached, streaming exits");

                            // TODO: do I need to wait for remaining acks before exiting?
                            return Ok(());
                        }
                    };

                    seq += 1;
                }
            }
        }
        .instrument(info_span!("read from stream"));

        tokio::try_join!(receving_acks, stream_reading).map(|_| ())
    }
}


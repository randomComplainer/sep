use derive_more::From;
use futures::{SinkExt, StreamExt, prelude::*};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use crate::sequence::{StreamEntry, StreamEntryValue};

use crate::ok_or;
use crate::protocol::msg::session as msg;

#[derive(Debug, From)]
pub enum Command {
    Data(#[from] msg::Data),
    Eof(#[from] msg::Eof),
}

#[derive(Debug, From)]
pub enum Event {
    Ack(#[from] msg::Ack),
    EofAck(#[from] msg::EofAck),
}

mod state {
    use std::collections::BinaryHeap;

    use super::*;

    #[derive(Debug, From)]
    pub enum Cmd {
        Data(#[from] msg::Data),
        Eof(#[from] msg::Eof),
        Wrote(#[from] u32),
        AllWrote,
    }

    #[derive(Debug, From)]
    pub enum Action {
        Data(#[from] msg::Data),
        Ack(#[from] msg::Ack),
        Eof,
        Done,
    }

    pub struct State {
        next_seq: u16,
        buffed_bytes: u32,
        buffed_entries: BinaryHeap<std::cmp::Reverse<StreamEntry>>,
    }

    impl State {
        pub fn new() -> Self {
            Self {
                next_seq: 0,
                buffed_bytes: 0,
                buffed_entries: Default::default(),
            }
        }

        pub fn on_cmd(&mut self, cmd: Cmd) -> Vec<Action> {
            match cmd {
                Cmd::Data(data) => self.on_data(data),
                Cmd::Eof(eof) => self.on_eof(eof),
                Cmd::Wrote(len) => self.on_wrote(len),
                Cmd::AllWrote => self.on_all_wrote(),
            }
        }

        fn on_data(&mut self, data: msg::Data) -> Vec<Action> {
            let len = data.data.as_ref().len() as u32;
            self.buffed_bytes += len;

            self.buffed_entries
                .push(std::cmp::Reverse(StreamEntry::data(data.seq, data.data)));

            self.try_get_ordered_data()
        }

        fn on_eof(&mut self, eof: msg::Eof) -> Vec<Action> {
            self.buffed_entries
                .push(std::cmp::Reverse(StreamEntry::eof(eof.seq)));

            self.try_get_ordered_data()
        }

        fn on_wrote(&mut self, len: u32) -> Vec<Action> {
            self.buffed_bytes -= len;
            Vec::from([Action::Ack(msg::Ack { bytes: len })])
        }

        fn on_all_wrote(&mut self) -> Vec<Action> {
            assert_eq!(0, self.buffed_bytes);
            assert_eq!(0, self.buffed_entries.len());
            Vec::from([Action::Done])
        }

        fn try_get_ordered_data(&mut self) -> Vec<Action> {
            let mut result: Vec<Action> = Default::default();

            while self
                .buffed_entries
                .peek()
                .map(|e| e.0.0 == self.next_seq)
                .unwrap_or(false)
            {
                self.next_seq += 1;
                let entry = self.buffed_entries.pop().unwrap();
                let seq = entry.0.0;

                match entry.0.1 {
                    StreamEntryValue::Data(buf) => {
                        result.push(msg::Data { seq, data: buf }.into());
                    }
                    StreamEntryValue::Eof => {
                        assert_eq!(0, self.buffed_entries.len());

                        result.push(Action::Eof);
                    }
                };
            }

            result
        }
    }
}

#[derive(From)]
enum EntryToWrite {
    Data(#[from] msg::Buf),
    Eof,
}

enum WroteEvt {
    Wrote(u32),
    AllWrote,
}

async fn create_writing_task(
    mut stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
    mut entries_rx: impl Stream<Item = EntryToWrite> + Unpin + Send + 'static,
    mut wrote_tx: impl Sink<WroteEvt> + Unpin + Send + Clone + 'static,
) -> std::io::Result<()> {
    use WroteEvt::*;

    while let Some(entry) = entries_rx.next().await {
        match entry {
            EntryToWrite::Data(buf) => {
                let len = buf.as_ref().len() as u32;

                stream_to_write
                    .write_all(buf.as_ref())
                    .await
                    .inspect_err(|err| tracing::error!(?err, "stream write error"))?;

                ok_or!(wrote_tx.send(Wrote(len)).await, return Ok(()));
            }
            EntryToWrite::Eof => {
                ok_or!(wrote_tx.send(AllWrote).await, return Ok(()));
                return Ok(());
            }
        };
    }

    Ok(())
}

pub async fn run(
    cmd_rx: impl Stream<Item = Command> + Unpin + Send + 'static,
    mut evt_tx: impl Sink<Event> + Unpin + Send + Clone + 'static,
    stream_to_write: impl AsyncWrite + Unpin + Send + 'static,
) -> Result<(), std::io::Error> {
    let (mut entries_tx, entries_rx) = futures::channel::mpsc::unbounded();
    let (wrote_tx, wrote_rx) = futures::channel::mpsc::unbounded();

    let io_task = create_writing_task(stream_to_write, entries_rx, wrote_tx);

    let mut state_cmd_stream = futures::stream::select(
        wrote_rx.then(|evt| {
            std::future::ready(match evt {
                WroteEvt::Wrote(n) => state::Cmd::Wrote(n),
                WroteEvt::AllWrote => state::Cmd::AllWrote,
            })
        }),
        cmd_rx.then(|cmd| {
            std::future::ready(match cmd {
                Command::Data(data) => state::Cmd::Data(data),
                Command::Eof(eof) => state::Cmd::Eof(eof),
            })
        }),
    );

    let state_task = async move {
        let mut state = state::State::new();
        while let Some(cmd) = state_cmd_stream.next().await {
            let actions = state.on_cmd(cmd);
            for action in actions {
                match action {
                    state::Action::Data(data) => {
                        ok_or!(entries_tx.send(data.data.into()).await, return);
                    }
                    state::Action::Eof => {
                        ok_or!(entries_tx.send(EntryToWrite::Eof).await, return);
                    }
                    state::Action::Ack(ack) => {
                        ok_or!(evt_tx.send(ack.into()).await, return);
                    }
                    state::Action::Done => return,
                };
            }
        }
    };

    match future::select(Box::pin(io_task), Box::pin(state_task)).await {
        future::Either::Left((io_result, state_task)) => {
            io_result?;
            state_task.await;
            Ok(())
        }
        future::Either::Right(_) => Ok(()),
    }
}

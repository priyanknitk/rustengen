use dashmap::DashMap;
use rustengan::{main_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::mpsc::Sender};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        #[serde(rename = "msg")]
        message: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        #[serde(rename = "msgs")]
        messages: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

struct KafkaNode {
    id: usize,
    log_map: DashMap<String, Vec<(usize, usize)>>,
    committed_offsets: DashMap<String, usize>,
    offset: usize,
}

impl Node<(), Payload, ()> for KafkaNode {
    fn from_init(_state: (), _init: Init, _tx: Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1,
            log_map: DashMap::new(),
            committed_offsets: DashMap::new(),
            offset: 0,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event: {:?}", input);
        };
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Send { key, message } => {
                let mut log = self.log_map.entry(key).or_insert_with(Vec::new);
                log.push((self.offset, message));
                reply.body.payload = Payload::SendOk {
                    offset: self.offset,
                };
                reply.send(output)?;
                self.offset += 1;
            }
            Payload::Poll { offsets } => {
                let mut messages = HashMap::new();
                for (key, offset) in offsets {
                    let msgs = self
                        .log_map
                        .entry(key.clone())
                        .or_insert(Vec::new());
                    // Find the first message with the offset using binary search
                    let index = match msgs.binary_search_by_key(&offset, |&(offset, _)| offset) {
                        Ok(index) => index,
                        Err(index) => index,
                    };
                    let msgs = msgs[index..].to_vec();
                    messages.insert(key, msgs);
                }
                reply.body.payload = Payload::PollOk { messages };
                reply.send(output)?;
            }
            Payload::CommitOffsets { offsets } => {
                for (key, offset) in offsets {
                    self.committed_offsets.entry(key).and_modify(|value| {
                        *value = offset;
                    }).or_insert(offset);
                }
                reply.body.payload = Payload::CommitOffsetsOk;
                reply.send(output)?;
            }
            Payload::ListCommittedOffsets { keys } => {
                let offsets = keys
                    .iter()
                    .map(|key| {
                        if let Some(log) = self.log_map.get(key) {
                            let offset = log.last().map(|(offset, _)| *offset).unwrap_or(0);
                            Some((key.clone(), offset))
                        } else {
                            None
                        }
                    })
                    .filter_map(|item| {
                        if let Some(item) = item {
                            Some(item)
                        } else {
                            None
                        }
                    })
                    .collect();
                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                reply.send(output)?;
            }
            Payload::SendOk { offset: _ }
            | Payload::PollOk { messages: _ }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { offsets: _ } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaNode, _, _>(())
}

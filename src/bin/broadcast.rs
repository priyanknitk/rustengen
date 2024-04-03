use std::{collections::HashMap, io::Write};

use rustengan::{main_loop, Init, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>
    },
    TopologyOk,
}

struct BroadcastNode {
    id: usize,
    messages: Vec<usize>,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_state: (), _init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1,
            messages: Vec::new(),
        })
    }
    
    fn step(&mut self, input: rustengan::Message<Payload>, output: &mut std::io::StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);
                let reply = rustengan::Message {
                    src: input.dst,
                    dst: input.src,
                    body: rustengan::Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::BroadcastOk,
                    },
                };
                self.id += 1;
                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n")?;
            }
            Payload::Read => {
                let reply = rustengan::Message {
                    src: input.dst,
                    dst: input.src,
                    body: rustengan::Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::ReadOk {
                            messages: self.messages.clone(),
                        },
                    },
                };
                self.id += 1;
                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n")?;
            }
            Payload::Topology { topology: _ } => {
                let reply = rustengan::Message {
                    src: input.dst,
                    dst: input.src,
                    body: rustengan::Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::TopologyOk,
                    },
                };
                self.id += 1;
                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n")?;
            }
            _ => {}
        }
        Ok(())
    }
}


fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
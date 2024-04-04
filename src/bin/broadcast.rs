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
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct BroadcastNode {
    id: usize,
    messages: Vec<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_state: (), _init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1,
            messages: Vec::new(),
            topology: HashMap::new(),
        })
    }

    fn step(
        &mut self,
        input: rustengan::Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                // if let Some(neighbor_ids) = self.topology.get(&input.dst) {
                //     if !self.messages.contains(&message) {
                //         let neighbor_ids = neighbor_ids
                //             .iter()
                //             .filter(|&neighbor_id| *neighbor_id != input.src);
                //         for neighbor_id in neighbor_ids {
                //             let message = rustengan::Message {
                //                 src: input.dst.clone(),
                //                 dst: neighbor_id.clone(),
                //                 body: rustengan::Body {
                //                     id: Some(self.id),
                //                     in_reply_to: input.body.id,
                //                     payload: Payload::Broadcast { message },
                //                 },
                //             };
                //             self.id += 1;
                //             serde_json::to_writer(&mut *output, &message)?;
                //             output.write_all(b"\n")?;
                //         }
                //     }
                // };

                self.messages.push(message);
                reply.body.payload = Payload::BroadcastOk;
                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n")?;
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n")?;
            }
            Payload::Topology { topology } => {
                self.topology = topology;
                reply.body.payload = Payload::TopologyOk;
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

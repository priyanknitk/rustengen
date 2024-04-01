use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk {
        id: usize,
    }
}

struct Node {
    id: String,
    node_ids: Vec<String>,
    msg_id: usize,
}

impl Node {
    pub fn new() -> Self {
        Node {
            id: "".to_string(),
            node_ids: vec![],
            msg_id: 0,
        }
    }

    pub fn step(
        &mut self,
        input: Message,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init {
                node_id,
                node_ids,
            } => {
                self.id = node_id;
                self.node_ids = node_ids;
                let reply = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    },
                };
                self.msg_id += 1;
                serde_json::to_writer(&mut *output, &reply).context("serialize response to init")?;
                output.write_all(b"\n").context("write newline")?;
            }
            Payload::Echo { echo } => {
                let reply = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                self.msg_id += 1;
                serde_json::to_writer(&mut *output, &reply).context("serialize response to echo")?;
                output.write_all(b"\n").context("write newline")?;
            }
            Payload::Generate => {
                let reply = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk { id: self.msg_id },
                    },
                };
                self.msg_id += 1;
                serde_json::to_writer(&mut *output, &reply).context("serialize response to generate")?;
                output.write_all(b"\n").context("write newline")?;
            }
            Payload::InitOk => bail!("Unexpected InitOk message"),
            _ => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut stdout = std::io::stdout().lock();
    let mut state: Node = Node::new();
    for input in inputs {
        let input = input?;
        state
            .step(input, &mut stdout)
            .context("Failed to process message")?;
    }
    Ok(())
}

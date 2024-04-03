use std::io::{BufRead, StdoutLock, Write};

use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

pub trait Node<S, Payload> {
    fn from_init(state: S, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned,
    N: Node<S, P>,
{
    let mut stdin = std::io::stdin().lock().lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("No init message received")
            .context("Failed to read init message")?,
    )
    .context("Failed to read init message")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("Expected Init message, got {:?}", init_msg.body.payload);
    };

    let mut node: N = Node::from_init(init_state, init).context("Failed to initialize node")?;

    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };
    serde_json::to_writer(&mut stdout, &reply).context("serialize response to init")?;
    stdout.write_all(b"\n").context("write newline")?;

    for line in stdin {
        let line = line?;
        let input: Message<P> = serde_json::from_str(&line).context("Failed to parse message")?;
        node.step(input, &mut stdout)
            .context("Failed to process message")?;
    }
    Ok(())
}

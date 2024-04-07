use dashmap::DashMap;
use rustengan::{main_loop, Event, Init, Node};
use serde::{Deserialize, Serialize};
use std::sync::mpsc::Sender;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    #[serde(rename = "txn")]
    Transaction {
        #[serde(rename = "txn")]
        transaction: Vec<(String, usize, Option<usize>)>,
    },
    #[serde(rename = "txn_ok")]
    TransactionOk {
        #[serde(rename = "txn")]
        transaction: Vec<(String, usize, Option<usize>)>,
    },
}

struct KeyValueStoreNode {
    id: usize,
    kv_store: DashMap<usize, usize>,
}

impl Node<(), Payload, ()> for KeyValueStoreNode {
    fn from_init(_state: (), _init: Init, _tx: Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1,
            kv_store: DashMap::new(),
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
            Payload::Transaction { transaction } => {
                let mut transaction_result: Vec<(String, usize, Option<usize>)> = vec![];
                transaction
                    .into_iter()
                    .for_each(|(op, key, value)| match op.as_str() {
                        "w" => {
                            let value = value.expect("value is required for write operation");
                            self.kv_store.insert(key, value);
                            transaction_result.push((op, key, Some(value)));
                        }
                        "r" => {
                            let value = self.kv_store.get(&key).map(|res| *res);
                            transaction_result.push((op, key, value));
                        }
                        _ => {
                            panic!("unknown operation: {}", op);
                        }
                    });
                reply.body.payload = Payload::TransactionOk {
                    transaction: transaction_result,
                };
                reply.send(output)?;
            }
            Payload::TransactionOk { transaction: _ } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KeyValueStoreNode, _, _>(())
}

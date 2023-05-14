use rand::Rng;
use rasengan::*;

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: MessageID,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<MessageID>,
    },
    Topology {
        topology: HashMap<NodeID, Vec<NodeID>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<MessageID>,
    },
}

enum InjectedPayload {
    Gossip,
}

struct BroadcastNode {
    node: NodeID,
    id: usize,
    messages: HashSet<MessageID>,
    known: HashMap<NodeID, HashSet<MessageID>>,
    neighbors: Vec<NodeID>,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        // Periodically gossip to other nodes
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if let Err(_) = _tx.send(Event::Injected(InjectedPayload::Gossip)) {
                break;
            }
        });

        Ok(Self {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            known: init
                .node_ids
                .iter()
                .map(|id| (id.clone(), HashSet::new()))
                .collect(),
            neighbors: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Shutdown => {}
            Event::Injected(InjectedPayload::Gossip) => {
                for neighbor in &self.neighbors {
                    let known_to_neighbor = self.known.get(neighbor).unwrap();
                    let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = self
                        .messages
                        .iter()
                        .copied()
                        .partition(|m| known_to_neighbor.contains(m));
                    let mut rng = rand::thread_rng();
                    let additional_cap = (known_to_neighbor.len() as f64 * 0.1) as usize;
                    notify_of.extend(already_known.iter().filter(|_| {
                        rng.gen_ratio(
                            additional_cap.min(already_known.len()) as u32,
                            already_known.len() as u32,
                        )
                    }));
                }
            }
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(output)?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply.send(output)?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighbors = topology.remove(&self.node).unwrap_or(Vec::new());
                        reply.body.payload = Payload::TopologyOk;
                        reply.send(output)?;
                    }
                    Payload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dst)
                            .expect("msg from unknown node")
                            .extend(seen);
                    }
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
                }
            }
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}

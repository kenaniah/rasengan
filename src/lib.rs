use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, StdoutLock, Write},
};

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    Shutdown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    /// Converts a message into a reply to the given message
    pub fn into_reply(self, id: Option<&mut MessageID>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|id| {
                    *id += 1;
                    *id
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    /// Send a message to the given output stream
    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, self)?;
        writeln!(output)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<MessageID>,
    pub in_reply_to: Option<MessageID>,
    #[serde(flatten)]
    pub payload: Payload,
}

pub type NodeID = String;
pub type MessageID = usize;

pub trait Node<State, Payload, InjectedPayload = ()> {
    fn from_init(
        state: State,
        init: Init,
        inject: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

pub fn main_loop<State, NodeType, Payload, InjectedPayload>(init_state: State) -> anyhow::Result<()>
where
    Payload: DeserializeOwned + Send + 'static,
    NodeType: Node<State, Payload, InjectedPayload>,
    InjectedPayload: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();

    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("no init message received")
            .context("failed to read init message from stdin")?,
    )
    .context("init message could not be deserialized")?;
    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be init");
    };
    let mut node: NodeType =
        Node::from_init(init_state, init, tx.clone()).context("node initialization failed")?;

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
    stdout.write_all(b"\n").context("write trailing newline")?;

    drop(stdin);
    let jh = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.context("Maelstrom input from STDIN could not be read")?;
            let input: Message<Payload> = serde_json::from_str(&line)
                .context("Maelstrom input from STDIN could not be deserialized")?;
            if let Err(_) = tx.send(Event::Message(input)) {
                return Ok::<_, anyhow::Error>(());
            }
        }
        let _ = tx.send(Event::Shutdown);
        Ok(())
    });

    for input in rx {
        node.step(input, &mut stdout)
            .context("Node step function failed")?;
    }

    jh.join()
        .expect("stdin thread panicked")
        .context("stdin thread err'd")?;

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Init {
    /// The current node's ID
    pub node_id: NodeID,
    /// The IDs of all other nodes in the network
    pub node_ids: Vec<NodeID>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    /// Initialization message given to each node at startup
    Init(Init),
    InitOk,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum BroadcastPayload {
    /// Sent after initialization to provide the network topology to use for broadcasting
    /// Consists of a map of node IDs to the IDs of their neighbors
    Topology {
        topology: HashMap<NodeID, Vec<NodeID>>,
    },
    TopologyOk,
    Broadcast {
        message: MessageID,
    },
    BroadcastOk,
    /// Requests all messages present on a node
    Read,
    ReadOk {
        messages: HashSet<MessageID>,
    },
}

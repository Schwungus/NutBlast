use std::{collections::HashSet, net::IpAddr, sync::mpsc, time::Instant};

use eventloop::BlasterEventLoop;
use serde::Deserialize;

use crate::{
    id::{BasicId, GameId, LobbyId},
    protocol::{
        payloads::{Kick, LobbyListing, ServerMessage},
        utils::Metadata,
    },
    tokens::TokenBucket,
};

mod eventloop;

enum PeerSessionCount {
    Some(usize),
    Decaying(Instant),
}

struct Peer {
    session_count: PeerSessionCount,
    ops: TokenBucket,
}

impl Peer {
    fn new() -> Self {
        Self {
            session_count: PeerSessionCount::Some(1),
            ops: TokenBucket::new(2.0, 3.0, 4.0),
        }
    }

    fn session_count(&self) -> usize {
        match self.session_count {
            PeerSessionCount::Some(count) => count,
            PeerSessionCount::Decaying(_) => 0,
        }
    }
}

#[derive(Clone)]
struct Lobby {
    players: HashSet<BasicId>,
    initiator: Option<IpAddr>,
    master: BasicId,
    metadata: Metadata,
    capacity: usize,
    listed: bool,
    idle_since: Option<Instant>,
    created_at: Instant,
    alterations_budget: TokenBucket,
    metadata_budget: TokenBucket,
}

impl Lobby {
    fn is_full(&self) -> bool {
        self.players.len() >= self.capacity
    }
}

#[derive(Clone)]
struct Player {
    ip: IpAddr,
    lid: LobbyId,
    metadata: Metadata,
    sender: TokioSender,
    birth: u128,
    metadata_budget: TokenBucket,
}

impl Player {
    fn send(&mut self, msg: ServerMessage) {
        let _ = self.sender.send(msg);
    }
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub ice_servers: Vec<String>,
}

#[derive(Clone)]
pub struct Blaster {
    channel: mpsc::Sender<BlasterOperation>,
}

impl Blaster {
    pub fn new(config: Config) -> Self {
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let mut event_loop = BlasterEventLoop::new(config.clone());

            while let Ok(msg) = rx.recv() {
                let recv = std::panic::AssertUnwindSafe(|| event_loop.recv(msg));

                if let Err(e) = std::panic::catch_unwind(recv) {
                    error!("EVENT-LOOP RESET!!! {e:?}");
                    event_loop = BlasterEventLoop::new(config.clone());
                }
            }
        });

        Self { channel: tx }
    }

    pub fn execute(&self, operation: BlasterOperation) {
        let _ = self.channel.send(operation);
    }

    pub fn introduce_session(&self, ip: IpAddr) -> bool {
        let (tx, rx) = mpsc::channel();
        self.execute(BlasterOperation::IntroduceSession { ip, tx });
        rx.recv().unwrap_or(false)
    }

    pub async fn close_session(&self, ip: IpAddr) {
        self.execute(BlasterOperation::CloseSession { ip });
    }
}

pub type TokioSender = tokio::sync::mpsc::UnboundedSender<ServerMessage>;
pub type TokioReceiver = tokio::sync::mpsc::UnboundedReceiver<ServerMessage>;

pub enum BlasterOperation {
    SetCapacity {
        initiator: BasicId,
        capacity: usize,
    },
    SetListed {
        initiator: BasicId,
        listed: bool,
    },
    SetMaster {
        initiator: BasicId,
        new_master: BasicId,
    },
    SetPlayerMeta {
        pid: BasicId,
        key: String,
        value: String,
    },
    ErasePlayerMeta {
        pid: BasicId,
        key: String,
    },
    SetLobbyMeta {
        initiator: BasicId,
        key: String,
        value: String,
    },
    EraseLobbyMeta {
        initiator: BasicId,
        key: String,
    },
    Relay {
        from: BasicId,
        to: BasicId,
        msg: ServerMessage,
    },
    ListLobbies {
        ip: IpAddr,
        gid: GameId,
        limit: usize,
        tx: mpsc::Sender<LobbyListing>,
    },
    HostLobby {
        initiator: IpAddr,
        lid: LobbyId,
        master: BasicId,
        lobby_meta: Metadata,
        capacity: usize,
        listed: bool,
        pid: BasicId,
        player_meta: Metadata,
        sender: TokioSender,
    },
    JoinLobby {
        ip: IpAddr,
        pid: BasicId,
        lid: LobbyId,
        player_meta: Metadata,
        sender: TokioSender,
    },
    KickPlayer {
        kicker: BasicId,
        kickee: BasicId,
    },
    RemovePlayer {
        pid: BasicId,
        reason: Option<Kick>,
    },
    IntroduceSession {
        ip: IpAddr,
        tx: mpsc::Sender<bool>,
    },
    Prune,
    CloseSession {
        ip: IpAddr,
    },
}

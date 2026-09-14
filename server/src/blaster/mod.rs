use std::{
    collections::HashSet,
    net::IpAddr,
    sync::mpsc,
    time::{Duration, Instant},
};

use eventloop::BlasterEventLoop;
use serde::Deserialize;
use tokio::sync::oneshot;

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

const CHUD_LOBBY_TIMEOUT: Duration = Duration::from_mins(3);

#[derive(Clone)]
struct Lobby {
    players: HashSet<BasicId>,
    initiator: Option<IpAddr>,
    master: BasicId,
    metadata: Metadata,
    capacity: usize,
    listed: bool,
    death_timer: Option<Instant>,
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
    queue: Vec<ServerMessage>,
    kick_me_now: Option<Kick>,
    birth: u128,
    metadata_budget: TokenBucket,
}

impl Player {
    fn send(&mut self, msg: ServerMessage) {
        const QUEUE_CAP: usize = 120;

        if matches!(msg, ServerMessage::Disconnected { .. }) || self.queue.len() < QUEUE_CAP {
            self.queue.push(msg);
        }
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
            let mut event_loop = BlasterEventLoop::new(config);

            while let Ok(msg) = rx.recv() {
                event_loop.recv(msg);
            }
        });

        Self { channel: tx }
    }

    pub fn execute(&self, operation: BlasterOperation) {
        let _ = self.channel.send(operation);
    }

    pub async fn introduce_session(&self, ip: IpAddr) -> bool {
        let (tx, rx) = oneshot::channel();
        self.execute(BlasterOperation::IntroduceSession { ip, tx });
        rx.await.unwrap_or(false)
    }

    pub async fn close_session(&self, ip: IpAddr) {
        self.execute(BlasterOperation::CloseSession { ip });
    }

    pub async fn is_kicked(&self, pid: &BasicId) -> Option<Kick> {
        let (tx, rx) = oneshot::channel();
        self.execute(BlasterOperation::IsKicked { pid: *pid, tx });
        rx.await.ok().and_then(|x| x)
    }
}

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
    JoinLobby {
        ip: IpAddr,
        pid: BasicId,
        lid: LobbyId,
        player_meta: Metadata,
        tx: oneshot::Sender<Result<(), Kick>>,
    },
    Relay {
        from: BasicId,
        to: BasicId,
        msg: ServerMessage,
    },
    ListLobbies {
        gid: GameId,
        limit: usize,
        tx: oneshot::Sender<Vec<LobbyListing>>,
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
        tx: oneshot::Sender<Result<(), Kick>>,
    },
    KickPlayer {
        kicker: BasicId,
        kickee: BasicId,
    },
    RemovePlayer {
        pid: BasicId,
        reason: Option<Kick>,
    },
    AdvanceLobbyTimer {
        pid: BasicId,
        tx: oneshot::Sender<Result<(), Kick>>,
    },
    FlushPlayerQueue {
        pid: BasicId,
        tx: oneshot::Sender<Vec<ServerMessage>>,
    },
    IsKicked {
        pid: BasicId,
        tx: oneshot::Sender<Option<Kick>>,
    },
    IntroduceSession {
        ip: IpAddr,
        tx: oneshot::Sender<bool>,
    },
    PruneStaleSessions,
    CloseSession {
        ip: IpAddr,
    },
    SignalPerIpCap {
        ip: IpAddr,
        tx: oneshot::Sender<bool>,
    },
}

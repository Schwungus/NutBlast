use std::{
    net::{IpAddr, SocketAddr},
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

struct Peer {
    session_count: usize,
    ops: TokenBucket,
}

impl Peer {
    const SESSION_OPS_RATE: f32 = 2.0;
    const SESSION_OPS_BURST: f32 = 4.0;

    fn new() -> Self {
        Self {
            session_count: 1,
            ops: TokenBucket::new(Self::SESSION_OPS_RATE, Self::SESSION_OPS_BURST),
        }
    }
}

const CHUD_LOBBY_TIMEOUT: Duration = Duration::from_mins(3);

#[derive(Clone)]
struct Lobby {
    player_count: usize,
    initiator: Option<IpAddr>,
    master: BasicId,
    meta: Metadata,
    capacity: usize,
    listed: bool,
    death_timer: Option<Instant>,
}

#[derive(Clone)]
struct Player {
    ip: IpAddr,
    lid: LobbyId,
    meta: Metadata,
    queue: Vec<ServerMessage>,
    kick_me_now: Option<Kick>,
}

impl Player {
    const QUEUE_CAP: usize = 30;

    fn send(&mut self, msg: ServerMessage) {
        if self.queue.len() < Self::QUEUE_CAP {
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

    pub async fn introduce_session(&self, addr: &SocketAddr) -> bool {
        let (tx, rx) = oneshot::channel();
        self.execute(BlasterOperation::IntroduceSession { ip: addr.ip(), tx });
        rx.await.unwrap_or(false)
    }

    pub async fn close_session(&self, addr: &SocketAddr) {
        self.execute(BlasterOperation::CloseSession { ip: addr.ip() });
    }

    pub async fn is_kicked(&self, pid: &BasicId) -> Option<Kick> {
        let (tx, rx) = oneshot::channel();
        self.execute(BlasterOperation::IsKicked { pid: *pid, tx });
        rx.await.ok().and_then(|x| x)
    }
}

pub enum BlasterOperation {
    CleanupLobbies,
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
    IntroducePlayer {
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
    InsertLobby {
        initiator: IpAddr,
        lid: LobbyId,
        master: BasicId,
        meta: Metadata,
        capacity: usize,
        listed: bool,
        tx: oneshot::Sender<Result<(), Kick>>,
    },
    KickPlayer {
        initiator: BasicId,
        pid: BasicId,
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
    CloseSession {
        ip: IpAddr,
    },
    SignalPerIpCap {
        ip: IpAddr,
        tx: oneshot::Sender<bool>,
    },
}

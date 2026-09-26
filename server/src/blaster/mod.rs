use std::{collections::HashSet, net::IpAddr, sync::mpsc, time::Instant};

use eventloop::BlasterEventLoop;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    protocol::{
        basic::{BasicId, GameId, LobbyId, Metadata},
        payloads::{Kick, LobbyListing, ServerMessage},
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
    sessions_budget: TokenBucket,
}

impl Peer {
    fn new() -> Self {
        Self {
            session_count: PeerSessionCount::Some(1),
            sessions_budget: TokenBucket::new("sessions_per_ip", 2.0, 3.0, 4.0),
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
    initiator: IpAddr,
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
    lid: LobbyId,
    metadata: Metadata,
    sender: TokioSender,
    birth: u128,
    metadata_budget: TokenBucket,
}

impl Player {
    fn send(&self, msg: ServerMessage) {
        let _ = self.sender.try_send(msg);
    }

    fn boot(&self, reason: Kick) {
        self.send(ServerMessage::Disconnected { reason });
    }

    fn try_take_from_budget(&self, budget: &mut TokenBucket, count: usize) -> bool {
        if let Err(reason) = budget.try_take(count) {
            self.boot(reason);
            false
        } else {
            true
        }
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Credentials {
    pub username: String,
    pub credential: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct IceServer {
    pub urls: String,
    #[serde(flatten)]
    pub creds: Option<Credentials>,
}

#[derive(Clone, Default, Deserialize)]
pub struct Config {
    pub ice_servers: Vec<IceServer>,
    pub trust_reverse_proxy_xff: Option<bool>,
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

    pub fn introduce_session(&self, ip: IpAddr) -> Option<SessionHandle> {
        let (tx, rx) = mpsc::channel();

        self.execute(BlasterOperation::IntroduceSession { ip, tx });

        if let Ok(true) = tokio::task::block_in_place(|| rx.recv()) {
            return Some(SessionHandle {
                blaster: self.clone(),
                ip,
            });
        }

        None
    }
}

#[must_use]
pub struct SessionHandle {
    blaster: Blaster,
    ip: IpAddr,
}

impl Drop for SessionHandle {
    fn drop(&mut self) {
        let op = BlasterOperation::CloseSession { ip: self.ip };
        self.blaster.execute(op);
    }
}

pub type TokioSender = tokio::sync::mpsc::Sender<ServerMessage>;
pub type TokioReceiver = tokio::sync::mpsc::Receiver<ServerMessage>;

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
        tx: oneshot::Sender<Result<Vec<LobbyListing>, Kick>>,
    },
    HostLobby {
        initiator: IpAddr,
        gid: GameId,
        lobby_meta: Metadata,
        capacity: usize,
        listed: bool,
        player_metadata: Metadata,
        sender: TokioSender,
        tx: oneshot::Sender<Result<BasicId, Kick>>,
    },
    JoinLobby {
        ip: IpAddr,
        lid: LobbyId,
        player_metadata: Metadata,
        sender: TokioSender,
        tx: oneshot::Sender<Result<BasicId, Kick>>,
    },
    KickPlayer {
        kicker: BasicId,
        kickee: BasicId,
    },
    RemovePlayer {
        pid: BasicId,
        reason: Kick,
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn introduce_session_in_tokio_runtime() {
        let blaster = Blaster::new(Config::default());
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        let handle = blaster.introduce_session(ip);
        assert!(handle.is_some());
    }
}

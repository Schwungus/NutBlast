use std::{
    net::SocketAddr,
    sync::mpsc,
    time::{Duration, Instant},
};

use eventloop::{BlasterEventLoop, BlasterOperation};
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
    const SESSION_OPS_PER_SEC: f32 = 2.0;
    const SESSION_OPS_BURST: f32 = 4.0;

    fn new() -> Self {
        Self {
            session_count: 1,
            ops: TokenBucket::new(Self::SESSION_OPS_PER_SEC, Self::SESSION_OPS_BURST),
        }
    }
}

const MAX_LOBBIES_IN_LIST: usize = 100;
const CHUD_LOBBY_TIMEOUT: Duration = Duration::from_mins(3);

#[derive(Clone)]
struct Lobby {
    master: BasicId,
    meta: Metadata,
    capacity: usize,
    listed: bool,
    death_timer: Option<Instant>,
}

#[derive(Clone)]
pub struct Player {
    lid: LobbyId,
    meta: Metadata,
    queue: Vec<ServerMessage>,
    kick_me_now: Option<Kick>,
}

impl Player {
    pub fn send(&mut self, msg: ServerMessage) {
        self.queue.push(msg);
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

    pub async fn is_kicked(&self, pid: &BasicId) -> Option<Kick> {
        let (tx, rx) = oneshot::channel();

        let _ = self
            .channel
            .send(BlasterOperation::IsKicked { pid: *pid, tx });

        rx.await.ok().and_then(|x| x)
    }

    pub async fn set_player_meta(&self, pid: BasicId, key: &str, value: &str) {
        let _ = self.channel.send(BlasterOperation::SetPlayerMeta {
            pid,
            key: key.to_string(),
            value: value.to_string(),
        });
    }

    pub async fn erase_player_meta(&self, pid: BasicId, key: &str) {
        let _ = self.channel.send(BlasterOperation::ErasePlayerMeta {
            pid,
            key: key.to_string(),
        });
    }

    pub async fn set_lobby_capacity(&self, lid: &LobbyId, capacity: usize) {
        let _ = self.channel.send(BlasterOperation::SetLobbyCapacity {
            lid: lid.clone(),
            capacity,
        });
    }

    pub async fn set_lobby_listed(&self, lid: &LobbyId, listed: bool) {
        let _ = self.channel.send(BlasterOperation::SetLobbyListed {
            lid: lid.clone(),
            listed,
        });
    }

    pub async fn set_lobby_meta(&self, lid: &LobbyId, key: &str, value: &str) {
        let _ = self.channel.send(BlasterOperation::SetLobbyMeta {
            lid: lid.clone(),
            key: key.to_string(),
            value: value.to_string(),
        });
    }

    pub async fn erase_lobby_meta(&self, lid: &LobbyId, key: &str) {
        let _ = self.channel.send(BlasterOperation::EraseLobbyMeta {
            lid: lid.clone(),
            key: key.to_string(),
        });
    }

    pub async fn kick_player(&self, lid: &LobbyId, pid: BasicId) {
        let _ = self.channel.send(BlasterOperation::KickPlayer {
            lid: lid.clone(),
            pid,
        });
    }

    pub async fn introduce_player(
        &self,
        pid: BasicId,
        lid: &LobbyId,
        player_meta: Metadata,
    ) -> Result<(), Kick> {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::IntroducePlayer {
            pid,
            lid: lid.clone(),
            player_meta,
            tx,
        });

        rx.await.unwrap_or(Ok(()))
    }

    pub async fn master_of(&self, lid: &LobbyId) -> Option<BasicId> {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::MasterOf {
            lid: lid.clone(),
            tx,
        });

        rx.await.unwrap_or(None)
    }

    pub async fn relay(&self, from: BasicId, to: BasicId, msg: ServerMessage) {
        let msg = BlasterOperation::Relay { from, to, msg };
        let _ = self.channel.send(msg);
    }

    pub async fn set_lobby_master(&self, initiator_pid: BasicId, new_master_pid: BasicId) {
        let _ = self.channel.send(BlasterOperation::SetLobbyMaster {
            initiator_pid,
            new_master_pid,
        });
    }

    pub async fn list_lobbies(&self, gid: &GameId, limit: usize) -> Vec<LobbyListing> {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::ListLobbies {
            gid: gid.clone(),
            limit,
            tx,
        });

        rx.await.unwrap_or_default()
    }

    pub async fn create_lobby(
        &self,
        lid: &LobbyId,
        master: BasicId,
        meta: Metadata,
        capacity: usize,
        listed: bool,
    ) -> Result<(), Kick> {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::InsertLobby {
            lid: lid.clone(),
            master,
            meta,
            capacity,
            listed,
            tx,
        });

        rx.await.unwrap_or(Ok(()))
    }

    pub async fn advance_lobby_timer(&self, lid: &LobbyId) -> Result<(), Kick> {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::AdvanceLobbyTimer {
            lid: lid.clone(),
            tx,
        });

        rx.await.unwrap_or(Ok(()))
    }

    pub async fn flush_player_queue(&self, pid: BasicId) -> Vec<ServerMessage> {
        let (tx, rx) = oneshot::channel();

        let msg = BlasterOperation::FlushPlayerQueue { pid, tx };
        let _ = self.channel.send(msg);

        rx.await.unwrap_or_default()
    }

    pub async fn remove_player(&self, pid: BasicId, reason: Option<Kick>) {
        let msg = BlasterOperation::RemovePlayer { pid, reason };
        let _ = self.channel.send(msg);
    }

    pub async fn cleanup_lobbies(&self) {
        let _ = self.channel.send(BlasterOperation::CleanupLobbies);
    }

    pub async fn introduce_session(&self, addr: &SocketAddr) -> bool {
        let (tx, rx) = oneshot::channel();

        let msg = BlasterOperation::IntroduceSession { ip: addr.ip(), tx };
        let _ = self.channel.send(msg);

        rx.await.unwrap_or(false)
    }

    pub async fn close_session(&self, addr: &SocketAddr) {
        let msg = BlasterOperation::CloseSession { ip: addr.ip() };
        let _ = self.channel.send(msg);
    }

    pub async fn signal_peer_op(&self, addr: &SocketAddr) -> bool {
        let (tx, rx) = oneshot::channel();

        let msg = BlasterOperation::SignalPeerOperation { ip: addr.ip(), tx };
        let _ = self.channel.send(msg);

        rx.await.unwrap_or(false)
    }
}

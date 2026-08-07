use std::{
    collections::{HashMap, HashSet},
    sync::mpsc,
    time::{Duration, Instant},
};

use indexmap::IndexMap;
use serde::Deserialize;
use tokio::sync::oneshot;

use crate::{
    MAX_FIELDS, MAX_PLAYERS,
    id::{BasicId, GameId, LobbyId},
    protocol::{Kick, LobbyListing, ServerMessage},
};

const MAX_LOBBIES_IN_LIST: usize = 100;
const CHUD_LOBBY_TIMEOUT: Duration = Duration::from_mins(10);

#[derive(Clone)]
pub struct Lobby {
    master: BasicId,
    meta: HashMap<String, String>,
    capacity: usize,
    listed: bool,
    swarm: bool,
    death_timer: Option<Instant>,
}

impl Lobby {
    pub fn ugly_new(
        master: BasicId,
        meta: HashMap<String, String>,
        capacity: usize,
        listed: bool,
    ) -> Self {
        Self {
            master,
            meta,
            capacity,
            listed,
            swarm: false,
            death_timer: None,
        }
    }

    pub fn new_swarm(master: BasicId, meta: HashMap<String, String>) -> Self {
        let mut lober = Self::ugly_new(master, meta, MAX_PLAYERS, false);
        lober.swarm = true;
        lober
    }
}

#[derive(Clone)]
pub struct Player {
    lid: LobbyId,
    meta: HashMap<String, String>,
    queue: Vec<ServerMessage>,
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

struct BlasterImpl {
    lobbies: HashMap<LobbyId, Lobby>,
    players: IndexMap<BasicId, Player>,
    config: Config,
}

impl BlasterImpl {
    fn players_in(&self, lid: &LobbyId) -> usize {
        let mut counter = 0;

        for (_, p) in self.players.iter() {
            if p.lid == *lid {
                counter += 1;
            }
        }

        counter
    }

    fn master_of(&mut self, lid: &LobbyId) -> Option<BasicId> {
        let empty = self.players_in(lid) == 0;
        let lobby = self.lobbies.get(lid)?.clone();

        if self.players.contains_key(&lobby.master) {
            Some(lobby.master)
        } else if empty {
            None
        } else {
            let new = *self.players.iter().find(|(_, p)| p.lid == *lid)?.0;
            self.lobbies.get_mut(lid)?.master = new;
            Some(new)
        }
    }

    fn send_to(&mut self, pid: &BasicId, msg: ServerMessage) {
        if let Some(player) = self.players.get_mut(pid) {
            player.send(msg);
        }
    }

    fn send_to_lobby(&mut self, lid: &LobbyId, msg: &ServerMessage) {
        for (_, player) in self.players.iter_mut() {
            if player.lid == *lid {
                player.send(msg.clone());
            }
        }
    }

    fn lobby_full(&self, lid: &LobbyId) -> bool {
        if let Some(ref lobby) = self.lobbies.get(lid) {
            return self.players_in(lid) >= lobby.capacity;
        } else {
            return false;
        }
    }

    fn recv(&mut self, msg: BlasterOperation) {
        match msg {
            BlasterOperation::SetLobbyCapacity { lid, capacity } => {
                if let Some(lobby) = self.lobbies.get_mut(&lid) {
                    lobby.capacity = capacity;

                    let msg = ServerMessage::SetCapacity { capacity };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetLobbyListed { lid, listed } => {
                if let Some(lobby) = self.lobbies.get_mut(&lid) {
                    lobby.listed = listed;

                    let msg = ServerMessage::SetListed { listed };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetPlayerMeta { pid, key, value } => {
                let lid = if let Some(player) = self.players.get_mut(&pid)
                    && (player.meta.contains_key(&key) || player.meta.len() < MAX_FIELDS)
                {
                    player.meta.insert(key.to_string(), value.to_string());
                    player.lid.clone()
                } else {
                    return;
                };

                let msg = ServerMessage::SetPlayerMeta {
                    pid,
                    key: key.to_string(),
                    value: value.to_string(),
                };

                self.send_to_lobby(&lid, &msg);
            }
            BlasterOperation::ErasePlayerMeta { pid, key } => {
                let lid = if let Some(player) = self.players.get_mut(&pid)
                    && player.meta.contains_key(&key)
                {
                    player.meta.remove(&key);
                    player.lid.clone()
                } else {
                    return;
                };

                let msg = ServerMessage::ErasePlayerMeta { pid, key };
                self.send_to_lobby(&lid, &msg);
            }
            BlasterOperation::SetLobbyMeta { lid, key, value } => {
                let Some(lober) = self.lobbies.get_mut(&lid) else {
                    return;
                };

                if lober.meta.contains_key(&key) || lober.meta.len() < MAX_FIELDS {
                    lober.meta.insert(key.to_string(), value.to_string());

                    let msg = ServerMessage::SetLobbyMeta {
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::EraseLobbyMeta { lid, key } => {
                let Some(lober) = self.lobbies.get_mut(&lid) else {
                    return;
                };

                if lober.meta.contains_key(&key) {
                    lober.meta.remove(&key);

                    let msg = ServerMessage::EraseLobbyMeta {
                        key: key.to_string(),
                    };

                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::IntroducePlayer {
                pid,
                lid,
                player_meta,
            } => {
                let Some(Lobby {
                    listed,
                    capacity,
                    meta: lobby_meta,
                    ..
                }) = self.lobbies.get(&lid).cloned()
                else {
                    return;
                };

                self.players.insert(
                    pid,
                    Player {
                        lid: lid.clone(),
                        meta: player_meta.clone(),
                        queue: Vec::new(),
                    },
                );

                let mastah = self.master_of(&lid);

                let pmeta: HashMap<_, _> = self
                    .players
                    .iter()
                    .filter_map(|(id, p)| {
                        if id != &pid && p.lid == lid {
                            Some((*id, p.meta.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();

                if let Some(player) = self.players.get_mut(&pid) {
                    player.send(ServerMessage::SetListed { listed });
                    player.send(ServerMessage::SetCapacity { capacity });

                    for (key, value) in lobby_meta {
                        player.send(ServerMessage::SetLobbyMeta { key, value });
                    }

                    for (&other, meta) in &pmeta {
                        player.send(ServerMessage::Joined {
                            pid: other,
                            meta: meta.clone(),
                        });
                    }

                    if let Some(mastah) = mastah {
                        player.send(ServerMessage::SetMaster { pid: mastah });
                    }

                    player.send(ServerMessage::Connected {
                        ice_servers: self.config.ice_servers.clone(),
                    });
                }

                for other in pmeta.keys() {
                    let msg = ServerMessage::Joined {
                        pid,
                        meta: player_meta.clone(),
                    };

                    self.send_to(other, msg);
                }
            }
            BlasterOperation::KickPlayer { lid, pid: kick_id } => {
                if let Some(guy) = self.players.get(&kick_id)
                    && guy.lid == lid
                {
                    let msg = ServerMessage::Disconnected {
                        reason: Kick::natural("kick", "Kicked by lobby's master"),
                    };

                    self.send_to(&kick_id, msg);
                }
            }
            BlasterOperation::RemovePlayer { lid, pid, reason } => {
                self.players.shift_remove(&pid);

                let left = ServerMessage::Left { pid, reason };
                self.send_to_lobby(&lid, &left);

                if let Some(mastah) = self.master_of(&lid) {
                    let msg = ServerMessage::SetMaster { pid: mastah };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetLobbyMaster {
                initiator_pid,
                new_master_pid,
            } => {
                let Some(lid) = self.players.get(&new_master_pid).map(|x| x.lid.clone()) else {
                    return;
                };

                if Some(initiator_pid) == self.master_of(&lid) && new_master_pid != initiator_pid {
                    self.lobbies
                        .get_mut(&lid)
                        .map(|l| l.master = new_master_pid);

                    let msg = ServerMessage::SetMaster {
                        pid: new_master_pid,
                    };

                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::ListLobbies { gid, limit, tx } => {
                let mut lobbies: HashMap<LobbyId, LobbyListing> = self
                    .lobbies
                    .iter()
                    .filter_map(|(lid, lobby)| {
                        if lid.gid != gid || !lobby.listed || lobby.swarm || self.lobby_full(lid) {
                            return None;
                        }

                        let lobby = LobbyListing {
                            lid: lid.lid,
                            max: lobby.capacity,
                            players: 0,
                            meta: lobby.meta.clone(),
                        };

                        Some((lid.clone(), lobby))
                    })
                    .take(limit.clamp(1, MAX_LOBBIES_IN_LIST))
                    .collect();

                for (_, player) in self.players.iter() {
                    if let Some(lober) = lobbies.get_mut(&player.lid) {
                        lober.players += 1;
                    }
                }

                let _ = tx.send(lobbies.into_values().collect());
            }
            BlasterOperation::AdvanceLobbyTimer { lid, tx } => {
                let chud = self.players_in(&lid) == 1;

                let Some(lobby) = self.lobbies.get_mut(&lid) else {
                    return;
                };

                let mut result = Ok(());

                if chud && let Some(start) = lobby.death_timer {
                    if Instant::now().duration_since(start) >= CHUD_LOBBY_TIMEOUT {
                        result = Err(Kick::natural("inactive_lobby", "Inactive lobby"));
                    }
                } else if chud {
                    lobby.death_timer = Some(Instant::now());
                } else {
                    lobby.death_timer = None;
                }

                let _ = tx.send(result);
            }
            BlasterOperation::FlushPlayerQueue { pid, tx } => {
                let _ = if let Some(player) = self.players.get_mut(&pid) {
                    let queue = player.queue.clone();
                    player.queue.clear();
                    tx.send(queue)
                } else {
                    tx.send(Vec::new())
                };
            }
            BlasterOperation::CleanupLobbies => {
                let mut nonempty = HashSet::new();

                for player in self.players.values() {
                    nonempty.insert(player.lid.clone());
                }

                self.lobbies.retain(move |k, l| {
                    if nonempty.contains(k) {
                        return true;
                    } else {
                        let noun = if l.swarm { "swarm" } else { "lober" };
                        info!("bye {noun}: {:?}", k);
                        return false;
                    }
                });
            }
            BlasterOperation::MasterOf { lid, tx } => {
                let _ = tx.send(self.master_of(&lid));
            }
            BlasterOperation::LobbyFull { lid, tx } => {
                let _ = tx.send(self.lobby_full(&lid));
            }
            BlasterOperation::SendTo { pid, msg } => {
                self.send_to(&pid, msg);
            }
            BlasterOperation::InsertLobby { lid, lobby } => {
                self.lobbies.insert(lid, lobby);
            }
            BlasterOperation::HasLobby { lid, tx } => {
                let _ = tx.send(self.lobbies.contains_key(&lid));
            }
            BlasterOperation::HasPlayer { pid, tx } => {
                let _ = tx.send(self.players.contains_key(&pid));
            }
            BlasterOperation::LobbyIsSwarm { lid, tx } => {
                let _ = tx.send(self.lobbies.get(&lid).map(|x| x.swarm).unwrap_or(false));
            }
        }
    }
}

enum BlasterOperation {
    CleanupLobbies,
    SetLobbyCapacity {
        lid: LobbyId,
        capacity: usize,
    },
    SetLobbyListed {
        lid: LobbyId,
        listed: bool,
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
        lid: LobbyId,
        key: String,
        value: String,
    },
    EraseLobbyMeta {
        lid: LobbyId,
        key: String,
    },
    MasterOf {
        lid: LobbyId,
        tx: oneshot::Sender<Option<BasicId>>,
    },
    HasLobby {
        lid: LobbyId,
        tx: oneshot::Sender<bool>,
    },
    HasPlayer {
        pid: BasicId,
        tx: oneshot::Sender<bool>,
    },
    LobbyFull {
        lid: LobbyId,
        tx: oneshot::Sender<bool>,
    },
    LobbyIsSwarm {
        lid: LobbyId,
        tx: oneshot::Sender<bool>,
    },
    IntroducePlayer {
        pid: BasicId,
        lid: LobbyId,
        player_meta: HashMap<String, String>,
    },
    SendTo {
        pid: BasicId,
        msg: ServerMessage,
    },
    SetLobbyMaster {
        initiator_pid: BasicId,
        new_master_pid: BasicId,
    },
    ListLobbies {
        gid: GameId,
        limit: usize,
        tx: oneshot::Sender<Vec<LobbyListing>>,
    },
    InsertLobby {
        lid: LobbyId,
        lobby: Lobby,
    },
    KickPlayer {
        lid: LobbyId,
        pid: BasicId,
    },
    RemovePlayer {
        lid: LobbyId,
        pid: BasicId,
        reason: Option<Kick>,
    },
    AdvanceLobbyTimer {
        lid: LobbyId,
        tx: oneshot::Sender<Result<(), Kick>>,
    },
    FlushPlayerQueue {
        pid: BasicId,
        tx: oneshot::Sender<Vec<ServerMessage>>,
    },
}

#[derive(Clone)]
pub struct Blaster {
    channel: mpsc::Sender<BlasterOperation>,
}

impl Blaster {
    pub fn new(config: Config) -> Self {
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let mut imp = BlasterImpl {
                lobbies: HashMap::new(),
                players: IndexMap::new(),
                config,
            };

            while let Ok(msg) = rx.recv() {
                imp.recv(msg);
            }
        });

        Self { channel: tx }
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
        player_meta: HashMap<String, String>,
    ) {
        let _ = self.channel.send(BlasterOperation::IntroducePlayer {
            pid,
            lid: lid.clone(),
            player_meta,
        });
    }

    pub async fn master_of(&self, lid: &LobbyId) -> Option<BasicId> {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::MasterOf {
            lid: lid.clone(),
            tx,
        });

        rx.await.unwrap_or(None)
    }

    pub async fn lobby_full(&self, lid: &LobbyId) -> bool {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::LobbyFull {
            lid: lid.clone(),
            tx,
        });

        rx.await.unwrap_or(false)
    }

    pub async fn has_lobby(&self, lid: &LobbyId) -> bool {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::HasLobby {
            lid: lid.clone(),
            tx,
        });

        rx.await.unwrap_or(false)
    }

    pub async fn lobby_is_swarm(&self, lid: &LobbyId) -> bool {
        let (tx, rx) = oneshot::channel();

        let _ = self.channel.send(BlasterOperation::LobbyIsSwarm {
            lid: lid.clone(),
            tx,
        });

        rx.await.unwrap_or(false)
    }

    pub async fn has_player(&self, pid: BasicId) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self.channel.send(BlasterOperation::HasPlayer { pid, tx });
        rx.await.unwrap_or(false)
    }

    pub async fn send_to(&self, pid: &BasicId, msg: ServerMessage) {
        let msg = BlasterOperation::SendTo { pid: *pid, msg };
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

    pub async fn insert_lobby(&self, lid: &LobbyId, lobby: Lobby) {
        let _ = self.channel.send(BlasterOperation::InsertLobby {
            lid: lid.clone(),
            lobby,
        });
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

    pub async fn remove_player(&self, lid: &LobbyId, pid: BasicId, reason: Option<Kick>) {
        let _ = self.channel.send(BlasterOperation::RemovePlayer {
            lid: lid.clone(),
            pid,
            reason,
        });
    }

    pub async fn cleanup_lobbies(&self) {
        let _ = self.channel.send(BlasterOperation::CleanupLobbies);
    }
}

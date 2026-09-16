use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::mpsc,
    time::{Duration, Instant},
};

use indexmap::IndexMap;

use crate::{
    MAX_PLAYERS,
    blaster::{BlasterOperation, Config, Lobby, Peer, PeerSessionCount, Player},
    id::{BasicId, GameId, LobbyId},
    protocol::{
        payloads::{Kick, LobbyListing, ServerMessage},
        utils::Metadata,
    },
    tokens::TokenBucket,
};

const MAX_SESSIONS_PER_IP: usize = 4;
const GLOBAL_MAX_SESSIONS: usize = 256;
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

const LOBBY_LISTING_CAP: usize = 32;

const LISTED_LOBBIES_PER_GID: usize = 32;
const LOBBIES_PER_IP: usize = 4;

const FLUSH_MAX: usize = 10;

const CHUD_THRESHOLD: usize = 2;
const CHUD_LOBBY_TIMEOUT: Duration = Duration::from_mins(3);

pub struct BlasterEventLoop {
    gid_lobbies: HashMap<GameId, HashSet<BasicId>>,
    lobbies: HashMap<LobbyId, Lobby>,
    players: IndexMap<BasicId, Player>,
    peers: HashMap<IpAddr, Peer>,
    config: Config,
}

impl BlasterEventLoop {
    pub fn new(config: Config) -> Self {
        Self {
            gid_lobbies: HashMap::new(),
            lobbies: HashMap::new(),
            players: IndexMap::new(),
            peers: HashMap::new(),
            config,
        }
    }

    fn send_to(&mut self, pid: BasicId, msg: ServerMessage) {
        if let Some(player) = self.players.get_mut(&pid) {
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

    fn insert_player(
        &mut self,
        kick_me_now: mpsc::Sender<Kick>,
        ip: IpAddr,
        pid: BasicId,
        lid: LobbyId,
        player_metadata: Metadata,
    ) {
        let Lobby {
            listed,
            capacity,
            metadata: lobby_metadata,
            created_at,
            master,
            players,
            ..
        } = if let Some(lobby) = self.lobbies.get_mut(&lid) {
            let result = lobby.clone();

            lobby.players.insert(pid);

            if lobby.players.len() >= CHUD_THRESHOLD {
                lobby.idle_since = None;
            }

            result
        } else {
            return;
        };

        let now = Instant::now();
        let birth = now.duration_since(created_at).as_millis();

        self.players.insert(
            pid,
            Player {
                lid: lid.clone(),
                metadata: player_metadata.clone(),
                queue: Vec::new(),
                kick_me_now,
                ip,
                birth,
                metadata_budget: TokenBucket::new_metadata(),
            },
        );

        self.send_to(pid, ServerMessage::SetListed { listed });
        self.send_to(pid, ServerMessage::SetCapacity { capacity });

        for (key, value) in lobby_metadata.0 {
            self.send_to(pid, ServerMessage::SetLobbyMeta { key, value });
        }

        self.send_to(pid, ServerMessage::SetMaster { pid: master });

        self.send_to(
            pid,
            ServerMessage::Connected {
                ice_servers: self.config.ice_servers.clone(),
                lid: lid.lid,
                pid,
                birth,
            },
        );

        for other_id in &players {
            if let Some(Player {
                metadata: meta,
                birth,
                ..
            }) = self.players.get(other_id).cloned()
            {
                self.send_to(
                    pid,
                    ServerMessage::Joined {
                        pid: *other_id,
                        metadata: meta,
                        birth,
                    },
                );
            }

            self.send_to(
                *other_id,
                ServerMessage::Joined {
                    pid,
                    metadata: player_metadata.clone(),
                    birth,
                },
            );
        }
    }

    fn cleanup_lobbies(&mut self) {
        let mut deletion = HashSet::new();

        for (lid, lober) in self.lobbies.iter() {
            if lober.players.len() == 0 {
                info!("bye lober: {lid:?}");
                deletion.insert(lid.clone());
            }
        }

        for lid in deletion {
            if let Some(set) = self.gid_lobbies.get_mut(&lid.gid) {
                set.remove(&lid.lid);

                if set.is_empty() {
                    self.gid_lobbies.remove(&lid.gid);
                }
            }

            self.lobbies.remove(&lid);
        }
    }

    fn cap_ip(&mut self, ip: IpAddr) -> bool {
        let peer = self.peers.get_mut(&ip);
        peer.map(|peer| peer.ops.take(1)).unwrap_or(false)
    }

    pub fn recv(&mut self, msg: BlasterOperation) {
        match msg {
            BlasterOperation::ListLobbies { ip, gid, limit, tx } => {
                if !self.cap_ip(ip) {
                    return;
                }

                let list = self
                    .gid_lobbies
                    .get(&gid)
                    .cloned()
                    .unwrap_or(HashSet::new());

                let list = list
                    .into_iter()
                    .filter_map(|lid| {
                        let lid = LobbyId {
                            lid,
                            gid: gid.clone(),
                        };

                        let lobby = self.lobbies.get(&lid)?;
                        Some((lid, lobby))
                    })
                    .filter(|(_, lobby)| lobby.listed && !lobby.is_full())
                    .map(|(lid, lobby)| LobbyListing {
                        lid: lid.lid,
                        max: lobby.capacity,
                        players: lobby.players.len(),
                        metadata: lobby.metadata.clone(),
                    });

                for lobby in list.take(limit.clamp(1, LOBBY_LISTING_CAP)) {
                    let _ = tx.send(lobby);
                }
            }
            BlasterOperation::HostLobby {
                initiator,
                lid,
                master,
                lobby_meta,
                capacity,
                listed,
                pid,
                player_meta,
                kick_me_now,
            } => {
                if !self.cap_ip(initiator) {
                    return;
                }

                if self.lobbies.contains_key(&lid) {
                    let reason = Kick::violation("lobby_exists", "Lobby already exists");
                    let _ = kick_me_now.send(reason);
                    return;
                }

                let iter = self.lobbies.values();
                let iter = iter.filter(|l| l.initiator == Some(initiator));
                let ip_has_listed = iter.clone().any(|l| l.listed);

                if (listed && ip_has_listed) || iter.count() >= LOBBIES_PER_IP {
                    let reason = Kick::violation("rate_limited", "Lobbies per IP limit");
                    let _ = kick_me_now.send(reason);
                    return;
                }

                if listed && let Some(set) = self.gid_lobbies.get(&lid.gid) {
                    let mut lid = lid.clone();

                    let count = set
                        .iter()
                        .filter_map(|&id| {
                            lid.lid = id;
                            self.lobbies.get(&lid)
                        })
                        .filter(|l| l.listed)
                        .count();

                    if count >= LISTED_LOBBIES_PER_GID {
                        let reason = Kick::violation("rate_limited", "Lobbies per GID limit");
                        let _ = kick_me_now.send(reason);
                        return;
                    }
                }

                info!("new lobby {lid:?}");

                let now = Instant::now();

                self.lobbies.insert(
                    lid.clone(),
                    Lobby {
                        initiator: Some(initiator),
                        players: HashSet::new(),
                        master,
                        metadata: lobby_meta,
                        capacity,
                        listed,
                        idle_since: Some(now),
                        created_at: now,
                        alterations_budget: TokenBucket::new(1.0, 2.0, 2.0),
                        metadata_budget: TokenBucket::new_metadata(),
                    },
                );

                info!("new lobby max={capacity} {lid:?}");

                if let Some(set) = self.gid_lobbies.get_mut(&lid.gid) {
                    set.insert(lid.lid);
                } else {
                    let mut set = HashSet::new();
                    set.insert(lid.lid);
                    self.gid_lobbies.insert(lid.gid.clone(), set);
                }

                self.insert_player(kick_me_now, initiator, pid, lid, player_meta);
            }
            BlasterOperation::JoinLobby {
                ip,
                pid,
                lid,
                player_meta,
                kick_me_now,
            } => {
                if !self.cap_ip(ip) {
                    return;
                }

                let Some(lobby) = self.lobbies.get(&lid) else {
                    let _ = kick_me_now.send(Kick::violation("lobby_not_found", "Lobby not found"));
                    return;
                };

                if lobby.is_full() {
                    let _ = kick_me_now.send(Kick::violation("lobby_full", "Lobby is full"));
                } else {
                    self.insert_player(kick_me_now, ip, pid, lid, player_meta);
                }
            }
            BlasterOperation::SetCapacity {
                initiator,
                capacity,
            } => {
                if (1..=MAX_PLAYERS).contains(&capacity)
                    && let Some(Player { lid, .. }) = self.players.get(&initiator).cloned()
                    && let Some(lobby) = self.lobbies.get_mut(&lid)
                    && lobby.master == initiator
                    && lobby.alterations_budget.take(1)
                {
                    lobby.capacity = capacity;

                    let msg = ServerMessage::SetCapacity { capacity };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetListed { initiator, listed } => {
                if let Some(Player { lid, .. }) = self.players.get(&initiator).cloned()
                    && let Some(lobby) = self.lobbies.get_mut(&lid)
                    && lobby.master == initiator
                    && lobby.alterations_budget.take(1)
                {
                    lobby.listed = listed;

                    let msg = ServerMessage::SetListed { listed };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetMaster {
                initiator,
                new_master,
            } => {
                if new_master != initiator
                    && let Some(Player { lid, .. }) = self.players.get(&new_master).cloned()
                    && let Some(lobby) = self.lobbies.get_mut(&lid)
                    && lobby.master == initiator
                    && lobby.alterations_budget.take(1)
                {
                    lobby.master = new_master;
                    self.send_to_lobby(&lid, &ServerMessage::SetMaster { pid: new_master });
                }
            }
            BlasterOperation::SetPlayerMeta { pid, key, value } => {
                let lid = if let Some(player) = self.players.get_mut(&pid)
                    && player.metadata.can_add(&key)
                    && player.metadata_budget.take(key.len() + value.len())
                {
                    player.metadata.0.insert(key.to_string(), value.to_string());
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
                    && player.metadata.0.contains_key(&key)
                {
                    player.metadata.0.remove(&key);
                    player.lid.clone()
                } else {
                    return;
                };

                let msg = ServerMessage::ErasePlayerMeta { pid, key };
                self.send_to_lobby(&lid, &msg);
            }
            BlasterOperation::SetLobbyMeta {
                initiator,
                key,
                value,
            } => {
                if let Some(Player { lid, .. }) = self.players.get(&initiator).cloned()
                    && let Some(lobby) = self.lobbies.get_mut(&lid)
                    && lobby.master == initiator
                    && lobby.metadata.can_add(&key)
                    && lobby.metadata_budget.take(key.len() + value.len())
                {
                    lobby.metadata.0.insert(key.to_string(), value.to_string());

                    let msg = ServerMessage::SetLobbyMeta {
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::EraseLobbyMeta { initiator, key } => {
                if let Some(Player { lid, .. }) = self.players.get(&initiator).cloned()
                    && let Some(lobby) = self.lobbies.get_mut(&lid)
                    && lobby.master == initiator
                    && lobby.metadata.0.contains_key(&key)
                {
                    lobby.metadata.0.remove(&key);

                    let msg = ServerMessage::EraseLobbyMeta {
                        key: key.to_string(),
                    };

                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::KickPlayer {
                kicker,
                kickee: kick_id,
            } => {
                if let Some(Player { lid, .. }) = self.players.get(&kicker).cloned()
                    && let Some(Lobby { master, .. }) = self.lobbies.get(&lid).cloned()
                    && kicker == master
                    && let Some(kickee) = self.players.get_mut(&kick_id)
                    && kickee.lid == lid
                {
                    kickee.kick(Kick::natural("kick", "Kicked by lobby's master"));
                }
            }
            BlasterOperation::RemovePlayer { pid, reason } => {
                let Some(Player { ip, lid, .. }) = self.players.shift_remove(&pid) else {
                    return;
                };

                let Some(lobby) = self.lobbies.get_mut(&lid) else {
                    return;
                };

                lobby.players.remove(&pid);

                if lobby.players.len() < CHUD_THRESHOLD && lobby.idle_since.is_none() {
                    lobby.idle_since.replace(Instant::now());
                }

                if lobby.initiator == Some(ip)
                    && !self.players.iter().any(|(_, p)| p.lid == lid && p.ip == ip)
                {
                    lobby.initiator = None;
                }

                if pid == lobby.master
                    && let Some(&new) = lobby.players.iter().next()
                {
                    lobby.master = new;

                    let msg = ServerMessage::SetMaster { pid: lobby.master };
                    self.send_to_lobby(&lid, &msg);
                }

                let left = ServerMessage::Left { pid, reason };
                self.send_to_lobby(&lid, &left);

                self.cleanup_lobbies();
            }
            BlasterOperation::FlushPlayerQueue { pid, tx } => {
                if let Some(player) = self.players.get_mut(&pid) {
                    let count = FLUSH_MAX.min(player.queue.len());

                    for msg in player.queue.drain(..count) {
                        let _ = tx.send(msg);
                    }
                }
            }
            BlasterOperation::Relay { from, to, msg } => {
                if let Some(p_from) = self.players.get(&from)
                    && let Some(p_to) = self.players.get(&to)
                {
                    if p_from.lid == p_to.lid {
                        self.send_to(to, msg);
                    }
                }
            }
            BlasterOperation::IntroduceSession { ip, tx } => {
                let total_sessions: usize = self.peers.values().map(|c| c.session_count()).sum();

                let _ = tx.send(if total_sessions >= GLOBAL_MAX_SESSIONS {
                    error!("{ip}: global session limit");
                    false
                } else if let Some(peer) = self.peers.get_mut(&ip) {
                    if peer.session_count() >= MAX_SESSIONS_PER_IP {
                        error!("{ip}: too many sessions");
                        false
                    } else {
                        peer.session_count = PeerSessionCount::Some(peer.session_count() + 1);
                        true
                    }
                } else {
                    self.peers.insert(ip, Peer::new());
                    true
                });
            }
            BlasterOperation::Prune => {
                let now = Instant::now();

                for lobby in self.lobbies.values() {
                    if let Some(idle_since) = lobby.idle_since
                        && now.duration_since(idle_since) >= CHUD_LOBBY_TIMEOUT
                    {
                        for pid in &lobby.players {
                            if let Some(player) = self.players.get_mut(pid) {
                                player.kick(Kick::natural("inactive_lobby", "Inactive lobby"));
                            }
                        }
                    }
                }

                let before = self.peers.len();

                self.peers.retain(|_, peer| {
                    if let PeerSessionCount::Decaying(death) = peer.session_count {
                        now.duration_since(death) < SESSION_IDLE_TIMEOUT
                    } else {
                        true
                    }
                });

                info!("{} peers pruned", before - self.peers.len());
            }
            BlasterOperation::CloseSession { ip } => {
                if let Some(peer) = self.peers.get_mut(&ip) {
                    match peer.session_count {
                        PeerSessionCount::Decaying(_) => (),
                        PeerSessionCount::Some(count) => {
                            peer.session_count = match count.saturating_sub(1) {
                                0 => PeerSessionCount::Decaying(Instant::now()),
                                more => PeerSessionCount::Some(more),
                            }
                        }
                    };
                }
            }
        }
    }
}

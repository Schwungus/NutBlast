use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    time::Instant,
};

use indexmap::IndexMap;

use crate::{
    blaster::{BlasterOperation, CHUD_LOBBY_TIMEOUT, Config, Lobby, Peer, Player},
    id::{BasicId, LobbyId},
    protocol::payloads::{Kick, LobbyListing, ServerMessage},
};

const MAX_SESSIONS_PER_IP: usize = 4;
const GLOBAL_MAX_SESSIONS: usize = 256;
const LOBBY_LISTING_CAP: usize = 100;
const LOBBIES_PER_IP: usize = 2;
const FLUSH_MAX: usize = 10;

pub struct BlasterEventLoop {
    lobbies: HashMap<LobbyId, Lobby>,
    players: IndexMap<BasicId, Player>,
    peers: HashMap<IpAddr, Peer>,
    config: Config,
}

impl BlasterEventLoop {
    pub fn new(config: Config) -> Self {
        Self {
            lobbies: HashMap::new(),
            players: IndexMap::new(),
            peers: HashMap::new(),
            config,
        }
    }

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

        if let Some(guy) = self.players.get(&lobby.master)
            && guy.lid == *lid
        {
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

    pub fn recv(&mut self, msg: BlasterOperation) {
        match msg {
            BlasterOperation::SetCapacity {
                initiator,
                capacity,
            } => {
                if let Some(Player { lid, .. }) = self.players.get(&initiator).cloned()
                    && self.master_of(&lid) == Some(initiator)
                    && let Some(lobby) = self.lobbies.get_mut(&lid)
                {
                    lobby.capacity = capacity;

                    let msg = ServerMessage::SetCapacity { capacity };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetListed { initiator, listed } => {
                if let Some(Player { lid, .. }) = self.players.get(&initiator).cloned()
                    && self.master_of(&lid) == Some(initiator)
                    && let Some(lobby) = self.lobbies.get_mut(&lid)
                {
                    lobby.listed = listed;

                    let msg = ServerMessage::SetListed { listed };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetPlayerMeta { pid, key, value } => {
                let lid = if let Some(player) = self.players.get_mut(&pid)
                    && player.meta.can_add(&key)
                {
                    player.meta.0.insert(key.to_string(), value.to_string());
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
                    && player.meta.0.contains_key(&key)
                {
                    player.meta.0.remove(&key);
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
                let Some(Player { lid, .. }) = self.players.get(&initiator).cloned() else {
                    return;
                };

                if self.master_of(&lid) != Some(initiator) {
                    return;
                }

                let Some(lober) = self.lobbies.get_mut(&lid) else {
                    return;
                };

                if lober.meta.can_add(&key) {
                    lober.meta.0.insert(key.to_string(), value.to_string());

                    let msg = ServerMessage::SetLobbyMeta {
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::EraseLobbyMeta { initiator, key } => {
                let Some(Player { lid, .. }) = self.players.get(&initiator).cloned() else {
                    return;
                };

                if self.master_of(&lid) != Some(initiator) {
                    return;
                }

                let Some(lober) = self.lobbies.get_mut(&lid) else {
                    return;
                };

                if lober.meta.0.contains_key(&key) {
                    lober.meta.0.remove(&key);

                    let msg = ServerMessage::EraseLobbyMeta {
                        key: key.to_string(),
                    };

                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::IntroducePlayer {
                ip,
                pid,
                lid,
                player_meta,
                tx,
            } => {
                let Some(Lobby {
                    listed,
                    capacity,
                    meta: lobby_meta,
                    ..
                }) = self.lobbies.get(&lid).cloned()
                else {
                    let _ = tx.send(Err(Kick::violation("lobby_not_found", "Lobby not found")));
                    return;
                };

                if self.players_in(&lid) >= capacity {
                    let _ = tx.send(Err(Kick::violation("lobby_full", "Lobby is full")));
                    return;
                }

                self.players.insert(
                    pid,
                    Player {
                        lid: lid.clone(),
                        meta: player_meta.clone(),
                        queue: Vec::new(),
                        kick_me_now: None,
                        ip,
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

                    for (key, value) in lobby_meta.0 {
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
                        lid: lid.lid,
                        pid,
                    });
                }

                for other in pmeta.keys() {
                    let msg = ServerMessage::Joined {
                        pid,
                        meta: player_meta.clone(),
                    };

                    self.send_to(other, msg);
                }

                let _ = tx.send(Ok(()));
            }
            BlasterOperation::KickPlayer {
                initiator,
                pid: kick_id,
            } => {
                let Some(Player { lid, .. }) = self.players.get(&initiator).cloned() else {
                    return;
                };

                if self.master_of(&lid) == Some(initiator)
                    && let Some(guy) = self.players.get_mut(&kick_id)
                    && guy.lid == lid
                {
                    guy.kick_me_now = Some(Kick::natural("kick", "Kicked by lobby's master"));
                }
            }
            BlasterOperation::RemovePlayer { pid, reason } => {
                let Some(Player { ip, lid, .. }) = self.players.shift_remove(&pid) else {
                    return;
                };

                if let Some(lober) = self.lobbies.get_mut(&lid)
                    && lober.initiator == Some(ip)
                {
                    lober.initiator = None;
                }

                let left = ServerMessage::Left { pid, reason };
                self.send_to_lobby(&lid, &left);

                if let Some(mastah) = self.master_of(&lid) {
                    let msg = ServerMessage::SetMaster { pid: mastah };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::SetMaster {
                initiator,
                new_master,
            } => {
                let Some(lid) = self.players.get(&new_master).map(|x| x.lid.clone()) else {
                    return;
                };

                if Some(initiator) == self.master_of(&lid) && new_master != initiator {
                    self.lobbies.get_mut(&lid).map(|l| l.master = new_master);

                    let msg = ServerMessage::SetMaster { pid: new_master };
                    self.send_to_lobby(&lid, &msg);
                }
            }
            BlasterOperation::ListLobbies { gid, limit, tx } => {
                let mut lobbies: HashMap<LobbyId, LobbyListing> = self
                    .lobbies
                    .iter()
                    .filter_map(|(lid, lobby)| {
                        if lid.gid != gid || !lobby.listed || self.lobby_full(lid) {
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
                    .take(limit.clamp(1, LOBBY_LISTING_CAP))
                    .collect();

                for (_, player) in self.players.iter() {
                    if let Some(lober) = lobbies.get_mut(&player.lid) {
                        lober.players += 1;
                    }
                }

                let _ = tx.send(lobbies.into_values().collect());
            }
            BlasterOperation::AdvanceLobbyTimer { pid, tx } => {
                let Some(Player { lid, .. }) = self.players.get(&pid).clone() else {
                    return;
                };

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
                    let count = FLUSH_MAX.min(player.queue.len());
                    let rest = player.queue.split_off(count);
                    let _ = tx.send(player.queue.clone());
                    player.queue = rest;
                } else {
                    let _ = tx.send(Vec::new());
                };
            }
            BlasterOperation::CleanupLobbies => {
                let mut nonempty = HashSet::new();

                for player in self.players.values() {
                    nonempty.insert(player.lid.clone());
                }

                self.lobbies.retain(move |k, _| {
                    if nonempty.contains(k) {
                        return true;
                    } else {
                        info!("bye lober: {:?}", k);
                        return false;
                    }
                });
            }
            BlasterOperation::Relay { from, to, msg } => {
                if let Some(p_from) = self.players.get(&from)
                    && let Some(p_to) = self.players.get(&to)
                {
                    if p_from.lid == p_to.lid {
                        self.send_to(&to, msg);
                    }
                }
            }
            BlasterOperation::InsertLobby {
                initiator,
                lid,
                master,
                meta,
                capacity,
                listed,
                tx,
            } => {
                let _ = tx.send((move || {
                    if self.lobbies.contains_key(&lid) {
                        return Err(Kick::violation("lobby_exists", "Lobby already exists"));
                    }

                    let iter = self.lobbies.iter();
                    let iter = iter.filter(|(_, l)| l.initiator == Some(initiator));

                    if iter.count() >= LOBBIES_PER_IP {
                        return Err(Kick::violation("rate_limited", "Lobbies per IP limit"));
                    }

                    info!("new lobby {lid:?}");

                    self.lobbies.insert(
                        lid,
                        Lobby {
                            initiator: Some(initiator),
                            master,
                            meta,
                            capacity,
                            listed,
                            death_timer: None,
                        },
                    );

                    Ok(())
                })());
            }
            BlasterOperation::IsKicked { pid, tx } => {
                let _ = tx.send(self.players.get(&pid).and_then(|x| x.kick_me_now.clone()));
            }
            BlasterOperation::IntroduceSession { ip, tx } => {
                let total_sessions: usize = self.peers.values().map(|c| c.session_count).sum();

                let _ = tx.send(if total_sessions >= GLOBAL_MAX_SESSIONS {
                    error!("{ip}: global session limit");
                    false
                } else if let Some(peer) = self.peers.get_mut(&ip) {
                    if peer.session_count >= MAX_SESSIONS_PER_IP {
                        error!("{ip}: too many sessions");
                        false
                    } else {
                        peer.session_count += 1;
                        true
                    }
                } else {
                    self.peers.insert(ip, Peer::new());
                    true
                });
            }
            BlasterOperation::CloseSession { ip } => {
                if let Some(session) = self.peers.get_mut(&ip) {
                    session.session_count = session.session_count.saturating_sub(1);

                    if session.session_count == 0 {
                        self.peers.remove(&ip);
                    }
                }
            }
            BlasterOperation::SignalPerIpCap { ip, tx } => {
                let peer = self.peers.get_mut(&ip);
                let _ = tx.send(peer.map(|peer| peer.ops.try_take(1)).unwrap_or(false));
            }
        }
    }
}

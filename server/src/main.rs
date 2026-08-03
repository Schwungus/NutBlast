#[macro_use]
extern crate log;

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use color_eyre::eyre::{self, eyre};
use futures_util::{SinkExt as _, StreamExt as _, stream::SplitSink};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as TungError, Message},
};

const GAME_ID_LEN: usize = 63;

const MAX_PLAYERS: usize = 16;
const MAX_FIELDS: usize = 16;
const FIELD_NAME_MAX: usize = 255;
const FIELD_VALUE_MAX: usize = 8191;
const MAX_LOBBIES_IN_LIST: usize = 100;

const PAYLOADS_PER_SEC: f32 = 30.0;

const TICK_DELAY: Duration = Duration::from_millis(1000 / 60);
const CHUD_LOBBY_TIMEOUT: Duration = Duration::from_mins(10);

type BasicId = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct LobbyId {
    lid: BasicId,
    gid: String,
}

#[derive(Clone)]
struct Lobby {
    master: BasicId,
    meta: HashMap<String, String>,
    capacity: usize,
    listed: bool,
    death_timer: Option<Instant>,
}

#[derive(Clone)]
struct Player {
    lid: LobbyId,
    meta: HashMap<String, String>,
    queue: Vec<ServerMessage>,
}

impl Player {
    fn send(&mut self, msg: &ServerMessage) {
        self.queue.push(msg.clone());
    }
}

struct State {
    lobbies: HashMap<LobbyId, Lobby>,
    players: IndexMap<BasicId, Player>,
}

#[derive(Clone, Serialize)]
struct LobbyListing {
    lid: BasicId,
    players: usize,
    max: usize,
    meta: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
struct Reason {
    err: bool,
    code: String,
    msg: String,
}

impl Reason {
    fn ok(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            err: false,
            code: code.into(),
            msg: msg.into(),
        }
    }

    fn err(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            err: true,
            code: code.into(),
            msg: msg.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    Ping,
    List {
        gid: String,
        limit: usize,
    },
    Host {
        pid: BasicId,
        #[serde(flatten)]
        lid: LobbyId,
        capacity: usize,
        listed: bool,
        player_meta: HashMap<String, String>,
        lobby_meta: HashMap<String, String>,
    },
    Join {
        pid: BasicId,
        #[serde(flatten)]
        lid: LobbyId,
        player_meta: HashMap<String, String>,
    },
    Swarm {
        pid: BasicId,
        gid: String,
        player_meta: HashMap<String, String>,
        lobby_meta: HashMap<String, String>,
    },
    SetListed {
        listed: bool,
    },
    SetCapacity {
        capacity: usize,
    },
    SetPlayerMeta {
        key: String,
        value: String,
    },
    ErasePlayerMeta {
        key: String,
    },
    SetLobbyMeta {
        key: String,
        value: String,
    },
    EraseLobbyMeta {
        key: String,
    },
    PassCandidate {
        to: BasicId,
        candidate: String,
        mid: String,
    },
    PassOffer {
        to: BasicId,
        sdp: String,
    },
    PassAnswer {
        to: BasicId,
        sdp: String,
    },
    Kick {
        pid: BasicId,
    },
    SetMaster {
        pid: BasicId,
    },
}

#[derive(Clone, Serialize)]
#[serde(tag = "type")]
enum ServerMessage {
    Connected {
        ice_servers: Vec<String>,
    },
    Pong,
    Disconnected {
        reason: Reason,
    },
    SetListed {
        listed: bool,
    },
    SetCapacity {
        capacity: usize,
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
        key: String,
        value: String,
    },
    EraseLobbyMeta {
        key: String,
    },
    SetMaster {
        pid: BasicId,
    },
    Joined {
        pid: BasicId,
        meta: HashMap<String, String>,
    },
    Left {
        pid: BasicId,
        reason: Option<Reason>,
    },
    Candidate {
        from: BasicId,
        candidate: String,
        mid: String,
    },
    Offer {
        from: BasicId,
        sdp: String,
    },
    Answer {
        from: BasicId,
        sdp: String,
    },
    List {
        list: Vec<LobbyListing>,
    },
}

impl State {
    fn introduce_player(
        &mut self,
        config: Arc<Config>,
        pid: BasicId,
        lid: &LobbyId,
        player_meta: HashMap<String, String>,
    ) {
        let Some(Lobby {
            listed,
            capacity,
            meta: lobby_meta,
            ..
        }) = self.lobbies.get(lid).cloned()
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
                if id != &pid && &p.lid == lid {
                    Some((*id, p.meta.clone()))
                } else {
                    None
                }
            })
            .collect();

        if let Some(player) = self.players.get_mut(&pid) {
            player.send(&ServerMessage::SetListed { listed });
            player.send(&ServerMessage::SetCapacity { capacity });

            for (key, value) in lobby_meta {
                player.send(&ServerMessage::SetLobbyMeta { key, value });
            }

            for (&other, meta) in &pmeta {
                player.send(&ServerMessage::Joined {
                    pid: other,
                    meta: meta.clone(),
                });
            }

            if let Some(mastah) = mastah {
                player.send(&ServerMessage::SetMaster { pid: mastah });
            }

            player.send(&ServerMessage::Connected {
                ice_servers: config.ice_servers.clone(),
            });
        }

        for other in pmeta.keys() {
            let msg = ServerMessage::Joined {
                pid,
                meta: player_meta.clone(),
            };

            self.send_to(other, &msg);
        }
    }

    fn master_of(&mut self, lid: &LobbyId) -> Option<BasicId> {
        let empty = self.players_in(lid) == 0;
        let lobby = self.lobbies.get_mut(lid)?;

        if self.players.contains_key(&lobby.master) {
            Some(lobby.master)
        } else if empty {
            None
        } else {
            let new = *self.players.iter().find(|(_, p)| p.lid == *lid)?.0;
            lobby.master = new;
            Some(new)
        }
    }

    fn players_in(&self, lobby: &LobbyId) -> usize {
        let mut counter = 0;

        for (_, p) in self.players.iter() {
            if p.lid == *lobby {
                counter += 1;
            }
        }

        counter
    }

    fn lobby_full(&self, lid: &LobbyId) -> bool {
        if let Some(ref lobby) = self.lobbies.get(lid) {
            return self.players_in(lid) >= lobby.capacity;
        } else {
            return false;
        }
    }

    fn send_to(&mut self, pid: &BasicId, msg: &ServerMessage) {
        if let Some(player) = self.players.get_mut(pid) {
            player.send(msg);
        }
    }

    fn send_to_lobby(&mut self, lid: &LobbyId, msg: &ServerMessage) {
        for (_, player) in &mut self.players {
            if player.lid == *lid {
                player.send(&msg);
            }
        }
    }
}

#[derive(Deserialize)]
struct Config {
    ice_servers: Vec<String>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let _ = color_eyre::install();

    let config: Config = serde_json::from_reader(BufReader::new(
        File::open("nutblaster.json").map_err(|x| eyre!("nutblaster.json: {x}"))?,
    ))?;

    let config = Arc::new(config);

    let addr = if let Some(addr) = std::env::args().nth(1) {
        addr
    } else {
        String::from("127.0.0.1:36900")
    };

    let listener = TcpListener::bind(&addr).await?;

    info!("listening on: ws://{}", addr);

    let state = Arc::new(Mutex::new(State {
        lobbies: HashMap::new(),
        players: IndexMap::new(),
    }));

    while let Ok((stream, player_addr)) = listener.accept().await {
        tokio::spawn(handle(config.clone(), state.clone(), stream, player_addr));
    }

    Ok(())
}

fn check_meta(meta: &HashMap<String, String>) -> bool {
    meta.len() < MAX_FIELDS
        && meta.iter().all(|(key, value)| {
            (1..=FIELD_NAME_MAX).contains(&key.len())
                && (0..=FIELD_VALUE_MAX).contains(&value.len())
        })
}

enum Outcome {
    Good,
    Disconnected(Option<ServerMessage>),
    Boot(Reason),
}

struct Connection {
    load: f32,
    state: Arc<Mutex<State>>,
    addr: SocketAddr,
    pid: Option<BasicId>,
    lid: Option<LobbyId>,
    bye_reason: Option<Reason>,
}

impl Connection {
    async fn recv(
        &mut self,
        config: Arc<Config>,
        msg: Option<Result<Message, TungError>>,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    ) -> bool {
        match msg {
            Some(Ok(msg)) => {
                match self.handle(config, msg) {
                    Outcome::Good => {}
                    Outcome::Boot(reason) => {
                        warn!("boot to the face for {}: {}", self.addr, reason.code);
                        let bye = Some(ServerMessage::Disconnected { reason });
                        self.finalize(sender, bye).await;
                        return false;
                    }
                    Outcome::Disconnected(fatality) => {
                        self.finalize(sender, fatality).await;
                        return false;
                    }
                }

                self.flush(sender).await;
            }
            Some(Err(e)) => {
                if !matches!(e, TungError::ConnectionClosed) {
                    error!("{}: {}", self.addr, e);
                }

                return false;
            }
            None => {
                return false;
            }
        }

        return true;
    }

    fn list_lobbies(&self, state: MutexGuard<State>, gid: &str, limit: usize) -> Vec<LobbyListing> {
        let mut lobbies: HashMap<LobbyId, LobbyListing> = state
            .lobbies
            .iter()
            .filter_map(|(lid, lobby)| {
                if lid.gid != gid || !lobby.listed || state.lobby_full(lid) {
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

        for (_, player) in state.players.iter() {
            if let Some(lober) = lobbies.get_mut(&player.lid) {
                lober.players += 1;
            }
        }

        lobbies.into_values().collect()
    }

    fn handle(&mut self, config: Arc<Config>, msg: Message) -> Outcome {
        let json = match msg {
            Message::Text(text) => text.to_string(),
            Message::Close(_) => return Outcome::Disconnected(None),
            Message::Binary(_) => {
                return Outcome::Boot(Reason::err(
                    "binary_unsupported",
                    "Binary messages not supported",
                ));
            }
            _ => return Outcome::Good,
        };

        let request = match serde_json::from_str(&json) {
            Ok(ok) => ok,
            Err(err) => {
                error!("parse msg from {}: {}", self.addr, err);
                return Outcome::Boot(Reason::err("bad_json", "JSON parse error"));
            }
        };

        let mut state = self.state.freaking_lock();

        match request {
            ClientMessage::Ping => {
                if let Some(ref pid) = self.pid {
                    state.send_to(pid, &ServerMessage::Pong);
                }
            }
            ClientMessage::List { gid, limit } => {
                return Outcome::Disconnected(Some(ServerMessage::List {
                    list: self.list_lobbies(state, &gid, limit),
                }));
            }
            ClientMessage::Host {
                pid,
                lid,
                capacity,
                listed,
                player_meta,
                lobby_meta,
            } if (1..=MAX_PLAYERS).contains(&capacity)
                && self.pid.is_none()
                && self.lid.is_none()
                && !state.players.contains_key(&pid)
                && (1..=GAME_ID_LEN).contains(&lid.gid.len())
                && check_meta(&player_meta)
                && check_meta(&lobby_meta) =>
            {
                self.pid = Some(pid);
                self.lid = Some(lid.clone());

                if state.lobbies.contains_key(&lid) {
                    return Outcome::Boot(Reason::err("lobby_exists", "Lobby already exists"));
                }

                info!("new lobby max={1} {:?}", lid, capacity);

                let lober = Lobby {
                    master: pid,
                    meta: lobby_meta,
                    capacity,
                    listed,
                    death_timer: None,
                };

                state.lobbies.insert(lid.clone(), lober);

                state.players.insert(
                    pid,
                    Player {
                        lid: lid.clone(),
                        meta: player_meta.clone(),
                        queue: Vec::new(),
                    },
                );

                state.introduce_player(config, pid, &lid, player_meta);
            }
            ClientMessage::Join {
                pid,
                lid,
                player_meta,
            } if self.pid.is_none()
                && self.lid.is_none()
                && !state.players.contains_key(&pid)
                && (1..=GAME_ID_LEN).contains(&lid.gid.len())
                && check_meta(&player_meta) =>
            {
                self.pid = Some(pid);
                self.lid = Some(lid.clone());

                if !state.lobbies.contains_key(&lid) {
                    return Outcome::Boot(Reason::err("lobby_not_found", "Lobby not found"));
                }

                if state.lobby_full(&lid) {
                    return Outcome::Boot(Reason::err("lobby_full", "Lobby is full"));
                }

                state.introduce_player(config, pid, &lid, player_meta);
            }
            ClientMessage::PassCandidate {
                ref to,
                candidate,
                mid,
            } if let Some(from) = self.pid => {
                let msg = ServerMessage::Candidate {
                    from,
                    candidate,
                    mid,
                };

                state.send_to(to, &msg);
            }
            ClientMessage::PassOffer { ref to, sdp } if let Some(from) = self.pid => {
                state.send_to(to, &ServerMessage::Offer { from, sdp });
            }
            ClientMessage::PassAnswer { ref to, sdp } if let Some(from) = self.pid => {
                state.send_to(to, &ServerMessage::Answer { from, sdp });
            }
            ClientMessage::SetListed { listed }
                if let Some(pid) = self.pid
                    && let Some(ref lid) = self.lid =>
            {
                if state.master_of(lid) == Some(pid)
                    && let Some(lober) = state.lobbies.get_mut(lid)
                {
                    lober.listed = listed;
                    state.send_to_lobby(lid, &ServerMessage::SetListed { listed });
                }
            }
            ClientMessage::SetCapacity { capacity }
                if let Some(pid) = self.pid
                    && let Some(ref lid) = self.lid =>
            {
                if state.master_of(lid) == Some(pid)
                    && let Some(lober) = state.lobbies.get_mut(lid)
                {
                    lober.capacity = capacity;
                    state.send_to_lobby(lid, &ServerMessage::SetCapacity { capacity });
                }
            }
            // ok to boot since the size limits are enforced client-side
            ClientMessage::SetPlayerMeta { key, value }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && (0..=FIELD_VALUE_MAX).contains(&value.len())
                    && let Some(ref lid) = self.lid
                    && let Some(pid) = self.pid
                    && let Some(player) = state.players.get_mut(&pid) =>
            {
                if player.meta.contains_key(&key) || player.meta.len() < MAX_FIELDS {
                    player.meta.insert(key.to_string(), value.to_string());

                    let msg = ServerMessage::SetPlayerMeta {
                        pid,
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    state.send_to_lobby(lid, &msg);
                }
            }
            ClientMessage::ErasePlayerMeta { key }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && let Some(ref lid) = self.lid
                    && let Some(pid) = self.pid
                    && let Some(player) = state.players.get_mut(&pid) =>
            {
                if player.meta.contains_key(&key) {
                    player.meta.remove(&key);
                    state.send_to_lobby(lid, &ServerMessage::ErasePlayerMeta { pid, key });
                }
            }
            // ok to boot since the size limits are enforced client-side
            ClientMessage::SetLobbyMeta { key, value }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && (0..=FIELD_VALUE_MAX).contains(&value.len())
                    && let Some(ref lid) = self.lid
                    && let master = state.master_of(&lid)
                    && let Some(lober) = state.lobbies.get_mut(&lid) =>
            {
                if master == self.pid
                    && (lober.meta.contains_key(&key) || lober.meta.len() < MAX_FIELDS)
                {
                    lober.meta.insert(key.to_string(), value.to_string());

                    let msg = ServerMessage::SetLobbyMeta {
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    state.send_to_lobby(lid, &msg);
                }
            }
            ClientMessage::EraseLobbyMeta { key }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && let Some(ref lid) = self.lid
                    && let master = state.master_of(&lid)
                    && let Some(lober) = state.lobbies.get_mut(&lid) =>
            {
                if master == self.pid && lober.meta.contains_key(&key) {
                    lober.meta.remove(&key);
                    state.send_to_lobby(lid, &ServerMessage::EraseLobbyMeta { key });
                }
            }
            ClientMessage::Kick { pid: id }
                if let Some(lid) = self.lid.clone()
                    && let Some(pid) = self.pid
                    && let Some(mastah) = state.master_of(&lid) =>
            {
                if pid == mastah
                    && id != pid
                    && let Some(guy) = state.players.get_mut(&id)
                    && guy.lid == lid
                {
                    guy.send(&ServerMessage::Disconnected {
                        reason: Reason::ok("kick", "Kicked by lobby's master"),
                    });
                }
            }
            ClientMessage::SetMaster { pid: id }
                if let Some(ref lid) = self.lid
                    && let Some(pid) = self.pid
                    && let Some(mastah) = state.master_of(&lid) =>
            {
                if pid == mastah
                    && id != pid
                    && state.players.get(&id).map(|x| &x.lid) == Some(lid)
                    && let Some(lobby) = state.lobbies.get_mut(lid)
                {
                    lobby.master = id;
                    state.send_to_lobby(lid, &ServerMessage::SetMaster { pid: id });
                }
            }
            other => {
                warn!("bad: {:?}", other);
                return Outcome::Boot(Reason::err("bad_payload", "Invalid payload"));
            }
        };

        Outcome::Good
    }

    async fn send_json(
        &mut self,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
        value: &ServerMessage,
    ) -> bool {
        if let ServerMessage::Disconnected { reason } = value {
            self.bye_reason = Some(reason.clone());
        }

        let s = match serde_json::to_string(value) {
            Ok(ok) => ok,
            Err(err) => {
                error!("serialize {}: {}", self.addr, err);
                return false;
            }
        };

        if let Err(err) = sender.send(Message::text(s)).await {
            error!("send to {}: {}", self.addr, err);
            return false;
        }

        true
    }

    async fn flush(&mut self, sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>) {
        let Some(pid) = self.pid.to_owned() else {
            return;
        };

        let queue = {
            let mut state = self.state.freaking_lock();

            state.players.get_mut(&pid).map(|p| {
                let queue = p.queue.clone();
                p.queue.clear();
                queue
            })
        };

        for msg in queue.unwrap_or_default() {
            self.send_json(sender, &msg).await;

            if let ServerMessage::Disconnected { .. } = msg {
                break;
            }
        }
    }

    async fn finalize(
        &mut self,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
        fatality: Option<ServerMessage>,
    ) {
        let _ = self.flush(sender).await;

        if let Some(fatality) = fatality {
            let _ = self.send_json(sender, &fatality).await;
        }
    }
}

async fn handle(
    config: Arc<Config>,
    state: Arc<Mutex<State>>,
    stream: TcpStream,
    player_addr: SocketAddr,
) {
    info!("conn: {}", player_addr);

    let (mut sender, mut receiver) = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => {
            info!("hi {}", player_addr);
            ws.split()
        }
        Err(e) => {
            error!("{}: {}", player_addr, e);
            return;
        }
    };

    let mut conn = Connection {
        load: 0.0,
        state: state.clone(),
        addr: player_addr,
        pid: None,
        lid: None,
        bye_reason: None,
    };

    loop {
        let start = Instant::now();
        let mut die = None;

        tokio::select! {
            msg = receiver.next() => {
                // #28. rate-limiting
                conn.load += 1.0 / PAYLOADS_PER_SEC;

                if conn.load >= 1.0 {
                    warn!("CALM DOWN, {}", player_addr);
                    die = Some(Reason::err("rate_limited", "Too many payloads"));
                } else if !conn.recv(config.clone(), msg, &mut sender).await {
                    break;
                }
            }
            _ = tokio::time::sleep(TICK_DELAY) => {
                conn.flush(&mut sender).await;
            }
        }

        let reimburse = Instant::now().duration_since(start).as_secs_f32();
        conn.load = (conn.load - reimburse).max(0.0);

        // #27. single-player lobby timeouts
        if let Some(lid) = conn.lid.clone() {
            let mut state = state.freaking_lock();

            let chud = state.players_in(&lid) == 1;
            let lober = state.lobbies.get_mut(&lid);

            if let Some(lober) = lober {
                if chud && let Some(start) = lober.death_timer {
                    if Instant::now().duration_since(start) >= CHUD_LOBBY_TIMEOUT {
                        die = Some(Reason::ok("inactive_lobby", "Inactive lobby"));
                    }
                } else if chud {
                    lober.death_timer = Some(Instant::now());
                } else {
                    lober.death_timer = None;
                }
            }
        }

        if let Some(reason) = die {
            let msg = ServerMessage::Disconnected { reason };
            conn.send_json(&mut sender, &msg).await;
            break;
        }
    }

    if let Some(pid) = conn.pid
        && let Some(ref lid) = conn.lid
    {
        let mut state = state.freaking_lock();
        state.players.shift_remove(&pid);

        let left = ServerMessage::Left {
            pid,
            reason: conn.bye_reason,
        };

        state.send_to_lobby(lid, &left);

        if let Some(mastah) = state.master_of(lid) {
            state.send_to_lobby(lid, &ServerMessage::SetMaster { pid: mastah });
        }
    }

    info!("bye {}", player_addr);

    if let Ok(mut ws) = receiver.reunite(sender) {
        let _ = ws.close(None).await;
    }

    let mut state = state.freaking_lock();
    let mut nonempty = HashSet::new();

    for player in state.players.values() {
        nonempty.insert(player.lid.clone());
    }

    state.lobbies.retain(move |k, _| {
        if nonempty.contains(k) {
            return true;
        } else {
            info!("bye lober: {:?}", k);
            return false;
        }
    });
}

trait ArcMutexStateExt {
    fn freaking_lock<'a>(&'a self) -> MutexGuard<'a, State>;
}

impl ArcMutexStateExt for Arc<Mutex<State>> {
    fn freaking_lock<'a>(&'a self) -> MutexGuard<'a, State> {
        self.lock().unwrap()
    }
}

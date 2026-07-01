#[macro_use]
extern crate log;

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use color_eyre::eyre;
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
const TICK_DELAY: Duration = Duration::from_millis(1000 / 60);

type BasicId = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct LobbyId {
    lid: BasicId,
    gid: String,
}

struct Lobby {
    meta: HashMap<String, String>,
    capacity: usize,
    listed: bool,
}

struct Player {
    lid: LobbyId,
    meta: HashMap<String, String>,
    queue: Vec<Response>,
}

impl Player {
    fn send(&mut self, response: &Response) {
        self.queue.push(response.clone());
    }
}

struct State {
    lobbies: HashMap<LobbyId, Lobby>,
    players: IndexMap<BasicId, Player>,
}

#[derive(Debug, Deserialize)]
enum ConnectionMode {
    Host,
    Join,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Request {
    Ping,
    List {
        gid: String,
        limit: usize,
    },
    Connect {
        mode: ConnectionMode,
        pid: BasicId,
        #[serde(flatten)]
        lid: LobbyId,
        capacity: usize,
        listed: bool,
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
    SetLobbyMeta {
        key: String,
        value: String,
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
        id: BasicId,
    },
}

#[derive(Clone, Serialize)]
struct LobbyListing {
    lid: BasicId,
    players: usize,
    max: usize,
    meta: HashMap<String, String>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type")]
enum Response {
    Pong,
    Bye {
        reason: Option<String>,
    },
    ListedSet {
        listed: bool,
    },
    CapacitySet {
        capacity: usize,
    },
    PlayerMetaSet {
        player: BasicId,
        key: String,
        value: String,
    },
    LobbyMetaSet {
        key: String,
        value: String,
    },
    NewMaster {
        id: BasicId,
    },
    Joined {
        id: BasicId,
        meta: HashMap<String, String>,
    },
    Left {
        id: BasicId,
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
    fn master_of(&self, lobby: &LobbyId) -> Option<BasicId> {
        self.players
            .iter()
            .filter(|(_, v)| v.lid == *lobby)
            .next()
            .map(|(k, _)| *k)
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

    fn send_to(&mut self, pid: &BasicId, resp: &Response) {
        if let Some(player) = self.players.get_mut(pid) {
            player.send(resp);
        }
    }

    fn send_to_lobby(&mut self, lid: &LobbyId, response: &Response) {
        for (_, player) in &mut self.players {
            if player.lid == *lid {
                player.send(&response);
            }
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let _ = color_eyre::install();

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
        tokio::spawn(handle(state.clone(), stream, player_addr));
    }

    Ok(())
}

enum Outcome {
    Good,
    Bye(Option<Response>),
    Boot(String),
}

impl Outcome {
    fn boot(s: impl Into<String>) -> Self {
        Self::Boot(s.into())
    }
}

struct Connection {
    state: Arc<Mutex<State>>,
    addr: SocketAddr,
    pid: Option<BasicId>,
    lid: Option<LobbyId>,
}

impl Connection {
    async fn recv(
        &mut self,
        msg: Option<Result<Message, TungError>>,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    ) -> bool {
        match msg {
            Some(Ok(msg)) => {
                match self.handle(msg) {
                    Outcome::Good => {}
                    Outcome::Boot(reason) => {
                        warn!("boot to the face for {}: {}", self.addr, reason);

                        let fatality = Some(Response::Bye {
                            reason: Some(reason),
                        });

                        self.finalize(sender, fatality).await;

                        return false;
                    }
                    Outcome::Bye(fatality) => {
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

    fn handle(&mut self, msg: Message) -> Outcome {
        let json = match msg {
            Message::Text(text) => text.to_string(),
            Message::Close(_) => return Outcome::Bye(None),
            Message::Binary(_) => return Outcome::boot("Binary messages not supported"),
            _ => return Outcome::Good,
        };

        let request = match serde_json::from_str(&json) {
            Ok(ok) => ok,
            Err(err) => {
                error!("parse msg from {}: {}", self.addr, err);
                return Outcome::boot("Bad JSON");
            }
        };

        let mut state = self.state.freaking_lock();

        match request {
            Request::Ping if let Some(ref pid) = self.pid => {
                state.send_to(pid, &Response::Pong);
            }
            Request::List { gid, limit } => {
                let mut list: HashMap<LobbyId, LobbyListing> = state
                    .lobbies
                    .iter()
                    .filter_map(|(lid, lobby)| {
                        if lid.gid != gid || !lobby.listed {
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
                    if let Some(lober) = list.get_mut(&player.lid) {
                        lober.players += 1;
                    }
                }

                return Outcome::Bye(Some(Response::List {
                    list: list.into_values().collect(),
                }));
            }
            Request::Connect {
                mode,
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
                && (1..=GAME_ID_LEN).contains(&lid.gid.len()) =>
            {
                self.pid = Some(pid);
                self.lid = Some(lid.clone());

                match (mode, state.lobbies.contains_key(&lid)) {
                    (ConnectionMode::Host, true) => return Outcome::boot("Lobby already exists"),
                    (ConnectionMode::Join, false) => return Outcome::boot("Lobby not found"),
                    _ => {}
                }

                let capacity = if state.lobbies.contains_key(&lid) {
                    state.lobbies[&lid].capacity
                } else {
                    info!("new lobby max={1} {:?}", lid, capacity);

                    let lober = Lobby {
                        meta: lobby_meta,
                        capacity,
                        listed,
                    };

                    state.lobbies.insert(lid.clone(), lober);

                    capacity
                };

                if let Some(Lobby { capacity, .. }) = state.lobbies.get(&lid)
                    && state.players_in(&lid) >= *capacity
                {
                    return Outcome::boot("Lobby is full");
                }

                let p = Player {
                    lid: lid.clone(),
                    meta: player_meta.clone(),
                    queue: Vec::new(),
                };

                state.players.insert(pid, p);

                let mastah = state.master_of(&lid);
                let meta = state.lobbies[&lid].meta.clone();

                let pmeta: HashMap<_, _> = state
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

                if let Some(player) = state.players.get_mut(&pid) {
                    player.send(&Response::ListedSet { listed });

                    player.send(&Response::CapacitySet { capacity });

                    for (key, value) in meta {
                        player.send(&Response::LobbyMetaSet { key, value });
                    }

                    for (&other, meta) in &pmeta {
                        player.send(&Response::Joined {
                            id: other,
                            meta: meta.clone(),
                        });
                    }

                    if let Some(mastah) = mastah {
                        player.send(&Response::NewMaster { id: mastah });
                    }
                }

                for other in pmeta.keys() {
                    let resp = Response::Joined {
                        id: pid,
                        meta: player_meta.clone(),
                    };

                    state.send_to(other, &resp);
                }
            }
            Request::PassCandidate {
                ref to,
                candidate,
                mid,
            } if let Some(from) = self.pid => {
                let resp = Response::Candidate {
                    from,
                    candidate,
                    mid,
                };

                state.send_to(to, &resp);
            }
            Request::PassOffer { ref to, sdp } if let Some(from) = self.pid => {
                state.send_to(to, &Response::Offer { from, sdp });
            }
            Request::PassAnswer { ref to, sdp } if let Some(from) = self.pid => {
                state.send_to(to, &Response::Answer { from, sdp });
            }
            Request::SetListed { listed }
                if let Some(pid) = self.pid
                    && let Some(ref lid) = self.lid =>
            {
                if state.master_of(lid) == Some(pid)
                    && let Some(lober) = state.lobbies.get_mut(lid)
                {
                    lober.listed = listed;
                    state.send_to_lobby(lid, &Response::ListedSet { listed });
                }
            }
            Request::SetCapacity { capacity }
                if let Some(pid) = self.pid
                    && let Some(ref lid) = self.lid =>
            {
                if state.master_of(lid) == Some(pid)
                    && let Some(lober) = state.lobbies.get_mut(lid)
                {
                    lober.capacity = capacity;
                    state.send_to_lobby(lid, &Response::CapacitySet { capacity });
                }
            }
            // ok to boot since the size limits are enforced client-side
            Request::SetPlayerMeta { key, value }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && (0..=FIELD_VALUE_MAX).contains(&value.len())
                    && let Some(ref lid) = self.lid
                    && let Some(pid) = self.pid
                    && let Some(player) = state.players.get_mut(&pid) =>
            {
                if player.meta.contains_key(&key) || player.meta.len() < MAX_FIELDS {
                    player.meta.insert(key.to_string(), value.to_string());

                    let resp = Response::PlayerMetaSet {
                        player: pid,
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    state.send_to_lobby(lid, &resp);
                }
            }
            // ok to boot since the size limits are enforced client-side
            Request::SetLobbyMeta { key, value }
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

                    let resp = Response::LobbyMetaSet {
                        key: key.to_string(),
                        value: value.to_string(),
                    };

                    state.send_to_lobby(lid, &resp);
                }
            }
            Request::Kick { id }
                if let Some(lid) = self.lid.clone()
                    && let Some(pid) = self.pid
                    && let Some(mastah) = state.master_of(&lid) =>
            {
                if pid == mastah
                    && id != pid
                    && let Some(guy) = state.players.get_mut(&id)
                    && guy.lid == lid
                {
                    let reason = Some(String::from("Kicked by lobby's master"));
                    guy.send(&Response::Bye { reason });
                }
            }
            other => {
                debug!("bad: {:?}", other);
                return Outcome::boot("Bad payload");
            }
        };

        Outcome::Good
    }

    async fn send_json(
        &mut self,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
        value: &Response,
    ) -> bool {
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

        for resp in queue.unwrap_or_default() {
            self.send_json(sender, &resp).await;

            if let Response::Bye { .. } = resp {
                break;
            }
        }
    }

    async fn finalize(
        &mut self,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
        fatality: Option<Response>,
    ) {
        let _ = self.flush(sender).await;

        if let Some(fatality) = fatality {
            let _ = self.send_json(sender, &fatality).await;
        }
    }
}

async fn handle(state: Arc<Mutex<State>>, stream: TcpStream, player_addr: SocketAddr) {
    info!("conn: {}", player_addr);

    let (mut ws_sender, mut ws_receiver) = match tokio_tungstenite::accept_async(stream).await {
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
        state: state.clone(),
        addr: player_addr,
        pid: None,
        lid: None,
    };

    loop {
        tokio::select! {
            msg = ws_receiver.next() => {
                if !conn.recv(msg, &mut ws_sender).await {
                    break;
                }
            }
            _ = tokio::time::sleep(TICK_DELAY) => {
                conn.flush( &mut ws_sender).await;
            }
        }
    }

    if let Some(pid) = conn.pid
        && let Some(ref lid) = conn.lid
    {
        let mut state = state.freaking_lock();
        state.players.shift_remove(&pid);

        let mastah = state.master_of(lid);

        for (&other, player) in &mut state.players {
            if player.lid == *lid && other != pid {
                player.send(&Response::Left { id: pid });

                if let Some(mastah) = mastah {
                    player.send(&Response::NewMaster { id: mastah })
                };
            }
        }
    }

    info!("bye {}", player_addr);

    if let Ok(mut ws) = ws_receiver.reunite(ws_sender) {
        let _ = ws.close(None).await;
    }

    {
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
}

trait ArcMutexStateExt {
    fn freaking_lock<'a>(&'a self) -> MutexGuard<'a, State>;
}

impl ArcMutexStateExt for Arc<Mutex<State>> {
    fn freaking_lock<'a>(&'a self) -> MutexGuard<'a, State> {
        self.lock().unwrap()
    }
}

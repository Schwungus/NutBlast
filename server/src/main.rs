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
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as TungError, Message},
};

const GAME_ID_LEN: usize = 16;
const LOBBY_NAME_LEN: usize = 32;

const MAX_PLAYERS: usize = 16;
const MAX_FIELDS: usize = 8;
const TICK_DELAY: Duration = Duration::from_millis(1000 / 60);

type BasicId = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct LobbyId {
    lid: BasicId,
    gid: String,
}

struct Lobby {
    name: String,
    meta: HashMap<String, String>,
    max_players: usize,
}

struct Player {
    counter: u64, // keeps player ordering stable (e.g. master doesn't change if a new player joins)
    lid: LobbyId,
    meta: HashMap<String, String>,
    queue: Vec<Response>,
}

impl Player {
    fn send(&mut self, response: Response) {
        self.queue.push(response);
    }
}

struct State {
    lobbies: HashMap<LobbyId, Lobby>,
    players: HashMap<BasicId, Player>,
    counter: u64,
}

#[derive(Debug, Deserialize)]
enum ConnectionMode {
    Host,
    Join,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Request {
    List {
        gid: String,
    },
    Connect {
        mode: ConnectionMode,
        pid: BasicId,
        #[serde(flatten)]
        lid: LobbyId,
        lname: Option<String>,
        max_players: usize,
        peer_meta: HashMap<String, String>,
        lobby_meta: HashMap<String, String>,
    },
    SetCapacity {
        capacity: usize,
    },
    SetPeerMeta {
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
}

#[derive(Clone, Serialize)]
struct LobbyListing {
    lid: BasicId,
    name: String,
    players: usize,
    max: usize,
    meta: HashMap<String, String>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type")]
enum Response {
    Bye {
        reason: Option<String>,
    },
    CapacitySet {
        capacity: usize,
    },
    PeerMetaSet {
        peer: BasicId,
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
            .min_by(|(_, a), (_, b)| a.counter.cmp(&b.counter))
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

    fn next_counter(&mut self) -> u64 {
        let next = self.counter;
        self.counter += 1;
        next
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
        players: HashMap::new(),
        counter: 0,
    }));

    while let Ok((stream, peer_addr)) = listener.accept().await {
        tokio::spawn(handle(state.clone(), stream, peer_addr));
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

        let mut state = self.state.fucking_lock();

        match request {
            Request::List { gid } => {
                let mut list: HashMap<LobbyId, LobbyListing> = state
                    .lobbies
                    .iter()
                    .filter_map(|(lid, lobby)| {
                        if lid.gid != gid {
                            return None;
                        }

                        let lobby = LobbyListing {
                            name: lobby.name.to_string(),
                            lid: lid.lid,
                            max: lobby.max_players,
                            players: 0,
                            meta: lobby.meta.clone(),
                        };

                        Some((lid.clone(), lobby))
                    })
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
                lname,
                max_players,
                peer_meta,
                lobby_meta,
            } if (1..=MAX_PLAYERS).contains(&max_players)
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

                let max_players = if state.lobbies.contains_key(&lid) {
                    state.lobbies[&lid].max_players
                } else {
                    info!("new lobby max={1} {:?}", lid, max_players);

                    let lname = if let Some(lname) = lname
                        && (1..=LOBBY_NAME_LEN).contains(&lname.len())
                    {
                        lname
                    } else {
                        return Outcome::boot("Invalid lobby name");
                    };

                    state.lobbies.insert(
                        lid.clone(),
                        Lobby {
                            name: lname.to_string(),
                            meta: lobby_meta,
                            max_players,
                        },
                    );

                    max_players
                };

                if let Some(Lobby { max_players, .. }) = state.lobbies.get(&lid)
                    && state.players_in(&lid) >= *max_players
                {
                    return Outcome::boot("Lobby is full");
                }

                let p = Player {
                    counter: state.next_counter(),
                    lid: lid.clone(),
                    meta: peer_meta.clone(),
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
                    player.send(Response::CapacitySet {
                        capacity: max_players,
                    });

                    for (key, value) in meta {
                        player.send(Response::LobbyMetaSet { key, value });
                    }

                    for (&other, meta) in &pmeta {
                        player.send(Response::Joined {
                            id: other,
                            meta: meta.clone(),
                        });
                    }

                    if let Some(mastah) = mastah {
                        player.send(Response::NewMaster { id: mastah });
                    }
                }

                for &other in pmeta.keys() {
                    if let Some(other) = state.players.get_mut(&other) {
                        other.send(Response::Joined {
                            id: pid,
                            meta: peer_meta.clone(),
                        })
                    };
                }
            }
            Request::PassCandidate { to, candidate, mid } if let Some(pid) = self.pid => {
                let Some(to) = state.players.get_mut(&to) else {
                    return Outcome::Good;
                };

                to.send(Response::Candidate {
                    from: pid,
                    candidate,
                    mid,
                });
            }
            Request::PassOffer { to, sdp } if let Some(from) = self.pid => {
                if let Some(to) = state.players.get_mut(&to) {
                    to.send(Response::Offer { from, sdp });
                };
            }
            Request::PassAnswer { to, sdp } if let Some(from) = self.pid => {
                if let Some(to) = state.players.get_mut(&to) {
                    to.send(Response::Answer { from, sdp });
                };
            }
            Request::SetCapacity { capacity } if let Some(ref lid) = self.lid => {
                let master = state.master_of(&lid);

                if master == self.pid
                    && let Some(lober) = state.lobbies.get_mut(&lid)
                {
                    lober.max_players = capacity;
                } else {
                    return Outcome::Good;
                }

                for (_, player) in &mut state.players {
                    if player.lid == *lid {
                        player.send(Response::CapacitySet { capacity });
                    }
                }
            }
            Request::SetPeerMeta { key, value }
                if let Some(ref lid) = self.lid
                    && let Some(pid) = self.pid
                    && let Some(player) = state.players.get_mut(&pid) =>
            {
                if !player.meta.contains_key(&key) && player.meta.len() >= MAX_FIELDS {
                    return Outcome::Good;
                } else {
                    player.meta.insert(key.to_string(), value.to_string());
                }

                for (_, player) in &mut state.players {
                    if player.lid == *lid {
                        player.send(Response::PeerMetaSet {
                            peer: pid,
                            key: key.to_string(),
                            value: value.to_string(),
                        });
                    }
                }
            }
            Request::SetLobbyMeta { key, value }
                if let Some(ref lid) = self.lid
                    && let master = state.master_of(&lid)
                    && let Some(lober) = state.lobbies.get_mut(&lid) =>
            {
                if master == self.pid
                    && (lober.meta.contains_key(&key) || lober.meta.len() < MAX_FIELDS)
                {
                    lober.meta.insert(key.to_string(), value.to_string());
                } else {
                    return Outcome::Good;
                }

                for (_, player) in &mut state.players {
                    if player.lid == *lid {
                        player.send(Response::LobbyMetaSet {
                            key: key.to_string(),
                            value: value.to_string(),
                        });
                    }
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
            let mut state = self.state.fucking_lock();

            state.players.get_mut(&pid).map(|p| {
                let queue = p.queue.clone();
                p.queue.clear();
                queue
            })
        };

        for resp in queue.unwrap_or_default() {
            self.send_json(sender, &resp).await;
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

async fn handle(state: Arc<Mutex<State>>, stream: TcpStream, peer_addr: SocketAddr) {
    info!("conn: {}", peer_addr);

    let (mut ws_sender, mut ws_receiver) = {
        let ws_stream = match tokio_tungstenite::accept_async(stream).await {
            Ok(ws) => {
                info!("hi {}", peer_addr);
                ws
            }
            Err(e) => {
                error!("{}: {}", peer_addr, e);
                return;
            }
        };

        ws_stream.split()
    };

    let mut conn = Connection {
        state: state.clone(),
        addr: peer_addr,
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
        let mut state = state.fucking_lock();
        state.players.remove(&pid);

        let mastah = state.master_of(lid);

        for (&other, player) in &mut state.players {
            if player.lid == *lid && other != pid {
                player.send(Response::Left { id: pid });

                if let Some(mastah) = mastah {
                    player.send(Response::NewMaster { id: mastah })
                };
            }
        }
    }

    info!("bye {}", peer_addr);

    if let Ok(mut ws) = ws_receiver.reunite(ws_sender) {
        let _ = ws.close(None).await;
    }

    {
        let mut state = state.fucking_lock();
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
    fn fucking_lock<'a>(&'a self) -> MutexGuard<'a, State>;
}

impl ArcMutexStateExt for Arc<Mutex<State>> {
    fn fucking_lock<'a>(&'a self) -> MutexGuard<'a, State> {
        self.lock().unwrap()
    }
}

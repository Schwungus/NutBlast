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
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

const TIMEOUT: Duration = Duration::from_secs(5);

const PLAYER_ID_LEN: usize = 4;
const GAME_ID_LEN: usize = 16;
const LOBBY_ID_MAX: usize = 32;
const LOBBY_ID_MIN: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LobbyId {
    game: String,
    name: String,
}

struct Lobby {
    meta: HashMap<String, String>,
}

struct Player {
    lobby_id: LobbyId,
    meta: HashMap<String, String>,
    queue: Vec<Response>,
}

struct State {
    lobbies: HashMap<LobbyId, Lobby>,
    players: HashMap<String, Player>,
}

#[derive(Deserialize)]
enum ConnectionMode {
    Host,
    Join,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum Payload {
    List {
        gid: String,
    },
    Update {
        mode: ConnectionMode,
        pid: String,
        gid: String,
        lid: String,
        peer_meta: HashMap<String, String>,
        lobby_meta: Option<HashMap<String, String>>,
    },
    Candidate {
        to: String,
        candidate: String,
        mid: String,
    },
    Offer {
        to: String,
        sdp: String,
    },
    Answer {
        to: String,
        sdp: String,
    },
}

#[derive(Clone, Serialize)]
struct ResponsePeer {
    meta: HashMap<String, String>,
}

#[derive(Clone, Serialize)]
struct LobbyListing {
    id: String,
    players: u8,
    max: u8,
    meta: HashMap<String, String>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type")]
enum Response {
    Bye {
        reason: Option<String>,
    },
    Update {
        master: String,
        peers: HashMap<String, ResponsePeer>,
        meta: HashMap<String, String>,
    },
    Candidate {
        from: String,
        candidate: String,
        mid: String,
    },
    Offer {
        from: String,
        sdp: String,
    },
    Answer {
        from: String,
        sdp: String,
    },
    Lobbies {
        list: Vec<LobbyListing>,
    },
}

impl State {
    fn master_of(&self, lobby: &LobbyId) -> String {
        self.players
            .iter()
            .find(|(_, v)| v.lobby_id == *lobby)
            .map(|(k, _)| k.to_string())
            .unwrap()
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
    }));

    while let Ok((stream, peer_addr)) = listener.accept().await {
        tokio::spawn(handle(state.clone(), stream, peer_addr));
    }

    Ok(())
}

struct Connection {
    addr: SocketAddr,
    pid: Option<String>,
    gid: Option<String>,
    lid: Option<String>,
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

impl Connection {
    fn handle(&mut self, state: Arc<Mutex<State>>, msg: Message) -> Outcome {
        let json = match msg {
            Message::Text(text) => text.to_string(),
            Message::Close(_) => return Outcome::Bye(None),
            Message::Binary(_) => return Outcome::boot("Sybau clanker"),
            _ => return Outcome::Good,
        };

        let payload = match serde_json::from_str(&json) {
            Ok(ok) => ok,
            Err(err) => {
                error!("parse msg from {}: {}", self.addr, err);
                return Outcome::boot("Speak JSON!!!");
            }
        };

        let mut state = state.fucking_lock();

        match payload {
            Payload::List { gid } => {
                let mut list: HashMap<LobbyId, LobbyListing> = state
                    .lobbies
                    .iter()
                    .filter_map(|(lid, lobby)| {
                        if lid.game == gid {
                            Some((
                                lid.clone(),
                                LobbyListing {
                                    id: lid.name.to_string(),
                                    max: 0, // TODO: add max players
                                    players: 0,
                                    meta: lobby.meta.clone(),
                                },
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();

                for (_, player) in state.players.iter() {
                    list.get_mut(&player.lobby_id).map(|l| l.players += 1);
                }

                let resp = Response::Lobbies {
                    list: list.into_values().collect(),
                };

                return Outcome::Bye(Some(resp));
            }
            Payload::Update {
                mode,
                pid,
                gid,
                lid,
                peer_meta,
                lobby_meta,
            } => {
                let lobby_meta = lobby_meta.unwrap_or_else(HashMap::new);

                match self.pid {
                    None => {
                        if pid.len() != PLAYER_ID_LEN {
                            return Outcome::boot("PID");
                        }

                        // no pid spoofing!!!
                        if state.players.contains_key(&pid) {
                            return Outcome::boot("PID");
                        }

                        self.pid = Some(pid.to_string());
                    }
                    Some(ref real) => {
                        if pid != *real {
                            return Outcome::boot("PID");
                        }
                    }
                }

                match self.lid {
                    None => {
                        if lid.len() < LOBBY_ID_MIN || lid.len() > LOBBY_ID_MAX {
                            return Outcome::boot("LID");
                        }

                        // no lobby-hopping!!!
                        self.lid = Some(lid.to_string());
                    }
                    Some(ref real) => {
                        if lid != *real {
                            return Outcome::boot("LID");
                        }
                    }
                }

                match self.gid {
                    None => {
                        if gid.len() > GAME_ID_LEN {
                            return Outcome::boot("GID");
                        }

                        // no game-hopping either!!!
                        self.gid = Some(gid.to_string());
                    }
                    Some(ref real) => {
                        if gid != *real {
                            return Outcome::boot("GID");
                        }
                    }
                }

                let lobby_id = LobbyId {
                    game: gid.to_string(),
                    name: lid.to_string(),
                };

                match (mode, state.lobbies.contains_key(&lobby_id)) {
                    (ConnectionMode::Host, true) => return Outcome::boot("Lobby already exists!"),
                    (ConnectionMode::Join, false) => return Outcome::boot("No such lobby!"),
                    _ => {}
                }

                if let Some(p) = state.players.get_mut(&pid) {
                    p.meta = peer_meta;
                } else {
                    let p = Player {
                        lobby_id: lobby_id.clone(),
                        meta: peer_meta,
                        queue: Vec::new(),
                    };

                    state.players.insert(pid.to_string(), p);
                }

                if !state.lobbies.contains_key(&lobby_id) {
                    info!("new lobby {:?}", lobby_id);
                    let lober = Lobby { meta: lobby_meta };
                    state.lobbies.insert(lobby_id.clone(), lober);
                } else if Some(state.master_of(&lobby_id)) == self.pid {
                    let lober = state.lobbies.get_mut(&lobby_id);
                    lober.map(|l| l.meta = lobby_meta);
                }

                let master = state.master_of(&lobby_id);

                let peers = state
                    .players
                    .iter()
                    .filter(|(_, p)| p.lobby_id == lobby_id)
                    .map(|(k, p)| {
                        let p = ResponsePeer {
                            meta: p.meta.clone(),
                        };

                        (k.to_string(), p)
                    })
                    .collect();

                let update = Response::Update {
                    master,
                    meta: state.lobbies[&lobby_id].meta.clone(),
                    peers,
                };

                let peer = state.players.get_mut(&pid);
                peer.map(|p| p.queue.push(update));
            }
            Payload::Candidate { to, candidate, mid } if let Some(ref pid) = self.pid => {
                let Some(to) = state.players.get_mut(&to) else {
                    return Outcome::Good;
                };

                to.queue.push(Response::Candidate {
                    from: pid.to_string(),
                    candidate,
                    mid,
                });
            }
            Payload::Offer { to, sdp } if let Some(ref pid) = self.pid => {
                let Some(to) = state.players.get_mut(&to) else {
                    return Outcome::Good;
                };

                to.queue.push(Response::Offer {
                    from: pid.to_string(),
                    sdp,
                });
            }
            Payload::Answer { to, sdp } if let Some(ref pid) = self.pid => {
                let Some(to) = state.players.get_mut(&to) else {
                    return Outcome::Good;
                };

                to.queue.push(Response::Answer {
                    from: pid.to_string(),
                    sdp,
                });
            }
            _ => {
                return Outcome::boot("wtf");
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

    async fn flush(
        &mut self,
        state: Arc<Mutex<State>>,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    ) {
        let Some(pid) = self.pid.to_owned() else {
            return;
        };

        let queue = {
            let state = state.fucking_lock();
            state.players.get(&pid).map(|p| p.queue.clone())
        };

        let Some(queue) = queue else {
            return;
        };

        for resp in queue {
            if !self.send_json(sender, &resp).await {
                continue;
            }
        }

        let mut state = state.fucking_lock();
        state.players.get_mut(&pid).map(|p| p.queue.clear());
    }

    async fn finalize(
        &mut self,
        state: Arc<Mutex<State>>,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
        fatality: Option<Response>,
    ) {
        let _ = self.flush(state.clone(), sender).await;

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
        addr: peer_addr,
        pid: None,
        gid: None,
        lid: None,
    };

    loop {
        tokio::select! {
            _ = tokio::time::sleep(TIMEOUT) => {
                break;
            }
            res = ws_receiver.next() => {
                match res {
                    Some(Ok(msg)) => {
                        match conn.handle(state.clone(), msg) {
                            Outcome::Good => {}
                            Outcome::Boot(reason) => {
                                warn!("boot to the face for {}: {}", peer_addr, reason);
                                conn.finalize(state.clone(), &mut ws_sender, Some(Response::Bye { reason: Some(reason) })).await;
                                break;
                            }
                            Outcome::Bye(fatality) => {
                                conn.finalize(state.clone(), &mut ws_sender, fatality).await;
                                break;
                            }
                        }

                        conn.flush(state.clone(), &mut ws_sender).await;
                    },
                    Some(Err(e)) => {
                        error!("{}: {}", peer_addr, e);
                        break;
                    },
                    None => {
                        break;
                    },
                }
            }
        }
    }

    if let Ok(mut ws) = ws_receiver.reunite(ws_sender) {
        let _ = ws.close(None).await;
    }

    let mut state = state.fucking_lock();

    if let Some(ref pid) = conn.pid {
        state.players.remove(pid);
    }

    info!("bye {}", peer_addr);

    let mut nonempty = HashSet::new();

    for ref player in state.players.values() {
        nonempty.insert(player.lobby_id.clone());
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
    fn fucking_lock<'a>(&'a self) -> MutexGuard<'a, State>;
}

impl ArcMutexStateExt for Arc<Mutex<State>> {
    fn fucking_lock<'a>(&'a self) -> MutexGuard<'a, State> {
        self.lock().unwrap()
    }
}

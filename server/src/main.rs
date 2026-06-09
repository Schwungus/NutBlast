#[macro_use]
extern crate log;

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt as _, StreamExt as _, stream::SplitSink};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

const PORT: u16 = 36900;

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
#[serde(tag = "type")]
enum Payload {
    Update {
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
#[serde(tag = "type")]
enum Response {
    Update {
        master: String,
        peers: HashMap<String, ResponsePeer>,
        meta: HashMap<String, String>,
    },
    Candidate {
        peer: String,
        candidate: String,
        mid: String,
    },
    Offer {
        peer: String,
        sdp: String,
    },
    Answer {
        peer: String,
        sdp: String,
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
async fn main() -> color_eyre::eyre::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let _ = color_eyre::install();

    let addr = format!("0.0.0.0:{}", PORT);
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

impl Connection {
    fn handle(&mut self, state: Arc<Mutex<State>>, msg: Message) -> bool {
        if msg.is_close() {
            return false;
        }

        if !msg.is_text() {
            return false; // no binary you stupid CLANKER
        }

        let text = match msg.to_text() {
            Ok(ok) => ok,
            Err(err) => {
                error!("text msg from {}: {}", self.addr, err);
                return false;
            }
        };

        let payload = match serde_json::from_str(text) {
            Ok(ok) => ok,
            Err(err) => {
                error!("parse msg from {}: {}", self.addr, err);
                return false;
            }
        };

        let mut state = state.lock().unwrap();

        match payload {
            Payload::Update {
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
                            return false;
                        }

                        // no pid spoofing!!!
                        if state.players.contains_key(&pid) {
                            return false;
                        }

                        self.pid = Some(pid.to_string());
                    }
                    Some(ref real) => {
                        if pid != *real {
                            return false;
                        }
                    }
                }

                match self.lid {
                    None => {
                        if lid.len() < LOBBY_ID_MIN || lid.len() > LOBBY_ID_MAX {
                            return false;
                        }

                        // no lobby-hopping!!!
                        self.lid = Some(lid.to_string());
                    }
                    Some(ref real) => {
                        if lid != *real {
                            return false;
                        }
                    }
                }

                match self.gid {
                    None => {
                        if gid.len() > GAME_ID_LEN {
                            return false;
                        }

                        // no game-hopping either!!!
                        self.gid = Some(gid.to_string());
                    }
                    Some(ref real) => {
                        if gid != *real {
                            return false;
                        }
                    }
                }

                let lobby_id = LobbyId {
                    game: gid.to_string(),
                    name: lid.to_string(),
                };

                if state.players.contains_key(&pid) {
                    state.players.get_mut(&pid).unwrap().meta = peer_meta;
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
                } else if state.master_of(&lobby_id) == self.pid.as_ref().unwrap().to_string() {
                    state.lobbies.get_mut(&lobby_id).unwrap().meta = lobby_meta;
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
                    meta: state.lobbies.get(&lobby_id).unwrap().meta.clone(),
                    peers,
                };

                let peer = state.players.get_mut(&pid).unwrap();
                peer.queue.push(update);

                true
            }
            Payload::Candidate { to, candidate, mid } if self.pid.is_some() => {
                let peer = state.players.get_mut(&to).unwrap();

                peer.queue.push(Response::Candidate {
                    peer: self.pid.as_ref().unwrap().to_string(),
                    candidate,
                    mid,
                });

                true
            }
            Payload::Offer { to, sdp } if self.pid.is_some() => {
                let peer = state.players.get_mut(&to).unwrap();

                peer.queue.push(Response::Offer {
                    peer: self.pid.as_ref().unwrap().to_string(),
                    sdp,
                });

                true
            }
            Payload::Answer { to, sdp } if self.pid.is_some() => {
                let peer = state.players.get_mut(&to).unwrap();

                peer.queue.push(Response::Answer {
                    peer: self.pid.as_ref().unwrap().to_string(),
                    sdp,
                });

                true
            }
            _ => false,
        }
    }

    async fn flush(
        &mut self,
        state: Arc<Mutex<State>>,
        sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    ) -> bool {
        if self.pid.is_none() {
            return true;
        }

        let queue = {
            let state = state.lock().unwrap();
            let peer = state.players.get(self.pid.as_ref().unwrap()).unwrap();
            peer.queue.clone()
        };

        for resp in queue {
            let s = match serde_json::to_string(&resp) {
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
        }

        let mut state = state.lock().unwrap();
        let peer = state.players.get_mut(self.pid.as_ref().unwrap()).unwrap();
        peer.queue.clear();

        true
    }
}

async fn handle(state: Arc<Mutex<State>>, stream: TcpStream, peer_addr: SocketAddr) {
    info!("conn: {}", peer_addr);

    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("{}: {}", peer_addr, e);
            return;
        }
    };

    info!("hi {}", peer_addr);

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    let mut conn = Connection {
        addr: peer_addr,
        pid: None,
        gid: None,
        lid: None,
    };

    loop {
        tokio::select! {
            res = ws_receiver.next() => {
                match res {
                    Some(Ok(msg)) => {
                        if !conn.handle(state.clone(), msg) {
                            break ;
                        }
                    }
                    Some(Err(e)) => {
                        error!("{}: {}", peer_addr, e);
                        break ;
                    }
                    None => {
                        break;
                    },
                }
            }
            _ = tokio::time::sleep(TIMEOUT) => {
                break ;
            }
        }

        if !conn.flush(state.clone(), &mut ws_sender).await {
            break;
        }
    }

    let mut state = state.lock().unwrap();

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

#[macro_use]
extern crate log;

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use futures_util::{SinkExt as _, StreamExt as _};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

const PORT: u16 = 36900;

const TIMEOUT: Duration = Duration::from_secs(5);

const PLAYER_ID_LEN: usize = 4;
const GAME_ID_LEN: usize = 16;
const LOBBY_ID_LEN: usize = 32;

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
    offers: HashMap<String, Offer>,
}

struct State {
    lobbies: RwLock<HashMap<LobbyId, Lobby>>,
    players: RwLock<HashMap<String, Player>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Offer {
    local_desc: Option<String>,
    candidates: Vec<String>,
}

#[derive(Deserialize)]
struct Payload {
    pid: String,
    gid: String,
    lid: String,
    peer_meta: HashMap<String, String>,
    lobby_meta: Option<HashMap<String, String>>,
    offers: HashMap<String, Offer>,
}

#[derive(Serialize)]
struct ResponsePeer {
    meta: HashMap<String, String>,
    offer: Option<Offer>,
}

#[derive(Serialize)]
struct Response {
    master: String,
    peers: HashMap<String, ResponsePeer>,
    meta: HashMap<String, String>,
}

impl State {
    fn master_of(&self, lobby: &LobbyId) -> String {
        self.players
            .read()
            .unwrap()
            .iter()
            .find(|(_, v)| v.lobby_id == *lobby)
            .map(|(k, _)| k.to_string())
            .unwrap() // TODO: guarantee safety
    }
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let _ = color_eyre::install();

    let addr = format!("0.0.0.0:{}", PORT); // TODO: stick behind a reverse-proxy
    let listener = TcpListener::bind(&addr).await?;

    info!("listening on: ws://{}", addr);

    let state = Arc::new(State {
        lobbies: RwLock::new(HashMap::new()),
        players: RwLock::new(HashMap::new()),
    });

    while let Ok((stream, peer_addr)) = listener.accept().await {
        tokio::spawn(handle_connection(state.clone(), stream, peer_addr));
    }

    Ok(())
}

async fn handle_connection(state: Arc<State>, stream: TcpStream, peer_addr: SocketAddr) {
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

    let mut pid: Option<String> = None;
    let mut gid: Option<String> = None;
    let mut lid: Option<String> = None;

    let mut handle = |msg: Message| {
        if msg.is_close() {
            return None;
        }

        if !msg.is_text() {
            return None; // no binary you stupid CLANKER
        }

        let text = match msg.to_text() {
            Ok(ok) => ok,
            Err(err) => {
                error!("text msg from {}: {}", peer_addr, err);
                return None;
            }
        };

        let Payload {
            pid: r_pid,
            gid: r_gid,
            lid: r_lid,
            peer_meta,
            lobby_meta,
            offers,
        } = match serde_json::from_str(text) {
            Ok(ok) => ok,
            Err(err) => {
                error!("parse msg from {}: {}", peer_addr, err);
                return None;
            }
        };

        let lobby_meta = lobby_meta.unwrap_or_else(HashMap::new);

        match pid {
            None => {
                if r_pid.len() != PLAYER_ID_LEN {
                    return None;
                }

                // no pid spoofing!!!
                if state.players.read().unwrap().contains_key(&r_pid) {
                    return None;
                }

                pid = Some(r_pid.to_string());
            }
            Some(ref real) => {
                if r_pid != *real {
                    return None;
                }
            }
        }

        match lid {
            None => {
                if r_lid.len() > LOBBY_ID_LEN {
                    return None;
                }

                // no lobby-hopping!!!
                lid = Some(r_lid.to_string());
            }
            Some(ref real) => {
                if r_lid != *real {
                    return None;
                }
            }
        }

        match gid {
            None => {
                if r_gid.len() > GAME_ID_LEN {
                    return None;
                }

                // no game-hopping either!!!
                gid = Some(r_gid.to_string());
            }
            Some(ref real) => {
                if r_gid != *real {
                    return None;
                }
            }
        }

        let lobby_id = LobbyId {
            game: r_gid.to_string(),
            name: r_lid.to_string(),
        };

        let fuckyou_offers = offers.clone();

        if state.players.read().unwrap().contains_key(&r_pid) {
            let mut w = state.players.write().unwrap();
            let w = w.get_mut(&r_pid).unwrap();

            w.meta = peer_meta;
            w.offers = offers;
        } else {
            state.players.write().unwrap().insert(
                r_pid,
                Player {
                    lobby_id: lobby_id.clone(),
                    meta: peer_meta,
                    offers,
                },
            );
        }

        if !state.lobbies.read().unwrap().contains_key(&lobby_id) {
            info!("new lobby {:?}", lobby_id);

            let mut lober = state.lobbies.write().unwrap();
            lober.insert(lobby_id.clone(), Lobby { meta: lobby_meta });
        } else if state.master_of(&lobby_id) == *pid.as_ref().unwrap() {
            let mut lober = state.lobbies.write().unwrap();
            lober.get_mut(&lobby_id).unwrap().meta = lobby_meta;
        }

        let master = state.master_of(&lobby_id);

        let lober = state.lobbies.read().unwrap();
        let peers = state.players.read().unwrap();

        let peers = peers
            .iter()
            .filter(|(_, p)| p.lobby_id == lobby_id)
            .map(|(k, p)| {
                let p = ResponsePeer {
                    meta: p.meta.clone(),
                    offer: fuckyou_offers.get(k).cloned(),
                };

                (k.to_string(), p)
            })
            .collect();

        Some(Response {
            master,
            meta: lober.get(&lobby_id).unwrap().meta.clone(),
            peers,
        })
    };

    'everything: loop {
        tokio::select! {
            res = ws_receiver.next() => {
                match res {
                    Some(Ok(msg)) => {
                        if let Some(resp) = handle(msg) {
                            let s = match serde_json::to_string(&resp) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    error!("serialize {}: {}", peer_addr, err);
                                    break 'everything;
                                }
                            };

                            if let Err(err) = ws_sender.send(Message::text(s)).await {
                                error!("send to {}: {}", peer_addr, err);
                                break 'everything;
                            }
                        } else {
                            break 'everything;
                        }
                    }
                    Some(Err(e)) => {
                        error!("{}: {}", peer_addr, e);
                        break 'everything;
                    }
                    None => {
                        break 'everything;
                    },
                }
            }
            _ = tokio::time::sleep(TIMEOUT) => {
                break 'everything;
            }
        }
    }

    if let Some(ref pid) = pid {
        state.players.write().unwrap().remove(pid);
    }

    info!("bye {}", peer_addr);

    let mut nonempty = HashSet::new();

    for ref player in state.players.read().unwrap().values() {
        nonempty.insert(player.lobby_id.clone());
    }

    let mut lober = state.lobbies.write().unwrap();

    lober.retain(move |k, _| {
        if nonempty.contains(k) {
            return true;
        } else {
            info!("bye lober: {:?}", k);
            return false;
        }
    });
}

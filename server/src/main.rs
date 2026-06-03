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
}

struct State {
    lobbies: RwLock<HashMap<LobbyId, Lobby>>,
    players: RwLock<HashMap<String, Player>>,
}

#[derive(Deserialize)]
struct Payload {
    pid: String,
    gid: String,
    lid: String,
    peer_meta: HashMap<String, String>,
    lobby_meta: Option<HashMap<String, String>>,
}

struct ResponsePeer {
    meta: HashMap<String, String>,
}

#[derive(Serialize)]
struct Response {
    peers: HashMap<String, String>,
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

    info!("accept: {}", peer_addr);

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    let mut pid: Option<String> = None;
    let mut gid: Option<String> = None;
    let mut lid: Option<String> = None;

    let mut handle = |msg: Message| {
        if msg.is_close() {
            info!("bye {}", peer_addr);
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

        let payload: Payload = match serde_json::from_str(text) {
            Ok(ok) => ok,
            Err(err) => {
                error!("parse msg from {}: {}", peer_addr, err);
                return None;
            }
        };

        match pid {
            None => {
                if payload.pid.len() != PLAYER_ID_LEN {
                    return None;
                }

                // no pid spoofing!!!
                if state.players.read().unwrap().contains_key(&payload.pid) {
                    return None;
                }

                pid = Some(payload.pid);
            }
            Some(ref real) => {
                if payload.pid != *real {
                    return None;
                }
            }
        }

        match lid {
            None => {
                if payload.lid.len() > LOBBY_ID_LEN {
                    return None;
                }

                // no lobby-hopping!!!
                lid = Some(payload.lid);
            }
            Some(ref real) => {
                if payload.lid != *real {
                    return None;
                }
            }
        }

        match gid {
            None => {
                if payload.gid.len() > GAME_ID_LEN {
                    return None;
                }

                // no game-hopping either!!!
                gid = Some(payload.gid);
            }
            Some(ref real) => {
                if payload.gid != *real {
                    return None;
                }
            }
        }

        let lobby_id = LobbyId {
            game: gid.as_ref().unwrap().to_string(),
            name: lid.as_ref().unwrap().to_string(),
        };

        if !state.lobbies.read().unwrap().contains_key(&lobby_id) {
            info!("new lobby {:?}", lobby_id);

            let mut lober = state.lobbies.write().unwrap();
            lober.insert(lobby_id.clone(), Lobby {});
        }

        if state.master_of(&lobby_id) == *pid.as_ref().unwrap() {
            state
                .lobbies
                .write()
                .unwrap()
                .get_mut(&lobby_id)
                .unwrap()
                .meta = payload.lobby_meta;
        }

        Some("")
    };

    loop {
        tokio::select! {
            res = ws_receiver.next() => {
                match res {
                    Some(Ok(msg)) => {
                        if let Some(resp) = handle(msg) {
                            let s = match serde_json::to_string(resp) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    error!("serialize {}: {}", peer_addr, err);
                                    break;
                                }
                            };

                            if let Err(err) = ws_sender.send(Message::text(s)).await {
                                error!("send to {}: {}", peer_addr, err);
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        error!("{}: {}", peer_addr, e);
                        break;
                    }
                    None => {
                        break;
                    },
                }
            }
            _ = tokio::time::sleep(TIMEOUT) => {
                break;
            }
        }
    }

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

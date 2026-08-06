use std::{
    collections::HashMap,
    hash::Hasher as _,
    net::SocketAddr,
    time::{Duration, Instant},
};

use fnv::FnvHasher;
use futures_util::{
    SinkExt as _, StreamExt as _,
    stream::{SplitSink, SplitStream},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as TungError, Message},
};

use crate::{
    FIELD_NAME_MAX, FIELD_VALUE_MAX, MAX_FIELDS, MAX_PLAYERS,
    blaster::{Blaster, Lobby},
    id::{BasicId, LobbyId},
    protocol::{ClientMessage, Kick, ServerMessage},
};

pub const PAYLOADS_PER_SEC: f32 = 30.0;

pub const TICK_DELAY: Duration = Duration::from_millis(1000 / 60);

pub struct Connection {
    blaster: Blaster,
    receiver: SplitStream<WebSocketStream<TcpStream>>,
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    addr: SocketAddr,
    pid: Option<BasicId>,
    lid: Option<LobbyId>,
    bye_reason: Option<Kick>,
    load: f32,
}

impl Connection {
    pub fn new(
        blaster: Blaster,
        addr: SocketAddr,
        sender: SplitSink<WebSocketStream<TcpStream>, Message>,
        receiver: SplitStream<WebSocketStream<TcpStream>>,
    ) -> Self {
        Self {
            blaster,
            addr,
            sender,
            receiver,
            load: 0.0,
            pid: None,
            lid: None,
            bye_reason: None,
        }
    }

    async fn handle_next_websock_msg(&mut self) -> Result<Loop, Kick> {
        let start = Instant::now();

        let result = tokio::select! {
            msg = self.receiver.next() => {
                self.handle_websock_msg(msg).await?
            }
            _ = tokio::time::sleep(TICK_DELAY) => {
                Loop::Continue
            }
        };

        self.flush().await;
        self.advance(start).await?;

        Ok(result)
    }

    async fn handle_websock_msg(
        &mut self,
        msg: Option<Result<Message, TungError>>,
    ) -> Result<Loop, Kick> {
        // #28. rate-limiting
        self.load += 1.0 / PAYLOADS_PER_SEC;

        if self.load >= 1.0 {
            warn!("CALM DOWN, {}", self.addr);
            return Err(Kick::violation("rate_limited", "Too many payloads"));
        }

        match msg {
            Some(Ok(msg)) => {
                return self.handle_client_msg(msg).await;
            }
            Some(Err(e)) => {
                if !matches!(e, TungError::ConnectionClosed) {
                    error!("{}: {}", self.addr, e);
                }

                return Ok(Loop::Stop);
            }
            None => {
                return Ok(Loop::Stop);
            }
        }
    }

    async fn handle_client_msg(&mut self, msg: Message) -> Result<Loop, Kick> {
        let json = match msg {
            Message::Text(text) => text.to_string(),
            Message::Close(_) => return Ok(Loop::Stop),
            Message::Binary(_) => {
                return Err(Kick::violation(
                    "binary_unsupported",
                    "Binary messages not supported",
                ));
            }
            _ => return Ok(Loop::Continue),
        };

        let msg = match serde_json::from_str(&json) {
            Ok(ok) => ok,
            Err(err) => {
                error!("parse msg from {}: {}", self.addr, err);
                return Err(Kick::violation("bad_json", "JSON parse error"));
            }
        };

        match msg {
            ClientMessage::Ping => {
                if let Some(ref pid) = self.pid {
                    self.blaster.send_to(pid, ServerMessage::Pong).await;
                }
            }
            ClientMessage::List { gid, limit } => {
                self.send(&ServerMessage::List {
                    list: self.blaster.list_lobbies(&gid, limit).await,
                })
                .await;

                return Ok(Loop::Stop);
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
                && !self.blaster.has_player(pid).await
                && lid.valid()
                && check_meta(&player_meta)
                && check_meta(&lobby_meta) =>
            {
                self.pid = Some(pid);
                self.lid = Some(lid.clone());

                if self.blaster.has_lobby(&lid).await {
                    return Err(Kick::violation("lobby_exists", "Lobby already exists"));
                }

                info!("new lobby max={capacity} {lid:?}");

                let lober = Lobby::ugly_new(pid, lobby_meta, capacity, listed);
                self.blaster.insert_lobby(&lid, lober).await;
                self.blaster.introduce_player(pid, &lid, player_meta).await;
            }
            ClientMessage::Join {
                pid,
                lid,
                player_meta,
            } if self.pid.is_none()
                && self.lid.is_none()
                && !self.blaster.has_player(pid).await
                && lid.valid()
                && check_meta(&player_meta) =>
            {
                self.pid = Some(pid);
                self.lid = Some(lid.clone());

                // also protecting swarms from abuse
                if !self.blaster.has_lobby(&lid).await || self.blaster.lobby_is_swarm(&lid).await {
                    return Err(Kick::violation("lobby_not_found", "Lobby not found"));
                }

                if self.blaster.lobby_full(&lid).await {
                    return Err(Kick::violation("lobby_full", "Lobby is full"));
                }

                self.blaster.introduce_player(pid, &lid, player_meta).await;
            }
            ClientMessage::Swarm {
                pid,
                gid,
                player_meta,
                lobby_meta,
            } if self.pid.is_none()
                && self.lid.is_none()
                && !self.blaster.has_player(pid).await
                && gid.valid()
                && check_meta(&player_meta)
                && check_meta(&lobby_meta) =>
            {
                self.pid = Some(pid);

                let mut lid = {
                    let mut hasher = FnvHasher::default();
                    hasher.write(gid.as_str().as_bytes());

                    let lid = hasher.finish();
                    LobbyId { gid, lid }
                };

                // INFINITE SWARMS!!!
                while self.blaster.lobby_full(&lid).await {
                    lid.lid += 1;
                }

                self.lid = Some(lid.clone());

                if !self.blaster.has_lobby(&lid).await {
                    info!("new swarm {lid:?}");

                    let lober = Lobby::new_swarm(pid, lobby_meta);
                    self.blaster.insert_lobby(&lid, lober).await;
                }

                self.blaster.introduce_player(pid, &lid, player_meta).await;
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

                self.blaster.send_to(to, msg).await;
            }
            ClientMessage::PassOffer { ref to, sdp } if let Some(from) = self.pid => {
                let msg = ServerMessage::Offer { from, sdp };
                self.blaster.send_to(to, msg).await;
            }
            ClientMessage::PassAnswer { ref to, sdp } if let Some(from) = self.pid => {
                let msg = ServerMessage::Answer { from, sdp };
                self.blaster.send_to(to, msg).await;
            }
            ClientMessage::SetListed { listed }
                if let Some(pid) = self.pid
                    && let Some(ref lid) = self.lid =>
            {
                if self.blaster.master_of(lid).await == Some(pid) {
                    self.blaster.set_lobby_listed(lid, listed).await;
                }
            }
            ClientMessage::SetCapacity { capacity }
                if let Some(pid) = self.pid
                    && let Some(ref lid) = self.lid =>
            {
                if self.blaster.master_of(lid).await == Some(pid) {
                    self.blaster.set_lobby_capacity(lid, capacity).await;
                }
            }
            // ok to boot since the size limits are enforced client-side
            ClientMessage::SetPlayerMeta { key, value }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && (0..=FIELD_VALUE_MAX).contains(&value.len())
                    && self.lid.is_some()
                    && let Some(pid) = self.pid =>
            {
                self.blaster.set_player_meta(pid, &key, &value).await;
            }
            ClientMessage::ErasePlayerMeta { key }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && self.lid.is_some()
                    && let Some(pid) = self.pid =>
            {
                self.blaster.erase_player_meta(pid, &key).await;
            }
            // ok to boot since the size limits are enforced client-side
            ClientMessage::SetLobbyMeta { key, value }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && (0..=FIELD_VALUE_MAX).contains(&value.len())
                    && let Some(ref lid) = self.lid
                    && let master = self.blaster.master_of(&lid).await =>
            {
                if master == self.pid {
                    self.blaster.set_lobby_meta(lid, &key, &value).await;
                }
            }
            ClientMessage::EraseLobbyMeta { key }
                if (1..=FIELD_NAME_MAX).contains(&key.len())
                    && let Some(ref lid) = self.lid
                    && let master = self.blaster.master_of(&lid).await =>
            {
                if master == self.pid {
                    self.blaster.erase_lobby_meta(lid, &key).await;
                }
            }
            ClientMessage::Kick { pid: kick_id }
                if let Some(lid) = self.lid.clone()
                    && let Some(pid) = self.pid
                    && let Some(mastah) = self.blaster.master_of(&lid).await =>
            {
                if pid == mastah && kick_id != pid {
                    self.blaster.kick_player(&lid, kick_id).await;
                }
            }
            ClientMessage::SetMaster {
                pid: new_master_pid,
            } if self.lid.is_some()
                && let Some(pid) = self.pid =>
            {
                self.blaster.set_lobby_master(pid, new_master_pid).await;
            }
            other => {
                warn!("bad: {:?}", other);
                return Err(Kick::violation("bad_payload", "Invalid payload"));
            }
        };

        Ok(Loop::Continue)
    }

    async fn advance(&mut self, start: Instant) -> Result<(), Kick> {
        let reimburse = Instant::now().duration_since(start).as_secs_f32();
        self.load = (self.load - reimburse).max(0.0);

        // #27. single-player lobby timeouts
        if let Some(ref lid) = self.lid {
            self.blaster.advance_lobby_timer(lid).await?;
        }

        Ok(())
    }

    async fn send(&mut self, value: &ServerMessage) -> bool {
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

        if let Err(err) = self.sender.send(Message::text(s)).await {
            error!("send to {}: {}", self.addr, err);
            return false;
        }

        true
    }

    async fn flush(&mut self) {
        let Some(pid) = self.pid.to_owned() else {
            return;
        };

        let queue = self.blaster.flush_player_queue(pid).await;

        for msg in queue {
            self.send(&msg).await;

            if let ServerMessage::Disconnected { .. } = msg {
                break;
            }
        }
    }

    pub async fn mainloop(mut self) {
        loop {
            match self.handle_next_websock_msg().await {
                Ok(Loop::Continue) => {}
                Ok(Loop::Stop) => break,
                Err(reason) => {
                    if let Kick::Violation { ref code, .. } = reason {
                        warn!("boot to the face for {}: {}", self.addr, code);
                    }

                    let bye = ServerMessage::Disconnected { reason };
                    self.send(&bye).await;
                    self.flush().await;

                    break;
                }
            }
        }

        if let Some(pid) = self.pid
            && let Some(ref lid) = self.lid
        {
            self.blaster.remove_player(lid, pid, self.bye_reason).await;
        }

        info!("bye {}", self.addr);

        if let Ok(mut ws) = self.receiver.reunite(self.sender) {
            let _ = ws.close(None).await;
        }

        self.blaster.cleanup_lobbies().await;
    }
}

fn check_meta(meta: &HashMap<String, String>) -> bool {
    meta.len() < MAX_FIELDS
        && meta.iter().all(|(key, value)| {
            (1..=FIELD_NAME_MAX).contains(&key.len())
                && (0..=FIELD_VALUE_MAX).contains(&value.len())
        })
}

enum Loop {
    Continue,
    Stop,
}

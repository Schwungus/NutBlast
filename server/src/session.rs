use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

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
    MAX_PLAYERS,
    blaster::Blaster,
    id::{BasicId, LobbyId},
    protocol::{
        payloads::{ClientMessage, Kick, ServerMessage},
        utils::{FieldKey, FieldValue},
    },
    tokens::TokenBucket,
};

pub const TICK_DELAY: Duration = Duration::from_millis(1000 / 60);

pub struct Session {
    blaster: Blaster,
    addr: SocketAddr,
    receiver: SplitStream<WebSocketStream<TcpStream>>,
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    pid: Option<BasicId>,
    lid: Option<LobbyId>,
    bye_reason: Option<Kick>,
    ops: TokenBucket,
}

impl Session {
    const IDLE_TIMEOUT: Duration = Duration::from_millis(5000);

    const MAX_PAYLOADS_PER_SEC: f32 = 30.0;
    const MAX_PAYLOADS_BURST: f32 = 30.0;

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
            pid: None,
            lid: None,
            bye_reason: None,
            ops: TokenBucket::new(Self::MAX_PAYLOADS_PER_SEC, Self::MAX_PAYLOADS_BURST),
        }
    }

    async fn handle_next_websocket_message(&mut self) -> Result<Loop, Kick> {
        let result = tokio::select! {
            msg = self.receiver.next() => {
                self.accept_websocket_message(msg).await
            }
            _ = tokio::time::sleep(TICK_DELAY) => {
                Ok(Loop::Continue)
            }
        };

        // #27. single-player lobby timeouts
        if let Some(ref lid) = self.lid {
            self.blaster.advance_lobby_timer(lid).await?;
        }

        self.flush().await;

        if let Some(pid) = self.pid
            && let Some(kick) = self.blaster.is_kicked(&pid).await
        {
            Err(kick)
        } else {
            result
        }
    }

    async fn accept_websocket_message(
        &mut self,
        msg: Option<Result<Message, TungError>>,
    ) -> Result<Loop, Kick> {
        if !self.ops.try_take() {
            warn!("CALM DOWN, {}", self.addr);
            return Err(Kick::violation("rate_limited", "Too many payloads"));
        }

        match msg {
            Some(Ok(msg)) => {
                return self.process_websocket_message(msg).await;
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

    async fn process_websocket_message(&mut self, msg: Message) -> Result<Loop, Kick> {
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
                    self.blaster.relay(*pid, *pid, ServerMessage::Pong).await;
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
                gid,
                capacity,
                listed,
                player_meta,
                lobby_meta,
            } if (1..=MAX_PLAYERS).contains(&capacity) && self.init_session().await => {
                let pid = rand::random();
                let lid = LobbyId {
                    gid,
                    lid: rand::random(),
                };

                self.pid = Some(pid);
                self.lid = Some(lid.clone());

                self.blaster
                    .create_lobby(&lid, pid, lobby_meta, capacity, listed)
                    .await?;
                info!("new lobby max={capacity} {lid:?}");

                self.blaster
                    .introduce_player(pid, &lid, player_meta)
                    .await?;
            }
            ClientMessage::Join { lid, player_meta } if self.init_session().await => {
                let pid = rand::random();

                self.pid = Some(pid);
                self.lid = Some(lid.clone());

                self.blaster
                    .introduce_player(pid, &lid, player_meta)
                    .await?;
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

                self.blaster.relay(from, *to, msg).await;
            }
            ClientMessage::PassOffer { ref to, sdp } if let Some(from) = self.pid => {
                let msg = ServerMessage::Offer { from, sdp };
                self.blaster.relay(from, *to, msg).await;
            }
            ClientMessage::PassAnswer { ref to, sdp } if let Some(from) = self.pid => {
                let msg = ServerMessage::Answer { from, sdp };
                self.blaster.relay(from, *to, msg).await;
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
                if (1..=MAX_PLAYERS).contains(&capacity)
                    && let Some(pid) = self.pid
                    && let Some(ref lid) = self.lid =>
            {
                if self.blaster.master_of(lid).await == Some(pid) {
                    self.blaster.set_lobby_capacity(lid, capacity).await;
                }
            }
            ClientMessage::SetPlayerMeta {
                key: FieldKey(key),
                value: FieldValue(value),
            } if self.lid.is_some()
                && let Some(pid) = self.pid =>
            {
                self.blaster.set_player_meta(pid, &key, &value).await;
            }
            ClientMessage::ErasePlayerMeta { key: FieldKey(key) }
                if self.lid.is_some()
                    && let Some(pid) = self.pid =>
            {
                self.blaster.erase_player_meta(pid, &key).await;
            }
            ClientMessage::SetLobbyMeta {
                key: FieldKey(key),
                value: FieldValue(value),
            } if let Some(ref lid) = self.lid
                && let master = self.blaster.master_of(&lid).await
                && master == self.pid =>
            {
                self.blaster.set_lobby_meta(lid, &key, &value).await;
            }
            ClientMessage::EraseLobbyMeta { key: FieldKey(key) }
                if let Some(ref lid) = self.lid
                    && let master = self.blaster.master_of(&lid).await
                    && master == self.pid =>
            {
                self.blaster.erase_lobby_meta(lid, &key).await;
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

    async fn init_session(&mut self) -> bool {
        self.pid.is_none() && self.lid.is_none() && self.signal_peer_op().await
    }

    async fn signal_peer_op(&mut self) -> bool {
        self.blaster.signal_peer_op(&self.addr).await
    }

    async fn send(&mut self, value: &ServerMessage) {
        if let ServerMessage::Disconnected { reason } = value {
            self.bye_reason = Some(reason.clone());
        }

        let s = match serde_json::to_string(value) {
            Ok(ok) => ok,
            Err(err) => {
                error!("serialize {}: {}", self.addr, err);
                return;
            }
        };

        if let Err(err) = self.sender.send(Message::text(s)).await {
            error!("send to {}: {}", self.addr, err);
        }
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
        let created_at = Instant::now();

        loop {
            match self.handle_next_websocket_message().await {
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

            if self.pid.is_none() && Instant::now().duration_since(created_at) > Self::IDLE_TIMEOUT
            {
                break;
            }
        }

        if let Some(pid) = self.pid {
            self.blaster.remove_player(pid, self.bye_reason).await;
        }

        info!("bye {}", self.addr);

        if let Ok(mut ws) = self.receiver.reunite(self.sender) {
            let _ = ws.close(None).await;
        }

        self.blaster.cleanup_lobbies().await;
    }
}

enum Loop {
    Continue,
    Stop,
}

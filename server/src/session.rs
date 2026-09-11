use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use futures_util::{
    SinkExt as _, StreamExt as _,
    stream::{SplitSink, SplitStream},
};
use tokio::{net::TcpStream, sync::oneshot};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as TungError, Message},
};

use crate::{
    MAX_PLAYERS,
    blaster::{Blaster, BlasterOperation},
    id::{BasicId, LobbyId},
    protocol::{
        payloads::{ClientMessage, Kick, ServerMessage},
        utils::{FieldKey, FieldValue},
    },
    tokens::TokenBucket,
};

const TICK_DELAY: Duration = Duration::from_millis(1000 / 60);

pub struct Session {
    blaster: Blaster,
    address: SocketAddr,
    receiver: SplitStream<WebSocketStream<TcpStream>>,
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    pid: Option<BasicId>,
    bye_reason: Option<Kick>,
    payloads: TokenBucket,
    bandwidth: TokenBucket,
}

impl Session {
    pub fn new(
        blaster: Blaster,
        address: SocketAddr,
        sender: SplitSink<WebSocketStream<TcpStream>, Message>,
        receiver: SplitStream<WebSocketStream<TcpStream>>,
    ) -> Self {
        const MAX_PAYLOADS_RATE: f32 = 30.0;
        const MAX_PAYLOADS_BURST: f32 = 30.0;

        const MAX_BANDWIDTH_RATE: f32 = 4096.0;
        const MAX_BANDWIDTH_BURST: f32 = 12288.0;

        Self {
            blaster,
            address,
            sender,
            receiver,
            pid: None,
            bye_reason: None,
            payloads: TokenBucket::new(MAX_PAYLOADS_RATE, MAX_PAYLOADS_BURST),
            bandwidth: TokenBucket::new(MAX_BANDWIDTH_RATE, MAX_BANDWIDTH_BURST),
        }
    }

    fn execute(&self, operation: BlasterOperation) {
        self.blaster.execute(operation);
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
        if let Some(pid) = self.pid {
            let (tx, rx) = oneshot::channel();
            let _ = self.execute(BlasterOperation::AdvanceLobbyTimer { pid, tx });
            rx.await.unwrap_or(Ok(()))?;
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
        self.payloads.try_take(1)?;

        match msg {
            Some(Ok(msg)) => {
                self.bandwidth.try_take(msg.len())?;
                return self.process_websocket_message(msg).await;
            }
            Some(Err(e)) => {
                if !matches!(e, TungError::ConnectionClosed) {
                    error!("{}: {}", self.address, e);
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
                error!("parse msg from {}: {}", self.address, err);
                return Err(Kick::violation("bad_json", "JSON parse error"));
            }
        };

        match msg {
            ClientMessage::Ping => {
                if let Some(ref pid) = self.pid {
                    self.relay(*pid, *pid, ServerMessage::Pong).await;
                }
            }
            ClientMessage::List { gid, limit } if self.is_fresh().await => {
                let (tx, rx) = oneshot::channel();

                self.execute(BlasterOperation::ListLobbies {
                    gid: gid.clone(),
                    limit,
                    tx,
                });

                let list = rx.await.unwrap_or_default();
                self.send(&ServerMessage::List { list }).await;

                return Ok(Loop::Stop);
            }
            ClientMessage::Host {
                gid,
                capacity,
                listed,
                player_meta,
                lobby_meta,
            } if (1..=MAX_PLAYERS).contains(&capacity) && self.is_fresh().await => {
                let pid = rand::random();
                self.pid = Some(pid);

                let lid = LobbyId {
                    gid,
                    lid: rand::random(),
                };

                let (tx, rx) = oneshot::channel();

                let _ = self.execute(BlasterOperation::HostLobby {
                    initiator: self.address.ip(),
                    lid: lid.clone(),
                    master: pid,
                    lobby_meta,
                    capacity,
                    listed,
                    pid,
                    player_meta,
                    tx,
                });

                rx.await.unwrap_or(Ok(()))?;

                info!("new lobby max={capacity} {lid:?}");
            }
            ClientMessage::Join { lid, player_meta } if self.is_fresh().await => {
                let pid = rand::random();
                self.pid = Some(pid);

                let (tx, rx) = oneshot::channel();

                self.execute(BlasterOperation::JoinLobby {
                    ip: self.address.ip(),
                    pid,
                    lid: lid.clone(),
                    player_meta,
                    tx,
                });

                rx.await.unwrap_or(Ok(()))?;
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

                self.relay(from, *to, msg).await;
            }
            ClientMessage::PassOffer { ref to, sdp } if let Some(from) = self.pid => {
                let msg = ServerMessage::Offer { from, sdp };
                self.relay(from, *to, msg).await;
            }
            ClientMessage::PassAnswer { ref to, sdp } if let Some(from) = self.pid => {
                let msg = ServerMessage::Answer { from, sdp };
                self.relay(from, *to, msg).await;
            }
            ClientMessage::SetListed { listed } if let Some(pid) = self.pid => {
                self.execute(BlasterOperation::SetListed {
                    initiator: pid,
                    listed,
                });
            }
            ClientMessage::SetCapacity { capacity }
                if (1..=MAX_PLAYERS).contains(&capacity)
                    && let Some(pid) = self.pid =>
            {
                self.execute(BlasterOperation::SetCapacity {
                    initiator: pid,
                    capacity,
                });
            }
            ClientMessage::SetPlayerMeta {
                key: FieldKey(key),
                value: FieldValue(value),
            } if let Some(pid) = self.pid => {
                self.execute(BlasterOperation::SetPlayerMeta {
                    pid,
                    key: key.to_string(),
                    value: value.to_string(),
                });
            }
            ClientMessage::ErasePlayerMeta { key: FieldKey(key) } if let Some(pid) = self.pid => {
                self.execute(BlasterOperation::ErasePlayerMeta {
                    pid,
                    key: key.to_string(),
                });
            }
            ClientMessage::SetLobbyMeta {
                key: FieldKey(key),
                value: FieldValue(value),
            } if let Some(pid) = self.pid => {
                self.execute(BlasterOperation::SetLobbyMeta {
                    initiator: pid,
                    key: key.to_string(),
                    value: value.to_string(),
                });
            }
            ClientMessage::EraseLobbyMeta { key: FieldKey(key) } if let Some(pid) = self.pid => {
                self.execute(BlasterOperation::EraseLobbyMeta {
                    initiator: pid,
                    key: key.to_string(),
                });
            }
            ClientMessage::Kick { pid: kickee } if let Some(pid) = self.pid => {
                self.execute(BlasterOperation::KickPlayer {
                    initiator: pid,
                    pid: kickee,
                });
            }
            ClientMessage::SetMaster { pid: new_master } if let Some(pid) = self.pid => {
                self.execute(BlasterOperation::SetMaster {
                    initiator: pid,
                    new_master,
                });
            }
            other => {
                warn!("bad: {:?}", other);
                return Err(Kick::violation("bad_payload", "Invalid payload"));
            }
        };

        Ok(Loop::Continue)
    }

    async fn relay(&self, from: BasicId, to: BasicId, msg: ServerMessage) {
        let op = BlasterOperation::Relay { from, to, msg };
        let _ = self.execute(op);
    }

    async fn is_fresh(&mut self) -> bool {
        self.pid.is_none() && self.cap_ip().await
    }

    async fn cap_ip(&mut self) -> bool {
        let (tx, rx) = oneshot::channel();

        let _ = self.execute(BlasterOperation::SignalPerIpCap {
            ip: self.address.ip(),
            tx,
        });

        rx.await.unwrap_or(false)
    }

    async fn send(&mut self, value: &ServerMessage) {
        if let ServerMessage::Disconnected { reason } = value {
            self.bye_reason = Some(reason.clone());
        }

        let s = match serde_json::to_string(value) {
            Ok(ok) => ok,
            Err(err) => {
                error!("serialize {}: {}", self.address, err);
                return;
            }
        };

        if let Err(err) = self.sender.send(Message::text(s)).await {
            error!("send to {}: {}", self.address, err);
        }
    }

    async fn flush(&mut self) {
        let Some(pid) = self.pid.to_owned() else {
            return;
        };

        let (tx, rx) = oneshot::channel();
        self.execute(BlasterOperation::FlushPlayerQueue { pid, tx });

        for msg in rx.await.unwrap_or_default() {
            self.send(&msg).await;

            if let ServerMessage::Disconnected { .. } = msg {
                break;
            }
        }
    }

    pub async fn mainloop(mut self) {
        const IDLE_TIMEOUT: Duration = Duration::from_millis(5000);
        let created_at = Instant::now();

        loop {
            match self.handle_next_websocket_message().await {
                Ok(Loop::Continue) => {}
                Ok(Loop::Stop) => break,
                Err(reason) => {
                    if let Kick::Violation { ref code, .. } = reason {
                        warn!("boot to the face for {}: {}", self.address, code);
                    }

                    let bye = ServerMessage::Disconnected { reason };
                    self.send(&bye).await;
                    self.flush().await;

                    break;
                }
            }

            if self.pid.is_none() && Instant::now().duration_since(created_at) > IDLE_TIMEOUT {
                break;
            }
        }

        if let Some(pid) = self.pid {
            self.execute(BlasterOperation::RemovePlayer {
                reason: self.bye_reason.clone(),
                pid,
            });
        }

        info!("bye {}", self.address);

        if let Ok(mut ws) = self.receiver.reunite(self.sender) {
            let _ = ws.close(None).await;
        }

        self.blaster.execute(BlasterOperation::CleanupLobbies);
    }
}

enum Loop {
    Continue,
    Stop,
}

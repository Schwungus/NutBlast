use std::{
    net::IpAddr,
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
    blaster::{Blaster, BlasterOperation, TokioReceiver, TokioSender},
    id::BasicId,
    protocol::{
        payloads::{ClientMessage, Kick, ServerMessage},
        utils::{CandidateString, FieldKey, FieldValue, SdpString},
    },
    tokens::TokenBucket,
};

const IDLE_TIMEOUT: Duration = Duration::from_millis(5000);

pub struct Session {
    blaster: Blaster,
    real_ip: IpAddr,
    ws_sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    ws_receiver: SplitStream<WebSocketStream<TcpStream>>,
    msg_sender: TokioSender,
    msg_receiver: TokioReceiver,
    pid: Option<BasicId>,
    bye_reason: Option<Kick>,
    payloads_budget: TokenBucket,
    relays_budget: TokenBucket,
    bandwidth_budget: TokenBucket,
}

impl Session {
    pub fn new(
        blaster: Blaster,
        real_ip: IpAddr,
        ws_sender: SplitSink<WebSocketStream<TcpStream>, Message>,
        ws_receiver: SplitStream<WebSocketStream<TcpStream>>,
    ) -> Self {
        let (msg_sender, msg_receiver) = tokio::sync::mpsc::unbounded_channel();

        Self {
            payloads_budget: TokenBucket::new(30.0, 30.0, 60.0),
            relays_budget: TokenBucket::new(5.0, 5.0, 40.0),
            bandwidth_budget: TokenBucket::new(4096.0, 4096.0, 12288.0),
            pid: None,
            bye_reason: None,
            blaster,
            real_ip,
            ws_sender,
            ws_receiver,
            msg_sender,
            msg_receiver,
        }
    }

    fn execute(&self, operation: BlasterOperation) {
        self.blaster.execute(operation);
    }

    async fn handle_next_websocket_message(&mut self) -> Result<Loop, Kick> {
        tokio::select! {
            _ = tokio::time::sleep(IDLE_TIMEOUT) => {
                return Ok(Loop::Stop);
            }
            Some(msg) = self.ws_receiver.next() => {
                match msg {
                    Ok(msg) => {
                        self.payloads_budget.try_take(1)?;
                        self.bandwidth_budget.try_take(msg.len())?;
                        return self.process_websocket_message(msg).await;
                    }
                    Err(e) => {
                        if !matches!(e, TungError::ConnectionClosed) {
                            error!("{}: {}", self.real_ip, e);
                        }

                        return Ok(Loop::Stop);
                    }
                }
            }
            Some(msg) = self.msg_receiver.recv() => {
                self.send(&msg).await;

                if let ServerMessage::Disconnected { .. } = msg {
                    return Ok(Loop::Stop);
                }
            }
        }

        Ok(Loop::Continue)
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
                error!("parse msg from {}: {}", self.real_ip, err);
                return Err(Kick::violation("bad_json", "JSON parse error"));
            }
        };

        match msg {
            ClientMessage::Ping => {
                if self.pid.is_some() {
                    self.send(&ServerMessage::Pong).await;
                }
            }
            ClientMessage::List { gid, limit } if self.pid.is_none() => {
                let (tx, rx) = oneshot::channel();

                self.execute(BlasterOperation::ListLobbies {
                    ip: self.real_ip,
                    gid: gid.clone(),
                    limit,
                    tx,
                });

                if let Ok(list) = rx.await {
                    self.send(&ServerMessage::List { list: list? }).await;
                }

                return Ok(Loop::Stop);
            }
            ClientMessage::Host {
                gid,
                capacity,
                listed,
                player_meta,
                lobby_meta,
            } if (1..=MAX_PLAYERS).contains(&capacity) && self.pid.is_none() => {
                let (tx, rx) = oneshot::channel();

                self.execute(BlasterOperation::HostLobby {
                    sender: self.msg_sender.clone(),
                    initiator: self.real_ip,
                    gid,
                    lobby_meta,
                    capacity,
                    listed,
                    player_meta,
                    tx,
                });

                if let Ok(pid) = rx.await {
                    self.pid = Some(pid?);
                }
            }
            ClientMessage::Join { lid, player_meta } if self.pid.is_none() => {
                let (tx, rx) = oneshot::channel();

                self.execute(BlasterOperation::JoinLobby {
                    sender: self.msg_sender.clone(),
                    ip: self.real_ip,
                    lid: lid.clone(),
                    player_meta,
                    tx,
                });

                if let Ok(pid) = rx.await {
                    self.pid = Some(pid?);
                }
            }
            ClientMessage::PassCandidate {
                to,
                candidate: CandidateString(candidate),
                mid: CandidateString(mid),
            } if let Some(from) = self.pid => {
                let msg = ServerMessage::Candidate {
                    from,
                    candidate,
                    mid,
                };

                self.relay(from, to, msg);
            }
            ClientMessage::PassOffer {
                to,
                sdp: SdpString(sdp),
            } if let Some(from) = self.pid => {
                self.relay(from, to, ServerMessage::Offer { from, sdp });
            }
            ClientMessage::PassAnswer {
                to,
                sdp: SdpString(sdp),
            } if let Some(from) = self.pid => {
                self.relay(from, to, ServerMessage::Answer { from, sdp });
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
                    kicker: pid,
                    kickee,
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

    fn relay(&mut self, from: BasicId, to: BasicId, msg: ServerMessage) {
        if self.relays_budget.take(1) {
            self.execute(BlasterOperation::Relay { from, to, msg });
        }
    }

    async fn send(&mut self, value: &ServerMessage) {
        if let ServerMessage::Disconnected { reason } = value {
            self.bye_reason = Some(reason.clone());
        }

        let s = match serde_json::to_string(value) {
            Ok(ok) => ok,
            Err(err) => {
                error!("serialize {}: {}", self.real_ip, err);
                return;
            }
        };

        if let Err(err) = self.ws_sender.send(Message::text(s)).await {
            error!("send to {}: {}", self.real_ip, err);
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
                        warn!("boot to the face for {}: {}", self.real_ip, code);
                    }

                    self.send(&ServerMessage::Disconnected { reason }).await;

                    break;
                }
            }

            if self.pid.is_none() && Instant::now().duration_since(created_at) >= IDLE_TIMEOUT {
                break;
            }
        }

        if let Some(pid) = self.pid {
            self.execute(BlasterOperation::RemovePlayer {
                reason: self.bye_reason.clone(),
                pid,
            });
        }
    }
}

enum Loop {
    Continue,
    Stop,
}

use serde::{Deserialize, Serialize};

use crate::{
    id::{BasicId, GameId, LobbyId},
    protocol::aux::{BoundedString, FieldKey, FieldValue, Metadata},
};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    Ping,
    List {
        gid: GameId,
        limit: usize,
    },
    Host {
        pid: BasicId,
        #[serde(flatten)]
        lid: LobbyId,
        capacity: usize,
        listed: bool,
        player_meta: Metadata,
        lobby_meta: Metadata,
    },
    Join {
        pid: BasicId,
        #[serde(flatten)]
        lid: LobbyId,
        player_meta: Metadata,
    },
    Swarm {
        pid: BasicId,
        gid: GameId,
        player_meta: Metadata,
        lobby_meta: Metadata,
    },
    SetListed {
        listed: bool,
    },
    SetCapacity {
        capacity: usize,
    },
    SetPlayerMeta {
        key: FieldKey,
        value: FieldValue,
    },
    ErasePlayerMeta {
        key: FieldKey,
    },
    SetLobbyMeta {
        key: FieldKey,
        value: FieldValue,
    },
    EraseLobbyMeta {
        key: FieldKey,
    },
    PassCandidate {
        to: BasicId,
        candidate: BoundedString,
        mid: BoundedString,
    },
    PassOffer {
        to: BasicId,
        sdp: BoundedString,
    },
    PassAnswer {
        to: BasicId,
        sdp: BoundedString,
    },
    Kick {
        pid: BasicId,
    },
    SetMaster {
        pid: BasicId,
    },
}

#[derive(Clone, Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    Pong,
    Connected {
        ice_servers: Vec<String>,
    },
    Disconnected {
        reason: Kick,
    },
    SetListed {
        listed: bool,
    },
    SetCapacity {
        capacity: usize,
    },
    SetPlayerMeta {
        pid: BasicId,
        key: String,
        value: String,
    },
    ErasePlayerMeta {
        pid: BasicId,
        key: String,
    },
    SetLobbyMeta {
        key: String,
        value: String,
    },
    EraseLobbyMeta {
        key: String,
    },
    SetMaster {
        pid: BasicId,
    },
    Joined {
        pid: BasicId,
        meta: Metadata,
    },
    Left {
        pid: BasicId,
        reason: Option<Kick>,
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

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Kick {
    Natural { code: String, msg: String },
    Violation { code: String, msg: String },
}

impl Kick {
    pub fn natural(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::Natural {
            code: code.into(),
            msg: msg.into(),
        }
    }

    pub fn violation(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::Violation {
            code: code.into(),
            msg: msg.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LobbyListing {
    pub lid: BasicId,
    pub players: usize,
    pub max: usize,
    pub meta: Metadata,
}

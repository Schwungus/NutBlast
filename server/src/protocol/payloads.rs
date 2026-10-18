use serde::{Deserialize, Serialize};

use crate::{
    blaster::IceServer,
    protocol::basic::{
        BasicId, CandidateString, Capacity, FieldKey, FieldValue, GameId, LobbyId, Metadata,
        SdpString,
    },
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
        gid: GameId,
        capacity: Capacity,
        listed: bool,
        #[serde(rename = "player_meta")]
        player_metadata: Metadata,
        #[serde(rename = "lobby_meta")]
        lobby_metadata: Metadata,
    },
    Join {
        #[serde(flatten)]
        lid: LobbyId,
        #[serde(rename = "player_meta")]
        player_metadata: Metadata,
    },
    Leave {
        code: String,
        msg: String,
    },
    SetListed {
        listed: bool,
    },
    SetCapacity {
        capacity: Capacity,
    },
    #[serde(rename = "SetPlayerMeta")]
    SetPlayerMetadata {
        key: FieldKey,
        value: FieldValue,
    },
    #[serde(rename = "ErasePlayerMeta")]
    ErasePlayerMetadata {
        key: FieldKey,
    },
    #[serde(rename = "SetLobbyMeta")]
    SetLobbyMetadata {
        key: FieldKey,
        value: FieldValue,
    },
    #[serde(rename = "EraseLobbyMeta")]
    EraseLobbyMetadata {
        key: FieldKey,
    },
    PassCandidate {
        to: BasicId,
        candidate: CandidateString,
        mid: CandidateString,
    },
    PassOffer {
        to: BasicId,
        sdp: SdpString,
    },
    PassAnswer {
        to: BasicId,
        sdp: SdpString,
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
    Pong {
        players: Vec<BasicId>,
    },
    Connected {
        pid: BasicId,
        lid: BasicId,
        birth: u128,
        ice_servers: Vec<IceServer>,
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
        #[serde(rename = "meta")]
        metadata: Metadata,
        birth: u128,
    },
    Left {
        pid: BasicId,
        reason: Kick,
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
    #[serde(rename = "meta")]
    pub metadata: Metadata,
}

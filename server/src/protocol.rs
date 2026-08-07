use std::collections::HashMap;

use serde::{Deserialize, Serialize, de};

use crate::id::{BasicId, GameId, LobbyId};

pub const MAX_FIELDS: usize = 16;
pub const FIELD_NAME_MAX: usize = 255;
pub const FIELD_VALUE_MAX: usize = 8191;

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
        key: String,
        value: String,
    },
    ErasePlayerMeta {
        key: String,
    },
    SetLobbyMeta {
        key: String,
        value: String,
    },
    EraseLobbyMeta {
        key: String,
    },
    PassCandidate {
        to: BasicId,
        candidate: String,
        mid: String,
    },
    PassOffer {
        to: BasicId,
        sdp: String,
    },
    PassAnswer {
        to: BasicId,
        sdp: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata(#[serde(deserialize_with = "deserialize_metadata")] HashMap<String, String>);

fn deserialize_metadata<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let meta = HashMap::<String, String>::deserialize(deserializer)?;

    if meta.len() < MAX_FIELDS
        && meta.iter().all(|(key, value)| {
            (1..=FIELD_NAME_MAX).contains(&key.len())
                && (0..=FIELD_VALUE_MAX).contains(&value.len())
        })
    {
        return Ok(meta);
    }

    Err(de::Error::custom("RTFM"))
}

impl Metadata {
    pub fn fields(&self) -> &HashMap<String, String> {
        &self.0
    }

    pub fn fields_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.0
    }
}

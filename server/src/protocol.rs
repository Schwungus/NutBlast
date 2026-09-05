use std::collections::HashMap;

use serde::{Deserialize, Serialize, de};

use crate::id::{BasicId, GameId, LobbyId};

pub const FIELD_NAME_MAX: usize = 255;
pub const FIELD_VALUE_MAX: usize = 8191;

pub const STRING_MAX_MAX_LEN: usize = 1024;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundedString(#[serde(deserialize_with = "deserialize_bounded_string")] pub String);

fn deserialize_bounded_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.len() > STRING_MAX_MAX_LEN {
        return Err(de::Error::custom(format!(
            "String exceeds max length of {STRING_MAX_MAX_LEN} bytes"
        )));
    }

    Ok(s)
}

impl ToString for BoundedString {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, Serialize, Deserialize)]
pub struct FieldKey(#[serde(deserialize_with = "deserialize_field_name")] pub String);

fn deserialize_field_name<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.is_empty() {
        return Err(de::Error::custom("field name cannot be empty"));
    }

    if s.len() > FIELD_NAME_MAX {
        let msg = format!("field name exceeds {FIELD_NAME_MAX} bytes");
        return Err(de::Error::custom(msg));
    }

    Ok(s)
}

impl ToString for FieldKey {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue(#[serde(deserialize_with = "deserialize_field_value")] pub String);

fn deserialize_field_value<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.len() > FIELD_VALUE_MAX {
        return Err(de::Error::custom(format!(
            "field value exceeds max length of {FIELD_VALUE_MAX} bytes",
        )));
    }

    Ok(s)
}

impl ToString for FieldValue {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata(
    #[serde(deserialize_with = "deserialize_metadata")] pub HashMap<String, String>,
);

impl Metadata {
    pub const MAX_FIELDS: usize = 16;

    pub fn can_add(&self, key: &str) -> bool {
        self.0.contains_key(key) || self.0.len() < Self::MAX_FIELDS
    }
}

fn deserialize_metadata<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let meta = HashMap::<FieldKey, FieldValue>::deserialize(deserializer)?;

    if meta.len() > Metadata::MAX_FIELDS {
        return Err(de::Error::custom(format!(
            "too many metadata fields (max {} allowed)",
            Metadata::MAX_FIELDS
        )));
    }

    Ok(meta.into_iter().map(|(k, v)| (k.0, v.0)).collect())
}

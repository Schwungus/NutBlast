use serde::{Deserialize, Serialize, de};

pub type BasicId = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct GameId(#[serde(deserialize_with = "validate_gid")] String);

fn validate_gid<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let gid = String::deserialize(deserializer)?;

    if gid.len() < 1 || gid.len() > GameId::MAX_LEN {
        let msg = format!("game ID must be within 1..={} bytes", GameId::MAX_LEN);
        return Err(de::Error::custom(msg));
    }

    Ok(gid)
}

impl GameId {
    pub const MAX_LEN: usize = 63;

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LobbyId {
    pub lid: BasicId,
    pub gid: GameId,
}

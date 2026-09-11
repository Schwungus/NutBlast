use serde::{Deserialize, Serialize, de};

pub type BasicId = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct GameId(#[serde(deserialize_with = "validate_gid")] String);

fn validate_gid<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    const MAX_LEN: usize = 63;

    let gid = String::deserialize(deserializer)?;

    if gid.is_empty() || gid.len() > MAX_LEN {
        let msg = format!("game ID must be nonempty, up to {MAX_LEN} bytes");
        return Err(de::Error::custom(msg));
    }

    let bad = |c: char| !c.is_ascii_alphanumeric() && !['-', '_', '.', ' '].contains(&c);

    if gid.chars().any(bad) {
        let msg = format!("game ID must be alphanumeric");
        return Err(de::Error::custom(msg));
    }

    Ok(gid)
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LobbyId {
    pub lid: BasicId,
    pub gid: GameId,
}

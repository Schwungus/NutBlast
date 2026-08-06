use serde::{Deserialize, Serialize};

pub type BasicId = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct GameId(String);

impl GameId {
    pub const MAX_LEN: usize = 63;

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn valid(&self) -> bool {
        (1..=Self::MAX_LEN).contains(&self.0.len())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LobbyId {
    pub lid: BasicId,
    pub gid: GameId,
}

impl LobbyId {
    pub fn valid(&self) -> bool {
        self.gid.valid()
    }
}

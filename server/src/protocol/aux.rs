use std::collections::HashMap;

use serde::{Deserialize, Serialize, de};

pub const FIELD_NAME_MAX: usize = 255;
pub const FIELD_VALUE_MAX: usize = 8191;

pub const STRING_MAX_MAX_LEN: usize = 1024;

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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    fn unwrap_key(key: &str) {
        serde_json::from_value::<FieldKey>(Value::String(key.to_string())).unwrap();
    }

    #[test]
    #[should_panic]
    fn field_name_lower_bound() {
        unwrap_key("");
    }

    #[test]
    #[should_panic]
    fn field_name_upper_bound() {
        unwrap_key(&"*".repeat(FIELD_NAME_MAX + 1));
    }

    #[test]
    fn field_name_deserializes() {
        unwrap_key("NutBlast.lobby.name");
        unwrap_key("0");
    }
}

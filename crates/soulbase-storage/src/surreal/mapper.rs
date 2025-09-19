use serde::de::DeserializeOwned;
use serde_json::Value;

pub struct SurrealMapper;

impl SurrealMapper {
    pub fn hydrate<T: DeserializeOwned>(value: Value) -> serde_json::Result<T> {
        serde_json::from_value(value)
    }

    #[cfg(feature = "surreal")]
    pub fn to_json(value: surrealdb::sql::Value) -> Value {
        serde_json::to_value(value).unwrap_or(Value::Null)
    }
}

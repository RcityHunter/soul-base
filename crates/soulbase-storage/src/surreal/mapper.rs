pub struct SurrealMapper;

impl SurrealMapper {
    pub fn hydrate<T: serde::de::DeserializeOwned>(
        value: serde_json::Value,
    ) -> serde_json::Result<T> {
        serde_json::from_value(value)
    }
}

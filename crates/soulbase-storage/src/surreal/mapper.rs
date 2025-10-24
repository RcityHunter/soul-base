use serde::de::DeserializeOwned;
use serde_json::Value;

pub struct SurrealMapper;

impl SurrealMapper {
    pub fn hydrate<T: DeserializeOwned>(value: Value) -> serde_json::Result<T> {
        serde_json::from_value(value)
    }

    #[cfg(feature = "surreal")]
    pub fn to_json(value: surrealdb::sql::Value) -> Value {
        value.into_json()
    }
}

#[cfg(test)]
mod tests {
    use super::SurrealMapper;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Doc {
        id: String,
        count: u32,
    }

    #[test]
    fn hydrate_deserializes_value() {
        let doc: Doc = SurrealMapper::hydrate(json!({"id": "a", "count": 2})).unwrap();
        assert_eq!(doc, Doc { id: "a".into(), count: 2 });
    }

    #[test]
    fn hydrate_propagates_error() {
        let err: Result<Doc, _> = SurrealMapper::hydrate(json!({"id": 1}));
        assert!(err.is_err());
    }
}

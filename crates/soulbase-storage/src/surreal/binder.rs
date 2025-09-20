use serde_json::Value;

pub struct QueryBinder;

impl QueryBinder {
    pub fn into_bindings(params: Value) -> Vec<(String, Value)> {
        match params {
            Value::Object(map) => map.into_iter().collect(),
            _ => Vec::new(),
        }
    }
}

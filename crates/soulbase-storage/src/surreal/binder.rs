pub struct QueryBinder;

impl QueryBinder {
    pub fn bind(_input: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({})
    }
}

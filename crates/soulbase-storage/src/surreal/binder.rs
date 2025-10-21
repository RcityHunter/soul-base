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

#[cfg(test)]
mod tests {
    use super::QueryBinder;
    use serde_json::{json, Value};

    #[test]
    fn converts_object_to_bindings() {
        let mut bindings = QueryBinder::into_bindings(json!({"a": 1, "b": "two"}));
        bindings.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0], ("a".into(), json!(1)));
        assert_eq!(bindings[1], ("b".into(), json!("two")));
    }

    #[test]
    fn non_object_returns_empty() {
        let bindings = QueryBinder::into_bindings(Value::String("ignored".into()));
        assert!(bindings.is_empty());
    }
}

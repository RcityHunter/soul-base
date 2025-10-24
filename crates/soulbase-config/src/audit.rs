use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ChangeRecord {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new: Option<Value>,
}

#[derive(Clone, Debug)]
enum PathSegment<'a> {
    Key(&'a str),
    Index(usize),
}

pub fn diff(old: &Value, new: &Value) -> Vec<ChangeRecord> {
    let mut acc = Vec::new();
    let mut path = Vec::new();
    diff_value(Some(old), Some(new), &mut path, &mut acc);
    acc
}

fn diff_value<'a>(
    old: Option<&'a Value>,
    new: Option<&'a Value>,
    path: &mut Vec<PathSegment<'a>>,
    acc: &mut Vec<ChangeRecord>,
) {
    match (old, new) {
        (None, None) => {}
        (Some(a), Some(b)) if a == b => {}
        (Some(Value::Object(a)), Some(Value::Object(b))) => {
            let mut keys = a.keys().map(|k| k.as_str()).collect::<Vec<_>>();
            for key in b.keys() {
                if !keys.contains(&key.as_str()) {
                    keys.push(key.as_str());
                }
            }
            keys.sort_unstable();
            for key in keys {
                path.push(PathSegment::Key(key));
                diff_value(a.get(key), b.get(key), path, acc);
                path.pop();
            }
        }
        (Some(Value::Array(a)), Some(Value::Array(b))) => {
            let len = a.len().max(b.len());
            for idx in 0..len {
                path.push(PathSegment::Index(idx));
                diff_value(a.get(idx), b.get(idx), path, acc);
                path.pop();
            }
        }
        (Some(a), Some(b)) => {
            acc.push(ChangeRecord {
                path: format_path(path),
                old: Some(a.clone()),
                new: Some(b.clone()),
            });
        }
        (None, Some(b)) => {
            acc.push(ChangeRecord {
                path: format_path(path),
                old: None,
                new: Some(b.clone()),
            });
        }
        (Some(a), None) => {
            acc.push(ChangeRecord {
                path: format_path(path),
                old: Some(a.clone()),
                new: None,
            });
        }
    }
}

fn format_path(path: &[PathSegment<'_>]) -> String {
    let mut out = String::new();
    for segment in path {
        match segment {
            PathSegment::Key(key) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            PathSegment::Index(idx) => {
                out.push('[');
                out.push_str(&idx.to_string());
                out.push(']');
            }
        }
    }
    if out.is_empty() {
        "<root>".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reports_added_and_removed_fields() {
        let old = json!({"a": 1, "b": {"c": 2}});
        let new = json!({"a": 1, "b": {"d": 3}, "e": [1, 2]});
        let mut changes = diff(&old, &new);
        changes.sort_by(|lhs, rhs| lhs.path.cmp(&rhs.path));

        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].path, "b.c");
        assert_eq!(changes[0].old, Some(json!(2)));
        assert!(changes[0].new.is_none());

        assert_eq!(changes[1].path, "b.d");
        assert_eq!(changes[1].old, None);
        assert_eq!(changes[1].new, Some(json!(3)));

        assert_eq!(changes[2].path, "e");
        assert_eq!(changes[2].old, None);
        assert_eq!(changes[2].new, Some(json!([1, 2])));
    }

    #[test]
    fn reports_array_differences() {
        let old = json!({"items": [1, 2, 3]});
        let new = json!({"items": [1, 3, 4]});
        let changes = diff(&old, &new);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].path, "items[1]");
        assert_eq!(changes[0].old, Some(json!(2)));
        assert_eq!(changes[0].new, Some(json!(3)));
        assert_eq!(changes[1].path, "items[2]");
        assert_eq!(changes[1].old, Some(json!(3)));
        assert_eq!(changes[1].new, Some(json!(4)));
    }
}

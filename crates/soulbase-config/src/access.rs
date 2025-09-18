pub fn get_path<'a>(root: &'a serde_json::Value, dotted: &str) -> Option<&'a serde_json::Value> {
    let mut current = root;
    for segment in dotted.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

pub fn set_path(
    root: &mut serde_json::Map<String, serde_json::Value>,
    dotted: &str,
    value: serde_json::Value,
) {
    let mut current = root;
    let mut segments = dotted.split('.').peekable();
    while let Some(segment) = segments.next() {
        if segments.peek().is_none() {
            current.insert(segment.to_string(), value);
            break;
        } else {
            current = current
                .entry(segment)
                .or_insert_with(|| serde_json::Value::Object(Default::default()))
                .as_object_mut()
                .expect("object");
        }
    }
}

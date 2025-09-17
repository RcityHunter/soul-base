use std::collections::BTreeMap;

pub fn labels_for_update(namespace: &str, reload_class: &str) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("namespace", namespace.to_string());
    map.insert("reload_class", reload_class.to_string());
    map
}

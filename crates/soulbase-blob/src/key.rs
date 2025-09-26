pub fn ensure_key(tenant: &str, key: &str) -> Result<(), String> {
    if tenant.is_empty() {
        return Err("tenant segment is required".into());
    }
    if !key.starts_with(&format!("{tenant}/")) {
        return Err("key must start with tenant/".into());
    }
    if key.contains("..") || key.starts_with('/') || key.contains('\\') {
        return Err("invalid key path".into());
    }
    Ok(())
}

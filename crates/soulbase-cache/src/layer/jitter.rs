pub fn apply_jitter(ttl_ms: i64, jitter_ms: Option<i64>, seed: i64) -> i64 {
    match jitter_ms {
        Some(j) if j > 0 => {
            let offset = seed % (2 * j + 1) - j;
            (ttl_ms + offset).max(0)
        }
        _ => ttl_ms.max(0),
    }
}

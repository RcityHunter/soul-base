use crate::policy::SwrPolicy;

pub fn is_stale(now_ms: i64, stored_at_ms: i64, ttl_ms: i64) -> bool {
    now_ms - stored_at_ms > ttl_ms
}

pub fn within_swr(now_ms: i64, stored_at_ms: i64, ttl_ms: i64, policy: &SwrPolicy) -> bool {
    if !policy.enable {
        return false;
    }
    now_ms - stored_at_ms <= ttl_ms + policy.stale_ms
}

use chrono::{DateTime, Duration, Utc};

#[derive(Clone, Debug)]
pub struct RetentionPolicy {
    ttl: Duration,
}

impl RetentionPolicy {
    pub fn new_ttl_seconds(seconds: i64) -> Self {
        Self {
            ttl: Duration::seconds(seconds.max(0)),
        }
    }

    pub fn is_expired(&self, timestamp: DateTime<Utc>) -> bool {
        timestamp + self.ttl < Utc::now()
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::new_ttl_seconds(60 * 60)
    }
}

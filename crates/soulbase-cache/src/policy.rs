use rand::Rng;

#[derive(Clone, Debug, Default)]
pub enum Admission {
    #[default]
    Always,
    Never,
}

#[derive(Clone, Debug)]
pub struct SwrPolicy {
    pub enable: bool,
    pub stale_ms: i64,
    pub refresh_concurrency: usize,
}

impl Default for SwrPolicy {
    fn default() -> Self {
        Self {
            enable: false,
            stale_ms: 60_000,
            refresh_concurrency: 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CachePolicy {
    pub ttl_ms: i64,
    pub jitter_ms: Option<i64>,
    pub admission: Admission,
    pub swr: Option<SwrPolicy>,
}

impl CachePolicy {
    pub fn with_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    pub fn effective_ttl_ms(&self) -> i64 {
        let ttl = self.ttl_ms.max(0);
        match self.jitter_ms {
            Some(jitter) if jitter > 0 => {
                let mut rng = rand::thread_rng();
                let delta: i64 = rng.gen_range(-jitter..=jitter);
                (ttl + delta).max(0)
            }
            _ => ttl,
        }
    }

    pub fn swr_enabled(&self) -> bool {
        self.swr.as_ref().map(|s| s.enable).unwrap_or(false)
    }
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            ttl_ms: 60_000,
            jitter_ms: None,
            admission: Admission::default(),
            swr: None,
        }
    }
}

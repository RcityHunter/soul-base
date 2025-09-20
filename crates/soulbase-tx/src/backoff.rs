use rand::{rngs::StdRng, Rng, SeedableRng};

pub trait BackoffPolicy: Send + Sync {
    fn next_after(&self, now_ms: i64, attempts: u32) -> i64;
    fn allowed(&self, attempts: u32) -> bool;
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_ms: i64,
    pub factor: f64,
    pub jitter: f64,
    pub cap_ms: i64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 5,
            base_ms: 250,
            factor: 2.0,
            jitter: 0.2,
            cap_ms: 30_000,
        }
    }
}

impl BackoffPolicy for RetryPolicy {
    fn next_after(&self, now_ms: i64, attempts: u32) -> i64 {
        if attempts == 0 {
            return now_ms;
        }
        let exponent = (attempts.saturating_sub(1)) as i32;
        let exp_delay = (self.base_ms as f64) * self.factor.powi(exponent);
        let capped = exp_delay.min(self.cap_ms as f64);
        let mut rng = StdRng::from_entropy();
        let jitter_factor = if self.jitter > 0.0 {
            let span = self.jitter.abs();
            1.0 + (rng.gen::<f64>() * 2.0 - 1.0) * span
        } else {
            1.0
        };
        let candidate = (capped * jitter_factor).max(self.base_ms as f64);
        now_ms + candidate.round() as i64
    }

    fn allowed(&self, attempts: u32) -> bool {
        attempts < self.max_attempts
    }
}

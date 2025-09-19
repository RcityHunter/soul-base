use rand::Rng;

pub trait Backoff: Send + Sync {
    fn next_delay_ms(&self, attempt: u32) -> i64;
    fn max_attempts(&self) -> u32;
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

impl Backoff for RetryPolicy {
    fn next_delay_ms(&self, attempt: u32) -> i64 {
        if attempt == 0 {
            return 0;
        }
        let exp = (attempt - 1) as f64;
        let mut delay = (self.base_ms as f64) * self.factor.powf(exp);
        if delay > self.cap_ms as f64 {
            delay = self.cap_ms as f64;
        }
        if self.jitter > 0.0 {
            let mut rng = rand::thread_rng();
            let jitter = rng.gen_range(-(self.jitter)..self.jitter);
            delay *= 1.0 + jitter;
            if delay < 0.0 {
                delay = self.base_ms as f64;
            }
        }
        delay.round() as i64
    }

    fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}

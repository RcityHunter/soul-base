use std::time::Instant;

pub struct RateLimiter {
    tokens: f64,
    capacity: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(capacity: usize, refill_per_sec: usize) -> Self {
        Self {
            tokens: capacity as f64,
            capacity: capacity as f64,
            refill_rate: refill_per_sec as f64,
            last_refill: Instant::now(),
        }
    }

    pub fn allow(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed();
        let add = self.refill_rate * elapsed.as_secs_f64();
        if add > 0.0 {
            self.tokens = (self.tokens + add).min(self.capacity);
            self.last_refill = Instant::now();
        }
    }
}

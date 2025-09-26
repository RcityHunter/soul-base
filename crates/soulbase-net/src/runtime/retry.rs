use std::time::Duration;

use rand::Rng;

use crate::policy::{BackoffCfg, RetryPolicy};

pub struct RetryState {
    attempts: u32,
}

impl RetryState {
    pub fn new() -> Self {
        Self { attempts: 0 }
    }

    pub fn next_delay(&mut self, policy: &RetryPolicy, backoff: &BackoffCfg) -> Option<Duration> {
        if !policy.enabled {
            return None;
        }
        if self.attempts + 1 >= policy.max_attempts {
            return None;
        }
        self.attempts += 1;

        let power = (self.attempts - 1) as i32;
        let multiplier = backoff.multiplier as f64;
        let mut secs = backoff.base_delay.as_secs_f64() * multiplier.powi(power);
        if secs <= 0.0 {
            secs = backoff.base_delay.as_secs_f64();
        }
        let max_secs = backoff.max_delay.as_secs_f64();
        if max_secs > 0.0 && secs > max_secs {
            secs = max_secs;
        }
        let mut delay = Duration::from_secs_f64(secs);
        if backoff.jitter {
            let millis = delay.as_millis().max(1) as u64;
            let jitter = rand::thread_rng().gen_range(0..millis);
            delay = Duration::from_millis(jitter);
        }
        Some(delay)
    }
}

use std::time::Instant;

use crate::policy::CircuitBreakerPolicy;

#[derive(Clone, Debug)]
pub enum CircuitState {
    Closed,
    Open { since: Instant },
    HalfOpen { probes: u32 },
}

impl Default for CircuitState {
    fn default() -> Self {
        CircuitState::Closed
    }
}

pub struct CircuitBreaker {
    policy: CircuitBreakerPolicy,
    state: CircuitState,
    failures: u32,
    successes: u32,
}

impl CircuitBreaker {
    pub fn new(policy: CircuitBreakerPolicy) -> Self {
        Self {
            policy,
            state: CircuitState::Closed,
            failures: 0,
            successes: 0,
        }
    }

    pub fn can_execute(&mut self) -> bool {
        match &mut self.state {
            CircuitState::Closed => true,
            CircuitState::Open { since } => {
                if since.elapsed() >= self.policy.open_for {
                    self.state = CircuitState::HalfOpen { probes: 1 };
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen { probes } => {
                if *probes < self.policy.half_open_max {
                    *probes += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn record_success(&mut self) {
        self.successes += 1;
        match &mut self.state {
            CircuitState::HalfOpen { .. } => {
                self.reset();
            }
            CircuitState::Closed => {
                let total = self.failures + self.successes;
                if total >= self.policy.min_samples {
                    self.failures = 0;
                    self.successes = 0;
                }
            }
            CircuitState::Open { .. } => {}
        }
    }

    pub fn record_failure(&mut self) {
        self.failures += 1;
        match &mut self.state {
            CircuitState::Closed | CircuitState::HalfOpen { .. } => {
                let total = self.failures + self.successes;
                if total >= self.policy.min_samples {
                    let ratio = self.failures as f32 / total as f32;
                    if ratio >= self.policy.failure_ratio {
                        self.state = CircuitState::Open {
                            since: Instant::now(),
                        };
                        self.failures = 0;
                        self.successes = 0;
                    }
                }
            }
            CircuitState::Open { .. } => {}
        }
    }

    fn reset(&mut self) {
        self.state = CircuitState::Closed;
        self.failures = 0;
        self.successes = 0;
    }
}

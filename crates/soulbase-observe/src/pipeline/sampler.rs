use parking_lot::Mutex;
use rand::Rng;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SamplerDecision {
    Record,
    Drop,
}

pub trait Sampler: Send + Sync {
    fn sample(&self) -> SamplerDecision;
}

#[derive(Clone, Debug)]
pub struct HeadSampler {
    probability: f64,
}

impl HeadSampler {
    pub fn new(probability: f64) -> Self {
        Self {
            probability: probability.clamp(0.0, 1.0),
        }
    }
}

impl Default for HeadSampler {
    fn default() -> Self {
        Self::new(1.0)
    }
}

impl Sampler for HeadSampler {
    fn sample(&self) -> SamplerDecision {
        if self.probability >= 1.0 {
            SamplerDecision::Record
        } else if self.probability <= 0.0 {
            SamplerDecision::Drop
        } else {
            let mut rng = rand::thread_rng();
            if rng.gen::<f64>() <= self.probability {
                SamplerDecision::Record
            } else {
                SamplerDecision::Drop
            }
        }
    }
}

/// A stub tail sampler that enables recording once every `window` decisions.
#[derive(Clone, Debug)]
pub struct TailSampler {
    window: u32,
    cursor: Arc<Mutex<u32>>,
}

impl TailSampler {
    pub fn new(window: u32) -> Self {
        Self {
            window: window.max(1),
            cursor: Arc::new(Mutex::new(0)),
        }
    }
}

impl Default for TailSampler {
    fn default() -> Self {
        Self::new(10)
    }
}

impl Sampler for TailSampler {
    fn sample(&self) -> SamplerDecision {
        let mut guard = self.cursor.lock();
        *guard = (*guard + 1) % self.window;
        if *guard == 0 {
            SamplerDecision::Record
        } else {
            SamplerDecision::Drop
        }
    }
}

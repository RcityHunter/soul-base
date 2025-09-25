use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "observe")]
use soulbase_observe::model::{MetricKind, MetricSpec};
#[cfg(feature = "observe")]
use soulbase_observe::sdk::metrics::{CounterHandle, Meter};

#[cfg(feature = "observe")]
pub mod spec {
    use soulbase_observe::model::{MetricKind, MetricSpec};

    pub const CANONICAL_OK_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_canonical_ok_total",
        kind: MetricKind::Counter,
        help: "Count of successful canonicalization operations.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const CANONICAL_ERR_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_canonical_err_total",
        kind: MetricKind::Counter,
        help: "Count of failed canonicalization operations.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const DIGEST_OK_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_digest_ok_total",
        kind: MetricKind::Counter,
        help: "Count of successful digest operations.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const DIGEST_ERR_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_digest_err_total",
        kind: MetricKind::Counter,
        help: "Count of failed digest operations.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const SIGN_OK_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_sign_ok_total",
        kind: MetricKind::Counter,
        help: "Count of successful signing operations.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const SIGN_ERR_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_sign_err_total",
        kind: MetricKind::Counter,
        help: "Count of failed signing operations.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const AEAD_OK_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_aead_ok_total",
        kind: MetricKind::Counter,
        help: "Count of successful AEAD operations.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const AEAD_ERR_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_crypto_aead_err_total",
        kind: MetricKind::Counter,
        help: "Count of failed AEAD operations.",
        buckets_ms: None,
        stable_labels: &[],
    };
}

#[cfg(feature = "observe")]
#[derive(Clone)]
struct ObservedHandles {
    canonical_ok: CounterHandle,
    canonical_err: CounterHandle,
    digest_ok: CounterHandle,
    digest_err: CounterHandle,
    sign_ok: CounterHandle,
    sign_err: CounterHandle,
    aead_ok: CounterHandle,
    aead_err: CounterHandle,
}

#[cfg(feature = "observe")]
impl ObservedHandles {
    fn new(meter: &dyn Meter) -> Self {
        Self {
            canonical_ok: meter.counter(&spec::CANONICAL_OK_TOTAL),
            canonical_err: meter.counter(&spec::CANONICAL_ERR_TOTAL),
            digest_ok: meter.counter(&spec::DIGEST_OK_TOTAL),
            digest_err: meter.counter(&spec::DIGEST_ERR_TOTAL),
            sign_ok: meter.counter(&spec::SIGN_OK_TOTAL),
            sign_err: meter.counter(&spec::SIGN_ERR_TOTAL),
            aead_ok: meter.counter(&spec::AEAD_OK_TOTAL),
            aead_err: meter.counter(&spec::AEAD_ERR_TOTAL),
        }
    }
}

#[derive(Clone)]
pub struct CryptoMetrics {
    inner: Arc<Inner>,
    #[cfg(feature = "observe")]
    observed: Option<ObservedHandles>,
}

#[derive(Default)]
struct Inner {
    canonical_ok: AtomicU64,
    canonical_err: AtomicU64,
    digest_ok: AtomicU64,
    digest_err: AtomicU64,
    sign_ok: AtomicU64,
    sign_err: AtomicU64,
    aead_ok: AtomicU64,
    aead_err: AtomicU64,
}

impl Default for CryptoMetrics {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            #[cfg(feature = "observe")]
            observed: None,
        }
    }
}

impl CryptoMetrics {
    #[cfg(feature = "observe")]
    pub fn with_meter(meter: &dyn Meter) -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            observed: Some(ObservedHandles::new(meter)),
        }
    }

    pub fn record_canonical_ok(&self) {
        self.inner.canonical_ok.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.canonical_ok.inc(1);
        }
    }

    pub fn record_canonical_err(&self) {
        self.inner.canonical_err.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.canonical_err.inc(1);
        }
    }

    pub fn record_digest_ok(&self) {
        self.inner.digest_ok.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.digest_ok.inc(1);
        }
    }

    pub fn record_digest_err(&self) {
        self.inner.digest_err.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.digest_err.inc(1);
        }
    }

    pub fn record_sign_ok(&self) {
        self.inner.sign_ok.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.sign_ok.inc(1);
        }
    }

    pub fn record_sign_err(&self) {
        self.inner.sign_err.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.sign_err.inc(1);
        }
    }

    pub fn record_aead_ok(&self) {
        self.inner.aead_ok.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.aead_ok.inc(1);
        }
    }

    pub fn record_aead_err(&self) {
        self.inner.aead_err.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.aead_err.inc(1);
        }
    }

    pub fn snapshot(&self) -> CryptoMetricsSnapshot {
        CryptoMetricsSnapshot {
            canonical_ok: self.inner.canonical_ok.load(Ordering::Relaxed),
            canonical_err: self.inner.canonical_err.load(Ordering::Relaxed),
            digest_ok: self.inner.digest_ok.load(Ordering::Relaxed),
            digest_err: self.inner.digest_err.load(Ordering::Relaxed),
            sign_ok: self.inner.sign_ok.load(Ordering::Relaxed),
            sign_err: self.inner.sign_err.load(Ordering::Relaxed),
            aead_ok: self.inner.aead_ok.load(Ordering::Relaxed),
            aead_err: self.inner.aead_err.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CryptoMetricsSnapshot {
    pub canonical_ok: u64,
    pub canonical_err: u64,
    pub digest_ok: u64,
    pub digest_err: u64,
    pub sign_ok: u64,
    pub sign_err: u64,
    pub aead_ok: u64,
    pub aead_err: u64,
}

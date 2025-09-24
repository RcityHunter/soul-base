use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct CryptoMetrics {
    inner: Arc<Inner>,
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

impl CryptoMetrics {
    pub fn record_canonical_ok(&self) {
        self.inner.canonical_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_canonical_err(&self) {
        self.inner.canonical_err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_digest_ok(&self) {
        self.inner.digest_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_digest_err(&self) {
        self.inner.digest_err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sign_ok(&self) {
        self.inner.sign_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sign_err(&self) {
        self.inner.sign_err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_aead_ok(&self) {
        self.inner.aead_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_aead_err(&self) {
        self.inner.aead_err.fetch_add(1, Ordering::Relaxed);
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

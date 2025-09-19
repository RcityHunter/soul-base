use soulbase_types::prelude::TenantId;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub struct OperationGuard {
    op: &'static str,
    tenant: Option<String>,
    target: Option<String>,
    start: Instant,
}

impl OperationGuard {
    pub fn new(op: &'static str, tenant: Option<&TenantId>, target: Option<&str>) -> Self {
        OperationGuard {
            op,
            tenant: tenant.map(|t| t.0.clone()),
            target: target.map(|t| t.to_string()),
            start: Instant::now(),
        }
    }

    pub fn finish(self, rows: usize, code: Option<&str>) {
        let latency = self.start.elapsed();
        record(
            self.op,
            self.tenant.as_deref(),
            self.target.as_deref(),
            latency,
            rows,
            code,
        );
    }
}

pub fn operation(
    op: &'static str,
    tenant: Option<&TenantId>,
    target: Option<&str>,
) -> OperationGuard {
    OperationGuard::new(op, tenant, target)
}

pub fn record(
    op: &'static str,
    tenant: Option<&str>,
    target: Option<&str>,
    latency: Duration,
    rows: usize,
    code: Option<&str>,
) {
    emit(op, tenant, target, latency, rows, code);
}

fn emit(
    op: &'static str,
    tenant: Option<&str>,
    target: Option<&str>,
    latency: Duration,
    rows: usize,
    code: Option<&str>,
) {
    #[cfg(feature = "observe")]
    {
        let mut labels = BTreeMap::new();
        if let Some(tenant) = tenant {
            labels.insert("tenant", tenant.to_string());
        }
        if let Some(target) = target {
            labels.insert("target", target.to_string());
        }
        if let Some(code) = code {
            labels.insert("code", code.to_string());
        }
        tracing::debug!(
            target = "soulbase::storage",
            op,
            latency_ms = latency.as_millis() as u64,
            rows,
            labels = ?labels,
            "storage operation"
        );
    }

    #[cfg(not(feature = "observe"))]
    let _ = (op, tenant, target, latency, rows, code);
}

pub fn labels(
    tenant: &TenantId,
    collection: &str,
    code: Option<&str>,
) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("tenant", tenant.0.clone());
    map.insert("collection", collection.to_string());
    if let Some(c) = code {
        map.insert("code", c.to_string());
    }
    map
}

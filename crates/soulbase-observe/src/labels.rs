pub const LBL_MIN: &[&str] = &[
    "tenant",
    "resource",
    "action",
    "route_id",
    "service",
    "method",
    "code",
    "kind",
    "retryable",
    "severity",
    "model_id",
    "provider",
    "tool_id",
    "sandbox_domain",
    "storage_table",
    "tx_kind",
    "config_version",
    "config_checksum",
];

/// Returns `true` if all required labels are present.
pub fn validate_minimal(labels: &[&str]) -> bool {
    LBL_MIN.iter().all(|expected| labels.contains(expected))
}

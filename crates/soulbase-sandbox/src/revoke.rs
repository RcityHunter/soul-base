use soulbase_types::prelude::Id;

/// Placeholder for revocation hooks (e.g. decision cache invalidation).
pub async fn revoke_decision(_subject_id: &Id) {
    // no-op for RIS
}

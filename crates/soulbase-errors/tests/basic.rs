use serde_json::json;
use soulbase_errors::prelude::*;

#[test]
fn build_and_render_public() {
    let err = ErrorBuilder::new(codes::AUTH_UNAUTHENTICATED)
        .user_msg("Please sign in.")
        .dev_msg("missing bearer token")
        .meta_kv("tenant", json!("tenantA"))
        .correlation("req-123")
        .build();

    let public_view = err.to_public();
    assert_eq!(public_view.code, "AUTH.UNAUTHENTICATED");
    assert_eq!(public_view.message, "Please sign in.");
    assert_eq!(public_view.correlation_id.as_deref(), Some("req-123"));

    let labels = labels(&err);
    assert_eq!(labels.get("code").unwrap(), "AUTH.UNAUTHENTICATED");
}

#[cfg(feature = "http")]
#[test]
fn http_status_mapping() {
    let err = ErrorBuilder::new(codes::QUOTA_RATELIMIT).build();
    let status = soulbase_errors::mapping_http::to_http_status(&err);
    assert_eq!(status.as_u16(), 429);
}

use soulbase_types::prelude::*;

#[test]
fn envelope_validates() {
    let actor = Subject {
        kind: SubjectKind::User,
        subject_id: Id("user_1".into()),
        tenant: TenantId("tenantA".into()),
        claims: Default::default(),
    };

    let envelope = Envelope::new(
        Id("env_1".into()),
        Timestamp(1_726_000_000_000),
        "tenantA:conv_1".into(),
        actor,
        "1.0.0",
        serde_json::json!({ "hello": "world" }),
    );

    assert!(envelope.validate().is_ok());
}

#[test]
fn envelope_rejects_tenant_mismatch() {
    let actor = Subject {
        kind: SubjectKind::Service,
        subject_id: Id("svc_1".into()),
        tenant: TenantId("tenantB".into()),
        claims: Default::default(),
    };

    let envelope = Envelope::new(
        Id("env_2".into()),
        Timestamp(1_726_000_000_000),
        "tenantA:conv_1".into(),
        actor,
        "1.0.0",
        serde_json::json!({}),
    );

    assert!(matches!(
        envelope.validate(),
        Err(ValidateError::TenantMismatch)
    ));
}

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

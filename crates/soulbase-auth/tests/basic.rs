use soulbase_auth::prelude::*;
use soulbase_auth::AuthFacade;

#[tokio::test]
async fn authorize_allow_path() {
    let facade = AuthFacade::minimal();
    let token = AuthnInput::BearerJwt("user_1@tenantA".into());
    let resource = ResourceUrn("soul:tool:browser".into());

    let decision = facade
        .authorize(
            token,
            resource.clone(),
            Action::Invoke,
            serde_json::json!({"allow": true, "cost": 2}),
            None,
            Some("corr-1".into()),
        )
        .await
        .expect("authorized");

    assert!(decision.allow);

    let cached = facade
        .authorize(
            AuthnInput::BearerJwt("user_1@tenantA".into()),
            resource,
            Action::Invoke,
            serde_json::json!({"allow": true, "cost": 2}),
            None,
            Some("corr-1".into()),
        )
        .await
        .expect("authorized");
    assert!(cached.allow);
}

#[tokio::test]
async fn authorize_deny_by_default() {
    let facade = AuthFacade::minimal();
    let decision = facade
        .authorize(
            AuthnInput::BearerJwt("user_2@tenantB".into()),
            ResourceUrn("soul:model:gpt-4o".into()),
            Action::Invoke,
            serde_json::json!({}),
            None,
            None,
        )
        .await
        .expect("decision ok");

    assert!(!decision.allow);
    assert_eq!(decision.reason.as_deref(), Some("deny-by-default"));
}

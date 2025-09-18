use futures::FutureExt;
use soulbase_auth::{prelude::*, AuthFacade};
use soulbase_interceptors::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

struct MockReq {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: serde_json::Value,
}

struct MockRes {
    status: u16,
    headers: HashMap<String, String>,
    body: Option<serde_json::Value>,
}

#[async_trait::async_trait]
impl ProtoRequest for MockReq {
    fn method(&self) -> &str {
        &self.method
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn header(&self, name: &str) -> Option<String> {
        self.headers.get(name).cloned()
    }

    async fn read_json(&mut self) -> Result<serde_json::Value, InterceptError> {
        Ok(self.body.clone())
    }
}

#[async_trait::async_trait]
impl ProtoResponse for MockRes {
    fn set_status(&mut self, code: u16) {
        self.status = code;
    }

    fn insert_header(&mut self, name: &str, value: &str) {
        self.headers.insert(name.to_string(), value.to_string());
    }

    async fn write_json(&mut self, body: &serde_json::Value) -> Result<(), InterceptError> {
        self.body = Some(body.clone());
        Ok(())
    }
}

#[tokio::test]
async fn pipeline_allows_when_attrs_allow_true() {
    let policy = RoutePolicy::new(vec![RoutePolicySpec {
        when: MatchCond::Http {
            method: "POST".into(),
            path_glob: "/v1/tool/run".into(),
        },
        bind: RouteBindingSpec {
            resource: "soul:tool:browser".into(),
            action: "Invoke".into(),
            attrs_template: None,
            attrs_from_body: true,
        },
    }]);

    let chain = InterceptorChain::new(vec![
        Box::new(ContextInitStage),
        Box::new(RoutePolicyStage { policy }),
        Box::new(AuthnMapStage {
            authenticator: Box::new(OidcAuthenticatorStub),
        }),
        Box::new(TenantGuardStage),
        Box::new(AuthzQuotaStage {
            facade: AuthFacade::minimal(),
        }),
        Box::new(ResilienceStage::new(
            Duration::from_secs(5),
            0,
            Duration::from_millis(0),
        )),
        Box::new(ResponseStampStage),
    ]);

    let mut req = MockReq {
        method: "POST".into(),
        path: "/v1/tool/run".into(),
        headers: [
            ("Authorization".into(), "Bearer user_1@tenantA".into()),
            ("X-Soul-Tenant".into(), "tenantA".into()),
        ]
        .into_iter()
        .collect(),
        body: serde_json::json!({"allow": true, "cost": 2}),
    };
    let mut res = MockRes {
        status: 0,
        headers: HashMap::new(),
        body: None,
    };

    let result = chain
        .run_with_handler(InterceptContext::default(), &mut req, &mut res, |_, r| {
            async move {
                let v = r.read_json().await?;
                Ok(serde_json::json!({"ok": true, "echo": v}))
            }
            .boxed()
        })
        .await;

    assert!(result.is_ok());
    assert_eq!(res.status, 200);
    assert!(res.headers.contains_key("X-Request-Id"));
    assert_eq!(res.body.as_ref().unwrap()["ok"], true);
}

#[tokio::test]
async fn pipeline_denies_when_not_allowed() {
    let policy = RoutePolicy::new(vec![RoutePolicySpec {
        when: MatchCond::Http {
            method: "POST".into(),
            path_glob: "/v1/tool/run".into(),
        },
        bind: RouteBindingSpec {
            resource: "soul:tool:browser".into(),
            action: "Invoke".into(),
            attrs_template: None,
            attrs_from_body: true,
        },
    }]);
    let chain = InterceptorChain::new(vec![
        Box::new(ContextInitStage),
        Box::new(RoutePolicyStage { policy }),
        Box::new(AuthnMapStage {
            authenticator: Box::new(OidcAuthenticatorStub),
        }),
        Box::new(TenantGuardStage),
        Box::new(AuthzQuotaStage {
            facade: AuthFacade::minimal(),
        }),
        Box::new(ResilienceStage::new(
            Duration::from_secs(5),
            0,
            Duration::from_millis(0),
        )),
        Box::new(ResponseStampStage),
    ]);

    let mut req = MockReq {
        method: "POST".into(),
        path: "/v1/tool/run".into(),
        headers: [
            ("Authorization".into(), "Bearer user_1@tenantA".into()),
            ("X-Soul-Tenant".into(), "tenantA".into()),
        ]
        .into_iter()
        .collect(),
        body: serde_json::json!({"allow": false}),
    };
    let mut res = MockRes {
        status: 0,
        headers: HashMap::new(),
        body: None,
    };

    let result = chain
        .run_with_handler(InterceptContext::default(), &mut req, &mut res, |_, _| {
            async move { Ok(serde_json::json!({"ok": false})) }.boxed()
        })
        .await;

    assert!(result.is_ok());
    assert_eq!(res.status, 403);
}

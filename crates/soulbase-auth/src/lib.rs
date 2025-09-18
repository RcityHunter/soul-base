pub mod attr;
pub mod authn;
pub mod cache;
pub mod consent;
pub mod errors;
pub mod events;
pub mod intercept;
pub mod model;
pub mod observe;
pub mod pdp;
pub mod prelude;
pub mod quota;

use prelude::*;
use soulbase_types::prelude::*;

pub struct AuthFacade {
    pub authenticator: Box<dyn Authenticator>,
    pub attr_provider: Box<dyn AttributeProvider>,
    pub authorizer: Box<dyn Authorizer>,
    pub consent: Box<dyn ConsentVerifier>,
    pub quota: Box<dyn QuotaStore>,
    pub cache: Box<dyn DecisionCache>,
}

impl AuthFacade {
    pub fn minimal() -> Self {
        Self {
            authenticator: Box::new(OidcAuthenticatorStub),
            attr_provider: Box::new(DefaultAttributeProvider),
            authorizer: Box::new(LocalAuthorizer),
            consent: Box::new(AlwaysOkConsent),
            quota: Box::new(MemoryQuota),
            cache: Box::new(MemoryDecisionCache::new(1_000)),
        }
    }

    pub async fn authorize(
        &self,
        input: AuthnInput,
        resource: ResourceUrn,
        action: Action,
        attrs: serde_json::Value,
        consent: Option<Consent>,
        correlation_id: Option<String>,
    ) -> Result<Decision, AuthError> {
        let subject = self.authenticator.authenticate(input.clone()).await?;

        let request = AuthzRequest {
            subject: subject.clone(),
            resource,
            action,
            attrs,
            consent,
            correlation_id,
        };

        let mut merged_attrs = request.attrs.clone();
        let augmentation = self
            .attr_provider
            .augment(&request)
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        merge_attrs(&mut merged_attrs, augmentation);

        let cache_key = decision_key(&request, &merged_attrs);
        if let Some(hit) = self.cache.get(&cache_key).await {
            return Ok(hit);
        }

        let mut decision = self.authorizer.decide(&request, &merged_attrs).await?;

        if let Some(consent_obj) = request.consent.as_ref() {
            if !self.consent.verify(consent_obj, &request).await? {
                decision.allow = false;
                decision.reason = Some("consent.invalid".into());
            }
        }

        if decision.allow {
            let quota_key = QuotaKey {
                tenant: request.subject.tenant.clone(),
                subject_id: request.subject.subject_id.clone(),
                resource: request.resource.clone(),
                action: request.action.clone(),
            };
            let outcome = self
                .quota
                .check_and_consume(&quota_key, cost_from_attrs(&merged_attrs))
                .await?;
            match outcome {
                QuotaOutcome::Allowed => {}
                QuotaOutcome::RateLimited => return Err(errors::rate_limited()),
                QuotaOutcome::BudgetExceeded => return Err(errors::budget_exceeded()),
            }
        }

        if decision.cache_ttl_ms > 0 {
            self.cache.put(cache_key, &decision).await;
        }

        Ok(decision)
    }
}

fn merge_attrs(base: &mut serde_json::Value, extra: serde_json::Value) {
    match (base, extra) {
        (serde_json::Value::Object(base_map), serde_json::Value::Object(extra_map)) => {
            for (k, v) in extra_map {
                merge_attrs(base_map.entry(k).or_insert(serde_json::Value::Null), v);
            }
        }
        (slot, value) => {
            *slot = value;
        }
    }
}

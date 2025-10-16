use super::Authorizer;
use crate::model::{Action, AuthzRequest, Decision, Obligation};
use crate::AuthError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    SubjectClaimEquals { claim: String, equals: String },
    SubjectTenantIn { tenants: Vec<String> },
    AttrEquals { path: String, equals: Value },
    AttrContains { path: String, contains: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyRule {
    pub id: String,
    pub description: Option<String>,
    pub resources: Vec<String>,
    pub actions: Vec<Action>,
    pub effect: PolicyEffect,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub obligations: Vec<Obligation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PolicySet {
    #[serde(default)]
    pub rules: Vec<PolicyRule>,
}

pub struct PolicyAuthorizer {
    rules: Vec<PolicyRule>,
}

impl PolicyAuthorizer {
    pub fn new(rules: Vec<PolicyRule>) -> Self {
        Self { rules }
    }

    fn resource_matches(pattern: &str, resource: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if !pattern.contains('*') {
            return pattern == resource;
        }
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            let start = parts[0];
            let end = parts[1];
            if !resource.starts_with(start) {
                return false;
            }
            if end.is_empty() {
                return true;
            }
            return resource.ends_with(end);
        }
        resource.contains(&parts.join(""))
    }

    fn action_matches(actions: &[Action], action: &Action) -> bool {
        actions.is_empty() || actions.iter().any(|a| a == action)
    }

    fn evaluate_conditions(
        conditions: &[Condition],
        req: &AuthzRequest,
        merged_attrs: &Value,
    ) -> bool {
        conditions.iter().all(|cond| match cond {
            Condition::SubjectClaimEquals { claim, equals } => req
                .subject
                .claims
                .get(claim)
                .and_then(|v| v.as_str())
                .map(|s| s == equals)
                .unwrap_or(false),
            Condition::SubjectTenantIn { tenants } => {
                tenants.iter().any(|t| t == &req.subject.tenant.0)
            }
            Condition::AttrEquals { path, equals } => Self::value_at_path(merged_attrs, path)
                .map(|v| v == equals)
                .unwrap_or(false),
            Condition::AttrContains { path, contains } => Self::value_at_path(merged_attrs, path)
                .map(|v| match v {
                    Value::String(s) => s.contains(contains),
                    Value::Array(items) => items.iter().any(|item| match item {
                        Value::String(s) => s.contains(contains),
                        _ => false,
                    }),
                    _ => false,
                })
                .unwrap_or(false),
        })
    }

    fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
        let mut current = value;
        for segment in path.split('.') {
            match current {
                Value::Object(map) => {
                    current = map.get(segment)?;
                }
                Value::Array(items) => {
                    let index: usize = segment.parse().ok()?;
                    current = items.get(index)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }
}

#[async_trait]
impl Authorizer for PolicyAuthorizer {
    async fn decide(
        &self,
        request: &AuthzRequest,
        merged_attrs: &Value,
    ) -> Result<Decision, AuthError> {
        for rule in &self.rules {
            if !rule
                .resources
                .iter()
                .any(|pattern| Self::resource_matches(pattern, &request.resource.0))
            {
                continue;
            }
            if !Self::action_matches(&rule.actions, &request.action) {
                continue;
            }
            if !Self::evaluate_conditions(&rule.conditions, request, merged_attrs) {
                continue;
            }

            let allow = matches!(rule.effect, PolicyEffect::Allow);
            let evidence = json!({
                "policy_id": rule.id,
                "effect": match rule.effect {
                    PolicyEffect::Allow => "allow",
                    PolicyEffect::Deny => "deny",
                },
                "resource": request.resource.0,
                "action": format!("{:?}", request.action),
            });
            return Ok(Decision {
                allow,
                reason: if allow {
                    None
                } else {
                    Some("policy-denied".into())
                },
                obligations: rule.obligations.clone(),
                evidence,
                cache_ttl_ms: if allow { 5_000 } else { 0 },
            });
        }
        Ok(Decision {
            allow: false,
            reason: Some("no-matching-policy".into()),
            obligations: Vec::new(),
            evidence: json!({
                "policy_id": null,
                "effect": "deny",
                "resource": request.resource.0,
                "action": format!("{:?}", request.action),
            }),
            cache_ttl_ms: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ResourceUrn;
    use soulbase_types::prelude::{Id, Subject, SubjectKind, TenantId};

    fn subject_with_claims(claims: Vec<(&str, &str)>) -> Subject {
        Subject {
            kind: SubjectKind::User,
            subject_id: Id("user-1".into()),
            tenant: TenantId("tenant-a".into()),
            claims: claims
                .into_iter()
                .map(|(k, v)| (k.into(), Value::String(v.into())))
                .collect(),
        }
    }

    #[tokio::test]
    async fn allow_rule_matches() {
        let rule = PolicyRule {
            id: "allow-read".into(),
            description: None,
            resources: vec!["urn:doc:*".into()],
            actions: vec![Action::Read],
            effect: PolicyEffect::Allow,
            conditions: vec![Condition::SubjectTenantIn {
                tenants: vec!["tenant-a".into()],
            }],
            obligations: Vec::new(),
        };
        let authorizer = PolicyAuthorizer::new(vec![rule]);
        let req = AuthzRequest {
            subject: subject_with_claims(vec![]),
            resource: ResourceUrn("urn:doc:123".into()),
            action: Action::Read,
            attrs: Value::Null,
            consent: None,
            correlation_id: None,
        };
        let decision = authorizer
            .decide(&req, &Value::Null)
            .await
            .expect("decision");
        assert!(decision.allow);
    }

    #[tokio::test]
    async fn deny_when_no_rule() {
        let authorizer = PolicyAuthorizer::new(vec![]);
        let req = AuthzRequest {
            subject: subject_with_claims(vec![]),
            resource: ResourceUrn("urn:doc:123".into()),
            action: Action::Read,
            attrs: Value::Null,
            consent: None,
            correlation_id: None,
        };
        let decision = authorizer
            .decide(&req, &Value::Null)
            .await
            .expect("decision");
        assert!(!decision.allow);
        assert_eq!(decision.reason.as_deref(), Some("no-matching-policy"));
    }
}

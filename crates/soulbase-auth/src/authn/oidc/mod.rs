#[cfg(feature = "authn-oidc")]
mod provider;

#[cfg(feature = "authn-oidc")]
pub use provider::{
    ClaimsAttributeConfig, ClaimsAttributeProvider, JwkConfig, JwkSource, OidcAuthenticator,
    OidcConfig,
};

use crate::errors;
use crate::model::AuthnInput;
use async_trait::async_trait;
use soulbase_types::prelude::{Id, Subject, SubjectKind, TenantId};

pub struct OidcAuthenticatorStub;

#[async_trait]
impl super::Authenticator for OidcAuthenticatorStub {
    async fn authenticate(&self, input: AuthnInput) -> Result<Subject, crate::errors::AuthError> {
        match input {
            AuthnInput::BearerJwt(token) => {
                let (sub, tenant) = token
                    .split_once('@')
                    .ok_or_else(|| errors::unauthenticated("Invalid bearer format"))?;
                Ok(Subject {
                    kind: SubjectKind::User,
                    subject_id: Id(sub.to_string()),
                    tenant: TenantId(tenant.to_string()),
                    claims: Default::default(),
                })
            }
            _ => Err(errors::unauthenticated("Unsupported authn input")),
        }
    }
}

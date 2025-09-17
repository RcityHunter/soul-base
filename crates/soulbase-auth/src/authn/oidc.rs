use super::*;
use crate::errors;
use soulbase_types::prelude::*;

pub struct OidcAuthenticatorStub;

#[async_trait::async_trait]
impl super::Authenticator for OidcAuthenticatorStub {
    async fn authenticate(&self, input: AuthnInput) -> Result<Subject, AuthError> {
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

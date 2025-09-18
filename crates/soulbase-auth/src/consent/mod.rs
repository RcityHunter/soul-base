use crate::errors::AuthError;
use crate::model::AuthzRequest;
use async_trait::async_trait;
use soulbase_types::prelude::Consent;

#[async_trait]
pub trait ConsentVerifier: Send + Sync {
    async fn verify(&self, consent: &Consent, request: &AuthzRequest) -> Result<bool, AuthError>;
}

pub struct AlwaysOkConsent;

#[async_trait::async_trait]
impl ConsentVerifier for AlwaysOkConsent {
    async fn verify(&self, _consent: &Consent, _request: &AuthzRequest) -> Result<bool, AuthError> {
        Ok(true)
    }
}

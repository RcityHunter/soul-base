use crate::errors::AuthError;
use crate::model::AuthnInput;
use async_trait::async_trait;
use soulbase_types::prelude::Subject;

pub mod oidc;

#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, input: AuthnInput) -> Result<Subject, AuthError>;
}

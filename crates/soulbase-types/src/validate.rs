use semver::Version;
use thiserror::Error;

use crate::{envelope::Envelope, subject::Subject};

#[derive(Debug, Error)]
pub enum ValidateError {
    #[error("empty_field:{0}")]
    EmptyField(&'static str),
    #[error("invalid_semver:{0}")]
    InvalidSemVer(String),
    #[error("tenant_mismatch")]
    TenantMismatch,
}

pub trait Validate {
    fn validate(&self) -> Result<(), ValidateError>;
}

impl Validate for Subject {
    fn validate(&self) -> Result<(), ValidateError> {
        if self.subject_id.0.is_empty() {
            return Err(ValidateError::EmptyField("subject_id"));
        }
        if self.tenant.0.is_empty() {
            return Err(ValidateError::EmptyField("tenant"));
        }
        Ok(())
    }
}

impl<T> Validate for Envelope<T> {
    fn validate(&self) -> Result<(), ValidateError> {
        if self.envelope_id.0.is_empty() {
            return Err(ValidateError::EmptyField("envelope_id"));
        }
        if self.partition_key.is_empty() {
            return Err(ValidateError::EmptyField("partition_key"));
        }
        if Version::parse(&self.schema_ver).is_err() {
            return Err(ValidateError::InvalidSemVer(self.schema_ver.clone()));
        }
        self.actor.validate()?;
        if !self.partition_key.contains(&self.actor.tenant.0) {
            return Err(ValidateError::TenantMismatch);
        }
        Ok(())
    }
}

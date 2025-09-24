use serde::{Deserialize, Serialize};

use crate::errors::CryptoError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyPolicy {
    pub kid: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub revoked: bool,
}

impl KeyPolicy {
    pub fn new(kid: impl Into<String>, issued_at_ms: i64, expires_at_ms: Option<i64>) -> Self {
        Self {
            kid: kid.into(),
            issued_at_ms,
            expires_at_ms,
            revoked: false,
        }
    }

    pub fn is_active(&self, now_ms: i64) -> Result<(), CryptoError> {
        if self.revoked {
            return Err(CryptoError::keystore_forbidden(&format!(
                "key {} has been revoked",
                self.kid
            )));
        }
        if let Some(exp) = self.expires_at_ms {
            if now_ms > exp {
                return Err(CryptoError::keystore_forbidden(&format!(
                    "key {} expired at {}",
                    self.kid, exp
                )));
            }
        }
        Ok(())
    }

    pub fn remaining_ttl(&self, now_ms: i64) -> Option<i64> {
        self.expires_at_ms.map(|exp| exp - now_ms)
    }
}

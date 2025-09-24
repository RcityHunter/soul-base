use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ed25519_dalek::{SigningKey, VerifyingKey};
use rand_core::OsRng;
use zeroize::Zeroizing;

use crate::errors::CryptoError;

use super::jwk::{JwkPrivateKey, JwkPublicKey};
use super::policy::KeyPolicy;

#[derive(Clone, Debug)]
pub struct SigningKeyMaterial {
    pub kid: String,
    pub signing_key: SigningKey,
    pub verifying_key: VerifyingKey,
    pub policy: KeyPolicy,
}

#[derive(Clone, Debug)]
pub struct VerifyKeyMaterial {
    pub kid: String,
    pub verifying_key: VerifyingKey,
    pub policy: KeyPolicy,
}

pub trait KeyStore: Clone + Send + Sync {
    fn current_signing_key(&self) -> Result<SigningKeyMaterial, CryptoError>;
    fn key_for_verification(&self, kid: &str) -> Result<VerifyKeyMaterial, CryptoError>;
    fn revoke(&self, kid: &str);
    fn export_public_jwk(&self, kid: &str) -> Result<JwkPublicKey, CryptoError>;
    fn export_private_jwk(&self, kid: &str) -> Result<JwkPrivateKey, CryptoError>;
}

#[derive(Clone)]
pub struct MemoryKeyStore {
    state: Arc<RwLock<KeyStoreState>>,
}

impl MemoryKeyStore {
    pub fn generate(kid: impl Into<String>, ttl_ms: i64) -> Self {
        let mut rng = OsRng;
        let signing = SigningKey::generate(&mut rng);
        let verifying = signing.verifying_key();
        let secret = Zeroizing::new(signing.to_bytes());
        let now = now_ms();
        let expires = if ttl_ms > 0 { Some(now + ttl_ms) } else { None };
        let kid = kid.into();
        let policy = KeyPolicy::new(kid.clone(), now, expires);
        let mut keys = HashMap::new();
        keys.insert(
            kid.clone(),
            KeyEntry {
                secret,
                verifying_key: verifying,
                policy,
            },
        );
        MemoryKeyStore {
            state: Arc::new(RwLock::new(KeyStoreState { keys, current: kid })),
        }
    }

    pub fn current_kid(&self) -> String {
        self.state
            .read()
            .expect("keystore lock poisoned")
            .current
            .clone()
    }

    fn entry(&self, kid: &str) -> Option<KeyEntry> {
        self.state
            .read()
            .ok()
            .and_then(|guard| guard.keys.get(kid).cloned())
    }

    fn ensure_active(policy: &KeyPolicy) -> Result<(), CryptoError> {
        policy.is_active(now_ms())
    }
}

impl KeyStore for MemoryKeyStore {
    fn current_signing_key(&self) -> Result<SigningKeyMaterial, CryptoError> {
        let entry = {
            let guard = self
                .state
                .read()
                .map_err(|_| CryptoError::keystore_unavailable("keystore lock poisoned"))?;
            let current = guard.current.clone();
            guard.keys.get(&current).cloned()
        }
        .ok_or_else(|| CryptoError::keystore_unavailable("current signing key missing"))?;

        let KeyEntry {
            secret,
            verifying_key,
            policy,
        } = entry;
        MemoryKeyStore::ensure_active(&policy)?;
        let signing_key = SigningKey::from_bytes(&secret);
        Ok(SigningKeyMaterial {
            kid: policy.kid.clone(),
            signing_key,
            verifying_key,
            policy,
        })
    }

    fn key_for_verification(&self, kid: &str) -> Result<VerifyKeyMaterial, CryptoError> {
        let entry = self.entry(kid).ok_or_else(|| {
            CryptoError::keystore_forbidden(&format!("verification key {kid} not found"))
        })?;
        let KeyEntry {
            secret: _,
            verifying_key,
            policy,
        } = entry;
        MemoryKeyStore::ensure_active(&policy)?;
        Ok(VerifyKeyMaterial {
            kid: policy.kid.clone(),
            verifying_key,
            policy,
        })
    }

    fn revoke(&self, kid: &str) {
        if let Ok(mut guard) = self.state.write() {
            if let Some(entry) = guard.keys.get_mut(kid) {
                entry.policy.revoked = true;
            }
        }
    }

    fn export_public_jwk(&self, kid: &str) -> Result<JwkPublicKey, CryptoError> {
        let entry = self.entry(kid).ok_or_else(|| {
            CryptoError::keystore_forbidden(&format!("public jwk {kid} not found"))
        })?;
        Ok(JwkPublicKey::from_verifying_key(
            entry.policy.kid.clone(),
            &entry.verifying_key,
            entry.policy.expires_at_ms,
        ))
    }

    fn export_private_jwk(&self, kid: &str) -> Result<JwkPrivateKey, CryptoError> {
        let entry = self.entry(kid).ok_or_else(|| {
            CryptoError::keystore_forbidden(&format!("private jwk {kid} not found"))
        })?;
        Ok(JwkPrivateKey::from_keys(
            entry.policy.kid.clone(),
            &entry.secret,
            &entry.verifying_key,
            entry.policy.expires_at_ms,
        ))
    }
}

#[derive(Clone)]
struct KeyEntry {
    secret: Zeroizing<[u8; 32]>,
    verifying_key: VerifyingKey,
    policy: KeyPolicy,
}

struct KeyStoreState {
    keys: HashMap<String, KeyEntry>,
    current: String,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as i64
}

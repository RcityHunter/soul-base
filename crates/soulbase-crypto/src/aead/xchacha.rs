use chacha20poly1305::aead::{Aead as _, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};

use crate::errors::CryptoError;

use super::mod_::Aead;

#[derive(Clone, Default)]
pub struct XChaChaAead;

impl XChaChaAead {
    fn cipher(&self, key: &[u8]) -> Result<XChaCha20Poly1305, CryptoError> {
        if key.len() != 32 {
            return Err(CryptoError::aead("XChaCha20-Poly1305 key must be 32 bytes"));
        }
        Ok(XChaCha20Poly1305::new(Key::from_slice(key)))
    }

    fn ensure_nonce<'a>(&self, nonce: &'a [u8]) -> Result<&'a XNonce, CryptoError> {
        if nonce.len() != 24 {
            return Err(CryptoError::aead(
                "XChaCha20-Poly1305 nonce must be 24 bytes",
            ));
        }
        Ok(XNonce::from_slice(nonce))
    }
}

impl Aead for XChaChaAead {
    fn seal(
        &self,
        key: &[u8],
        nonce: &[u8],
        aad: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        let cipher = self.cipher(key)?;
        let nonce = self.ensure_nonce(nonce)?;
        cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|err| CryptoError::aead(&format!("seal failed: {err}")))
    }

    fn open(
        &self,
        key: &[u8],
        nonce: &[u8],
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        let cipher = self.cipher(key)?;
        let nonce = self.ensure_nonce(nonce)?;
        cipher
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| CryptoError::aead("ciphertext authentication failed"))
    }
}

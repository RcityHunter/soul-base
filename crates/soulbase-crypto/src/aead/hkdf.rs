use hkdf::Hkdf;
use sha2::Sha256;

use crate::errors::CryptoError;

pub fn hkdf_extract_expand(salt: &[u8], ikm: &[u8], info: &[u8], length: usize) -> Vec<u8> {
    hkdf_extract_expand_checked(salt, ikm, info, length)
        .expect("hkdf output length exceeds maximum and should be validated upstream")
}

pub fn hkdf_extract_expand_checked(
    salt: &[u8],
    ikm: &[u8],
    info: &[u8],
    length: usize,
) -> Result<Vec<u8>, CryptoError> {
    let hk = Hkdf::<Sha256>::new(Some(salt), ikm);
    let mut okm = vec![0u8; length];
    hk.expand(info, &mut okm)
        .map_err(|err| CryptoError::hkdf(&format!("hkdf expand failed: {err}")))?;
    Ok(okm)
}

use crate::errors::BlobError;
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use urlencoding::encode;

type HmacSha256 = Hmac<Sha256>;

pub fn presign_get(
    secret: &str,
    bucket: &str,
    key: &str,
    expire_secs: u32,
) -> Result<String, BlobError> {
    let exp = Utc::now().timestamp() + expire_secs as i64;
    let to_sign = format!("GET\n{bucket}\n{key}\n{exp}");
    let sig = hmac_base64(secret, &to_sign)?;
    Ok(format!(
        "fs:///{bucket}/{key}?exp={exp}&sig={}",
        encode(&sig)
    ))
}

pub fn presign_put(
    secret: &str,
    bucket: &str,
    key: &str,
    expire_secs: u32,
    content_type: Option<String>,
    size: Option<u64>,
) -> Result<String, BlobError> {
    let exp = Utc::now().timestamp() + expire_secs as i64;
    let ct = content_type.unwrap_or_default();
    let size_val = size.unwrap_or(0);
    let to_sign = format!("PUT\n{bucket}\n{key}\n{exp}\n{ct}\n{size_val}");
    let sig = hmac_base64(secret, &to_sign)?;
    Ok(format!(
        "fs:///{bucket}/{key}?exp={exp}&ct={}&size={size_val}&sig={}",
        encode(&ct),
        encode(&sig)
    ))
}

pub fn verify_url(
    secret: &str,
    method: &str,
    bucket: &str,
    key: &str,
    exp: i64,
    content_type: Option<&str>,
    size: Option<u64>,
    sig: &str,
) -> bool {
    if Utc::now().timestamp() > exp {
        return false;
    }
    let base = match method {
        "GET" => format!("GET\n{bucket}\n{key}\n{exp}"),
        "PUT" => format!(
            "PUT\n{bucket}\n{key}\n{exp}\n{}\n{}",
            content_type.unwrap_or(""),
            size.unwrap_or(0)
        ),
        _ => return false,
    };
    match hmac_base64(secret, &base) {
        Ok(expected) => expected == sig,
        Err(_) => false,
    }
}

fn hmac_base64(secret: &str, msg: &str) -> Result<String, BlobError> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|err| BlobError::schema(&format!("invalid hmac secret: {err}")))?;
    mac.update(msg.as_bytes());
    let out = mac.finalize().into_bytes();
    Ok(base64::engine::general_purpose::STANDARD.encode(out))
}

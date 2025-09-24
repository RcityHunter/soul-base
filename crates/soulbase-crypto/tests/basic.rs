use rand::RngCore;
use serde_json::json;
use soulbase_crypto::prelude::*;

#[test]
fn canonical_json_is_stable_and_rejects_float() {
    let cano = JsonCanonicalizer::default();
    let a = json!({"b":2,"a":1,"c":{"y":1,"x":2},"arr":[3,2,1]});
    let b = json!({"c":{"x":2,"y":1},"a":1,"arr":[3,2,1],"b":2});

    let ca = cano.canonical_json(&a).unwrap();
    let cb = cano.canonical_json(&b).unwrap();
    assert_eq!(ca, cb);

    let f = json!({"a": 1.23});
    assert!(cano.canonical_json(&f).is_err());
}

#[test]
fn digest_and_commit() {
    let cano = JsonCanonicalizer::default();
    let dig = DefaultDigester::default();
    let payload = json!({"a":1,"b":2});
    let d = dig.commit_json(&cano, &payload, "sha256").unwrap();
    assert_eq!(d.algo, "sha256");
    assert!(d.size > 0);
    assert!(!d.as_base64url().is_empty());
}

#[cfg(feature = "jws-ed25519")]
#[test]
fn sign_and_verify_ed25519_jws_detached() {
    let ks = MemoryKeyStore::generate("ed25519:2025-01:keyA", 600_000);
    let signer = JwsEd25519Signer {
        keystore: ks.clone(),
    };
    let verifier = JwsEd25519Verifier {
        keystore: ks.clone(),
    };

    let payload = br#"{"msg":"hello"}"#;
    let jws = signer.sign_detached(payload).unwrap();
    verifier.verify_detached("unused", payload, &jws).unwrap();

    let bad = br#"{"msg":"hEllo"}"#;
    assert!(verifier.verify_detached("unused", bad, &jws).is_err());

    ks.revoke(&ks.current_kid());
    assert!(verifier.verify_detached("unused", payload, &jws).is_err());
}

#[cfg(feature = "aead-xchacha")]
#[test]
fn aead_xchacha_roundtrip_and_hkdf() {
    let aead = XChaChaAead::default();
    let key = hkdf_extract_expand(b"salt", b"ikm", b"tenant:resource:env", 32);

    let mut nonce = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut nonce);

    let aad = b"tenant|resource|env";
    let pt = b"secret-plaintext";
    let ct = aead.seal(&key, &nonce, aad, pt).unwrap();
    let dec = aead.open(&key, &nonce, aad, &ct).unwrap();
    assert_eq!(pt.to_vec(), dec);

    assert!(aead.open(&key, &nonce, b"mismatch", &ct).is_err());
}

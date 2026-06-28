//! Shared ed25519 update-signing helpers for the Sparkle-family adapters
//! (R509-F3 sparkle, R509-F4 winsparkle).
//!
//! Both adapters produce a Sparkle-shaped appcast whose enclosure may carry an
//! EdDSA `sparkle:edSignature` over the update archive's bytes. The key-loading
//! and signing logic is identical, so it lives here and both call it. ed25519
//! signatures are deterministic (RFC 8032), so a fixed key + fixed archive
//! yields a stable signature — which keeps dry-run output and tests stable.

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};

use crate::runner::RunnerError;

/// Decode + load an ed25519 signing key from its base64 slot value. Accepts a
/// 32-byte seed or the 64-byte `seed‖public` export Sparkle/WinSparkle emit
/// (the leading 32 bytes are the seed). Tolerates surrounding whitespace.
/// `slot` is the credential slot name, used only for error messages.
pub fn parse_signing_key(b64: &str, slot: &str) -> Result<SigningKey, RunnerError> {
    let cleaned: String = b64.split_whitespace().collect();
    let raw = base64::engine::general_purpose::STANDARD
        .decode(cleaned.as_bytes())
        .map_err(|e| {
            RunnerError::Outcome(format!(
                "{slot} is not valid base64 (expected a base64 ed25519 key): {e}"
            ))
        })?;
    let seed: [u8; 32] = match raw.len() {
        32 | 64 => raw[..32].try_into().expect("len checked"),
        n => {
            return Err(RunnerError::Outcome(format!(
                "{slot} decoded to {n} bytes; expected 32 (seed) or 64 (seed‖public)"
            )))
        }
    };
    Ok(SigningKey::from_bytes(&seed))
}

/// Sign `bytes` and return the base64 of the 64-byte ed25519 signature — the
/// `sparkle:edSignature` value.
pub fn sign_b64(key: &SigningKey, bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(key.sign(bytes).to_bytes())
}

/// First 12 chars of a base64 signature, for dry-run action lines.
pub fn sig_prefix(sig: &str) -> &str {
    &sig[..sig.len().min(12)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Verifier, VerifyingKey};

    const SEED: [u8; 32] = [9u8; 32];

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn parses_32_and_64_byte_keys_equivalently() {
        let vk = SigningKey::from_bytes(&SEED).verifying_key();
        let seed64: Vec<u8> = SEED.iter().copied().chain(vk.to_bytes()).collect();
        let k32 = parse_signing_key(&b64(&SEED), "SLOT").unwrap();
        let k64 = parse_signing_key(&b64(&seed64), "SLOT").unwrap();
        assert_eq!(k32.to_bytes(), k64.to_bytes());
    }

    #[test]
    fn wrong_length_names_the_slot() {
        let err = parse_signing_key(&b64(&[1u8; 16]), "MY_SLOT").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("MY_SLOT") && msg.contains("expected 32"));
    }

    #[test]
    fn bad_base64_names_the_slot() {
        let err = parse_signing_key("not base64!!!", "MY_SLOT").unwrap_err();
        assert!(format!("{err}").contains("MY_SLOT"));
    }

    #[test]
    fn signature_is_deterministic_and_validates() {
        let key = SigningKey::from_bytes(&SEED);
        let a = sign_b64(&key, b"payload");
        let b = sign_b64(&key, b"payload");
        assert_eq!(a, b, "ed25519 is deterministic");
        let sig = ed25519_dalek::Signature::from_slice(
            &base64::engine::general_purpose::STANDARD.decode(&a).unwrap(),
        )
        .unwrap();
        let vk: VerifyingKey = key.verifying_key();
        assert!(vk.verify(b"payload", &sig).is_ok());
    }

    #[test]
    fn sig_prefix_truncates() {
        assert_eq!(sig_prefix("abcdefghijklmnop"), "abcdefghijkl");
        assert_eq!(sig_prefix("short"), "short");
    }
}

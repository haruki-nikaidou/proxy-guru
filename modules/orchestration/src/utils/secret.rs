//! Encryption at rest for the secrets this module stores: DNS provider API
//! tokens, ACME account keys and certificate private keys.
//!
//! One symmetric master key, read from the environment (`GURU_MASTER_KEY`, 32
//! bytes base64), encrypts every secret with XChaCha20-Poly1305 under a fresh
//! random nonce. A stored secret is the string `enc1:<base64(nonce || ciphertext)>`;
//! the prefix is the format version, so a future key or cipher rotation can tell
//! old rows apart. The database never sees a plaintext secret and the API never
//! returns one.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use std::sync::Arc;

pub const MASTER_KEY_ENV: &str = "GURU_MASTER_KEY";
const FORMAT_PREFIX: &str = "enc1:";
const NONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("{MASTER_KEY_ENV} is not set")]
    Missing,
    #[error("{MASTER_KEY_ENV} must be {KEY_LEN} bytes encoded as base64")]
    InvalidKey,
    #[error("stored secret has an unknown format")]
    Format,
    #[error("stored secret could not be decrypted: wrong master key or corrupted row")]
    Decrypt,
    #[error("secret could not be encrypted")]
    Encrypt,
}

/// The master key. Cheap to clone; services own one.
#[derive(Clone)]
pub struct SecretKey {
    cipher: Arc<XChaCha20Poly1305>,
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(..)")
    }
}

impl SecretKey {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SecretError> {
        if bytes.len() != KEY_LEN {
            return Err(SecretError::InvalidKey);
        }
        let cipher =
            XChaCha20Poly1305::new_from_slice(bytes).map_err(|_| SecretError::InvalidKey)?;
        Ok(Self {
            cipher: Arc::new(cipher),
        })
    }

    /// Parses the base64 form used by `GURU_MASTER_KEY`.
    pub fn from_base64(encoded: &str) -> Result<Self, SecretError> {
        let bytes = BASE64
            .decode(encoded.trim())
            .map_err(|_| SecretError::InvalidKey)?;
        Self::from_bytes(&bytes)
    }

    pub fn from_env() -> Result<Self, SecretError> {
        let encoded = std::env::var(MASTER_KEY_ENV).map_err(|_| SecretError::Missing)?;
        Self::from_base64(&encoded)
    }

    /// A fresh random key in the base64 form `GURU_MASTER_KEY` expects; what
    /// `manage-tool` prints for a new deployment.
    pub fn generate_base64() -> String {
        let mut bytes = [0u8; KEY_LEN];
        rand::fill(&mut bytes);
        BASE64.encode(bytes)
    }

    /// Encrypts `plaintext` into the stored string form.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<String, SecretError> {
        let mut nonce = [0u8; NONCE_LEN];
        rand::fill(&mut nonce);
        let ciphertext = self
            .cipher
            .encrypt(&XNonce::from(nonce), plaintext)
            .map_err(|_| SecretError::Encrypt)?;
        let mut out = Vec::with_capacity(NONCE_LEN.saturating_add(ciphertext.len()));
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(format!("{FORMAT_PREFIX}{}", BASE64.encode(out)))
    }

    pub fn encrypt_str(&self, plaintext: &str) -> Result<String, SecretError> {
        self.encrypt(plaintext.as_bytes())
    }

    pub fn decrypt(&self, stored: &str) -> Result<Vec<u8>, SecretError> {
        let encoded = stored
            .strip_prefix(FORMAT_PREFIX)
            .ok_or(SecretError::Format)?;
        let bytes = BASE64.decode(encoded).map_err(|_| SecretError::Format)?;
        if bytes.len() < NONCE_LEN {
            return Err(SecretError::Format);
        }
        let (nonce, ciphertext) = bytes.split_at(NONCE_LEN);
        let nonce = XNonce::try_from(nonce).map_err(|_| SecretError::Format)?;
        self.cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|_| SecretError::Decrypt)
    }

    pub fn decrypt_str(&self, stored: &str) -> Result<String, SecretError> {
        String::from_utf8(self.decrypt(stored)?).map_err(|_| SecretError::Decrypt)
    }
}
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_rejects_the_wrong_key() {
        let key = SecretKey::from_base64(&SecretKey::generate_base64()).unwrap();
        let stored = key.encrypt_str("cf-token").unwrap();
        assert!(stored.starts_with("enc1:"));
        assert_eq!(key.decrypt_str(&stored).unwrap(), "cf-token");
        assert_ne!(
            stored,
            key.encrypt_str("cf-token").unwrap(),
            "nonce must be fresh"
        );

        let other = SecretKey::from_base64(&SecretKey::generate_base64()).unwrap();
        assert!(matches!(other.decrypt(&stored), Err(SecretError::Decrypt)));
        assert!(matches!(key.decrypt("plain"), Err(SecretError::Format)));
        assert!(matches!(
            SecretKey::from_base64("c2hvcnQ="),
            Err(SecretError::InvalidKey)
        ));
    }
}

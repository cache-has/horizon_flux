// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Encryption and key derivation for the secret store.
//!
//! Uses AES-256-GCM for authenticated encryption and Argon2id for
//! password-based key derivation.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::Argon2;
use rand::Rng;

use crate::error::SecretError;

/// Length of the salt used for Argon2 key derivation (16 bytes).
const SALT_LEN: usize = 16;
/// Length of the AES-256-GCM nonce (12 bytes).
const NONCE_LEN: usize = 12;
/// Length of the derived AES-256 key (32 bytes).
const KEY_LEN: usize = 32;

/// Derive a 256-bit encryption key from a password and salt using Argon2id.
pub fn derive_key(password: &[u8], salt: &[u8]) -> Result<[u8; KEY_LEN], SecretError> {
    let mut key = [0u8; KEY_LEN];
    Argon2::default()
        .hash_password_into(password, salt, &mut key)
        .map_err(|e| SecretError::Encryption(format!("key derivation failed: {e}")))?;
    Ok(key)
}

/// Generate a random salt for Argon2.
pub fn generate_salt() -> [u8; SALT_LEN] {
    rand::rng().random()
}

/// Encrypt a plaintext value using AES-256-GCM.
///
/// Returns `nonce || ciphertext` (12 bytes nonce prepended to ciphertext).
pub fn encrypt(key: &[u8; KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| SecretError::Encryption(format!("cipher init failed: {e}")))?;

    let nonce_bytes: [u8; NONCE_LEN] = rand::rng().random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| SecretError::Encryption(format!("encryption failed: {e}")))?;

    // Prepend nonce to ciphertext for storage.
    let mut result = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt a value produced by [`encrypt`].
///
/// Expects `nonce || ciphertext` format (12-byte nonce prefix).
pub fn decrypt(key: &[u8; KEY_LEN], data: &[u8]) -> Result<Vec<u8>, SecretError> {
    if data.len() < NONCE_LEN {
        return Err(SecretError::Decryption("ciphertext too short".to_string()));
    }

    let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| SecretError::Decryption(format!("cipher init failed: {e}")))?;

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| SecretError::Decryption(format!("decryption failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let password = b"test-password";
        let salt = generate_salt();
        let key = derive_key(password, &salt).unwrap();

        let plaintext = b"postgres://user:pass@host/db";
        let encrypted = encrypt(&key, plaintext).unwrap();
        let decrypted = decrypt(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let salt = generate_salt();
        let key1 = derive_key(b"password1", &salt).unwrap();
        let key2 = derive_key(b"password2", &salt).unwrap();

        let encrypted = encrypt(&key1, b"secret-value").unwrap();
        assert!(decrypt(&key2, &encrypted).is_err());
    }
}

//! MySQL password authentication: `mysql_native_password` and
//! `caching_sha2_password`.
//!
//! Only the on-wire algorithms live here. Plugin names are resolved by the
//! caller; an unknown plugin is rejected before any scramble is computed.

use database_types::{Error, Result};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

/// Authentication plugin advertised by the server handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plugin {
    /// Legacy SHA-1 challenge-response plugin.
    Native,
    /// MySQL 8 default, SHA-256 fast auth with an RSA fallback.
    CachingSha2,
}

impl Plugin {
    /// Map the wire plugin name onto a supported plugin.
    pub(crate) fn parse(name: &str) -> Result<Self> {
        match name.trim_matches('\0') {
            "mysql_native_password" => Ok(Self::Native),
            "caching_sha2_password" => Ok(Self::CachingSha2),
            other => Err(Error::Protocol { message: format!("unsupported MySQL authentication plugin `{other}`") }),
        }
    }

    /// Compute the initial auth response for this plugin.
    pub(crate) fn scramble(self, password: &str, nonce: &[u8]) -> Vec<u8> {
        match self {
            Self::Native => native_scramble(password, nonce),
            Self::CachingSha2 => sha2_scramble(password, nonce),
        }
    }
}

/// `SHA1(password) XOR SHA1(nonce || SHA1(SHA1(password)))`.
pub(crate) fn native_scramble(password: &str, nonce: &[u8]) -> Vec<u8> {
    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(&stage1);
    let mut seeded = nonce.to_vec();
    seeded.extend_from_slice(&stage2);
    let stage3 = Sha1::digest(&seeded);
    stage1.iter().zip(stage3.iter()).map(|(left, right)| left ^ right).collect()
}

/// `SHA256(password) XOR SHA256(SHA256(SHA256(password)) || nonce)`.
pub(crate) fn sha2_scramble(password: &str, nonce: &[u8]) -> Vec<u8> {
    let stage1 = Sha256::digest(password.as_bytes());
    let stage2 = Sha256::digest(&stage1);
    let mut seeded = stage2.to_vec();
    seeded.extend_from_slice(nonce);
    let stage3 = Sha256::digest(&seeded);
    stage1.iter().zip(stage3.iter()).map(|(left, right)| left ^ right).collect()
}

/// RSA-OAEP(SHA-1) payload for the `caching_sha2_password` full-auth path.
///
/// MySQL 用 OpenSSL 的 `RSA_PKCS1_OAEP_PADDING`，其 OAEP 摘要固定为 SHA-1，不是
/// 密钥长度对应的 SHA-256。明文是「密码 + 结尾 NUL」与 scramble nonce 逐字节循环 XOR。
pub(crate) fn rsa_encrypt(password: &str, nonce: &[u8], public_key_pem: &[u8]) -> Result<Vec<u8>> {
    use rsa::{Oaep, RsaPublicKey, pkcs8::DecodePublicKey};

    if nonce.is_empty() {
        return Err(Error::Protocol { message: "caching_sha2_password sent no scramble nonce".into() });
    }
    // 官方客户端按 C 字符串长度（含结尾 NUL）加密，缺这 1 字节服务器就会判密码不符。
    let mut plaintext = password.as_bytes().to_vec();
    plaintext.push(0);
    for (index, byte) in plaintext.iter_mut().enumerate() {
        *byte ^= nonce[index % nonce.len()];
    }
    let pem = std::str::from_utf8(public_key_pem)
        .map_err(|_| Error::Protocol { message: "caching_sha2_password public key is not valid UTF-8".into() })?;
    let key = RsaPublicKey::from_public_key_pem(pem).map_err(|error| Error::Protocol {
        message: format!("caching_sha2_password public key cannot be parsed: {error}"),
    })?;
    let mut rng = rsa::rand_core::OsRng;
    key.encrypt(&mut rng, Oaep::new::<sha1_rsa::Sha1>(), &plaintext).map_err(|error| Error::Protocol {
        message: format!("caching_sha2_password RSA encryption failed: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONCE: &[u8] = b"12345678901234567890";

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// 期望值由官方 `libmysql` 在同一次握手中实际发出的字节比对得到。
    #[test]
    fn scrambles_match_reference_vectors() {
        assert_eq!(hex(&sha2_scramble("secret", NONCE)), "51ecd6dedbd34d5445c0a190d4f51acf0d23b94db66c91f3f789faa9193751cd");
        assert_eq!(hex(&native_scramble("secret", NONCE)), "0f8b9033e0897c0a8338ebe3dea9010dda47ab56");
    }

    #[test]
    fn native_scramble_is_twenty_bytes_and_nonce_sensitive() {
        let first = native_scramble("secret", NONCE);
        assert_eq!(first.len(), 20);
        assert_ne!(first, native_scramble("secret", b"abcdefghijklmnopqrst"));
    }

    #[test]
    fn sha2_scramble_is_thirty_two_bytes_and_nonce_sensitive() {
        let first = sha2_scramble("secret", NONCE);
        assert_eq!(first.len(), 32);
        assert_ne!(first, sha2_scramble("secret", b"abcdefghijklmnopqrst"));
    }

    #[test]
    fn an_empty_password_still_produces_a_scramble() {
        assert_eq!(native_scramble("", NONCE).len(), 20);
        assert_eq!(sha2_scramble("", NONCE).len(), 32);
    }

    #[test]
    fn unknown_plugins_are_rejected_by_name() {
        assert!(Plugin::parse("sha256_password").is_err());
        assert_eq!(Plugin::parse("mysql_native_password\0").unwrap(), Plugin::Native);
        assert_eq!(Plugin::parse("caching_sha2_password").unwrap(), Plugin::CachingSha2);
    }
}

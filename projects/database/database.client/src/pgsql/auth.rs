//! PostgreSQL password authentication: MD5 and SCRAM-SHA-256.
//!
//! Only the on-wire algorithms live here. SASLprep is not applied; the
//! PostgreSQL server applies it to the stored verifier, and the common
//! ASCII password path is byte-identical without normalization.

use base64::{Engine as _, engine::general_purpose::STANDARD};

use super::error::PostgresError;

/// `md5(md5(password || user) || salt)` formatted the way PostgreSQL expects
/// in an MD5 password response.
pub(crate) fn md5_password(password: &str, user: &str, salt: &[u8]) -> String {
    let inner = md5_hex(format!("{password}{user}").as_bytes());
    let mut material = inner.into_bytes();
    material.extend_from_slice(salt);
    format!("md5{}", md5_hex(&material))
}

/// RFC 1321 MD5 as lowercase hex.
pub(crate) fn md5_hex(input: &[u8]) -> String {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 4,
        11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501, 0x698098d8, 0x8b44f7af,
        0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
        0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8,
        0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
        0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244, 0x432aff97,
        0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
        0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
    ];

    let mut padded = input.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&((input.len() as u64) * 8).to_le_bytes());

    let mut state = [0x67452301u32, 0xefcdab89, 0x98badcfe, 0x10325476];
    for chunk in padded.chunks(64) {
        let mut words = [0u32; 16];
        for (index, word) in words.iter_mut().enumerate() {
            *word = u32::from_le_bytes([chunk[index * 4], chunk[index * 4 + 1], chunk[index * 4 + 2], chunk[index * 4 + 3]]);
        }
        let [mut a, mut b, mut c, mut d] = state;
        for round in 0..64 {
            let (mix, index) = match round {
                0..=15 => ((b & c) | (!b & d), round),
                16..=31 => ((d & b) | (!d & c), (5 * round + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * round + 5) % 16),
                _ => (c ^ (b | !d), (7 * round) % 16),
            };
            let rotated = a.wrapping_add(mix).wrapping_add(K[round]).wrapping_add(words[index]).rotate_left(S[round]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(rotated);
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
    }

    state.iter().flat_map(|word| word.to_le_bytes()).map(|byte| format!("{byte:02x}")).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    }
    else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|byte| byte ^ 0x36));
    inner.update(data);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(block.map(|byte| byte ^ 0x5c));
    outer.update(inner);
    outer.finalize().into()
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut seeded = salt.to_vec();
    seeded.extend_from_slice(&1u32.to_be_bytes());
    let mut previous = hmac_sha256(password, &seeded);
    let mut output = previous;
    for _ in 1..iterations {
        previous = hmac_sha256(password, &previous);
        for (slot, value) in output.iter_mut().zip(previous) {
            *slot ^= value;
        }
    }
    output
}

/// One SCRAM-SHA-256 exchange driven by the server's challenge messages.
pub(crate) struct ScramClient {
    client_first_bare: String,
    server_first: String,
    nonce: String,
    password: String,
    expected_server_signature: Option<[u8; 32]>,
}

impl ScramClient {
    /// Builds the client-first message and returns it for the SASL initial response.
    pub(crate) fn new(user: &str, password: &str) -> Result<(Self, String), PostgresError> {
        let mut random = [0u8; 18];
        getrandom::fill(&mut random).map_err(|error| PostgresError::Protocol(format!("cannot generate a SCRAM nonce: {error}")))?;
        Ok(Self::with_nonce(user, password, &STANDARD.encode(random)))
    }

    fn with_nonce(user: &str, password: &str, nonce: &str) -> (Self, String) {
        let client_first_bare = format!("n={},r={nonce}", scram_escape(user));
        let state = Self {
            client_first_bare: client_first_bare.clone(),
            server_first: String::new(),
            nonce: nonce.to_owned(),
            password: password.to_owned(),
            expected_server_signature: None,
        };
        (state, format!("n,,{client_first_bare}"))
    }

    /// Handles `SASLContinue` and returns the client-final message.
    pub(crate) fn continue_with(&mut self, server_first: &str) -> Result<String, PostgresError> {
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for field in server_first.split(',') {
            if let Some(value) = field.strip_prefix("r=") {
                nonce = Some(value.to_owned());
            }
            else if let Some(value) = field.strip_prefix("s=") {
                salt = Some(value.to_owned());
            }
            else if let Some(value) = field.strip_prefix("i=") {
                iterations = Some(value.parse::<u32>().map_err(|_| PostgresError::Protocol("invalid SCRAM iteration count".into()))?);
            }
        }
        let nonce = nonce.ok_or(PostgresError::Protocol("SCRAM server-first is missing the nonce".into()))?;
        let salt = salt.ok_or(PostgresError::Protocol("SCRAM server-first is missing the salt".into()))?;
        let iterations = iterations.ok_or(PostgresError::Protocol("SCRAM server-first is missing the iteration count".into()))?;
        if !nonce.starts_with(&self.nonce) {
            return Err(PostgresError::Protocol("SCRAM server nonce does not extend the client nonce".into()));
        }
        let decoded_salt = STANDARD
            .decode(&salt)
            .map_err(|error| PostgresError::Protocol(format!("SCRAM salt is not valid base64: {error}")))?;
        let salted_password = pbkdf2_sha256(self.password.as_bytes(), &decoded_salt, iterations);
        self.server_first = server_first.to_owned();

        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key: [u8; 32] = {
            use sha2::{Digest, Sha256};
            Sha256::digest(client_key).into()
        };
        let client_final_without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{},{}", self.client_first_bare, self.server_first) + "," + &client_final_without_proof;
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut proof = client_key;
        for (slot, value) in proof.iter_mut().zip(client_signature) {
            *slot ^= value;
        }
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        self.expected_server_signature = Some(hmac_sha256(&server_key, auth_message.as_bytes()));
        Ok(format!("{client_final_without_proof},p={}", STANDARD.encode(proof)))
    }

    /// Verifies the `SASLFinal` server signature.
    pub(crate) fn verify(&self, server_final: &str) -> Result<(), PostgresError> {
        if let Some(error) = server_final.strip_prefix("e=") {
            return Err(PostgresError::Protocol(format!("SCRAM authentication failed: {error}")));
        }
        let signature = server_final
            .strip_prefix("v=")
            .ok_or(PostgresError::Protocol("SCRAM server-final is missing the signature".into()))?;
        let decoded = STANDARD
            .decode(signature)
            .map_err(|error| PostgresError::Protocol(format!("SCRAM signature is not valid base64: {error}")))?;
        let expected = self.expected_server_signature.ok_or(PostgresError::Protocol("SCRAM final arrived before continue".into()))?;
        if decoded != expected {
            return Err(PostgresError::Protocol("SCRAM server signature mismatch".into()));
        }
        Ok(())
    }
}

fn scram_escape(value: &str) -> String {
    value.replace('=', "=3D").replace(',', "=2C")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_matches_rfc1321_vectors() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(md5_hex(b"The quick brown fox jumps over the lazy dog"), "9e107d9d372bb6826bd81d3542a419d6");
    }

    #[test]
    fn md5_password_matches_the_postgres_algorithm() {
        let response = md5_password("secret", "postgres", &[1, 2, 3, 4]);
        let inner = md5_hex(b"secretpostgres");
        let mut material = inner.into_bytes();
        material.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(response, format!("md5{}", md5_hex(&material)));
    }

    #[test]
    fn pbkdf2_matches_rfc_6070_shape() {
        assert_eq!(
            pbkdf2_sha256(b"password", b"salt", 1),
            [0x12, 0x0f, 0xb6, 0xcf, 0xfc, 0xf8, 0xb3, 0x2c, 0x43, 0xe7, 0x22, 0x52, 0x56, 0xc4, 0xf8, 0x37, 0xa8, 0x65, 0x48, 0xc9, 0x2c, 0xcc, 0x35, 0x48, 0x08, 0x05, 0x98, 0x7c, 0xb7, 0x0b, 0xe1, 0x7b]
        );
    }

    #[test]
    fn scram_matches_rfc_7677_vector() {
        let nonce = "rOprNGfwEbeRWgbNEkqO";
        let (mut client, client_first) = ScramClient::with_nonce("user", "pencil", nonce);
        assert_eq!(client_first, format!("n,,n=user,r={nonce}"));
        let server_first =
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final = client.continue_with(server_first).expect("continue");
        assert_eq!(
            client_final,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        client.verify("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=").expect("verify");
    }

    #[test]
    fn scram_rejects_a_nonce_the_server_did_not_extend() {
        let (mut client, _) = ScramClient::with_nonce("user", "pencil", "abc");
        let server_first = "r=zzz,s=QSXCR+Q6sek8bf92,i=4096";
        assert!(client.continue_with(server_first).is_err());
    }

    #[test]
    fn scram_rejects_a_tampered_server_signature() {
        let (mut client, _) = ScramClient::with_nonce("user", "pencil", "abc");
        let server_first = "r=abc123,s=QSXCR+Q6sek8bf92,i=4096";
        client.continue_with(server_first).expect("continue");
        assert!(client.verify("v=rmF9pqV8S7suAoZWja4dJRkFsKQ=").is_err());
    }
}

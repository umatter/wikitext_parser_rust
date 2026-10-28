//! Text fingerprints: 16 lowercase hex characters of XXH3-64.

use xxhash_rust::xxh3::xxh3_64;

pub fn fp(normalised: &str) -> String {
    format!("{:016x}", xxh3_64(normalised.as_bytes()))
}

/// Lowercase, collapse Unicode whitespace (`char::is_whitespace`) to one
/// space, trim.
pub fn norm(s: &str) -> String {
    s.to_lowercase().split_whitespace().collect::<Vec<_>>().join(" ")
}

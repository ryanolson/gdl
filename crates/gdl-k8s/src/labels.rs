//! Label values, which hold at most 63 characters, and the hashes that
//! shorten them and name a ConfigMap's contents.

use serde_json::Value;

/// The longest label value, and the longest name of a Service: a DNS
/// label's.
pub const LABEL_LIMIT: usize = 63;

/// The characters of a long value that its label keeps.
pub const LABEL_PREFIX: usize = 52;

/// `value` as a label's value, which is at most 63 characters: itself when
/// it fits, else its first 52 characters, `-`, and 10 hex digits of its
/// hash, so that two long values that share their start still differ. The
/// whole value is then in the annotation of the label's key.
pub fn label_value(value: &str) -> String {
    if value.len() <= LABEL_LIMIT {
        return value.to_string();
    }
    let hash = format!("{:016x}", fnv(value));
    format!("{}-{}", &value[..LABEL_PREFIX], &hash[..10])
}

/// A checksum of a ConfigMap's data, FNV-1a over its JSON text, as hex.
pub fn checksum(config: &Value) -> String {
    format!("{:016x}", fnv(&config["data"].to_string()))
}

/// FNV-1a of `text`.
pub fn fnv(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value of up to 63 characters is its own label; a longer one is its
    /// first 52 characters, `-`, and 10 hex digits of its hash, and two
    /// that share those 52 characters still differ.
    #[test]
    fn a_long_value_is_labelled_by_its_start_and_hash() {
        let fits = "m".repeat(63);
        assert_eq!(label_value(&fits), fits);
        let long = "m".repeat(64);
        let label = label_value(&long);
        assert_eq!(label.len(), 63, "{label}");
        let (start, hash) = label.split_at(LABEL_PREFIX);
        assert_eq!(start, &long[..LABEL_PREFIX]);
        assert!(hash.starts_with('-') && hash[1..].bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(label_value(&"m".repeat(65)), label);
    }
}

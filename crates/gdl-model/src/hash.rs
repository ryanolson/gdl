/// FNV-1a, 64-bit. The contract and placement hashes cross process and
/// release boundaries, so they cannot use `std`'s hasher, whose output is not
/// stable across Rust versions. They identify configurations; they are not a
/// security boundary.
pub(crate) struct Fnv(u64);

impl Fnv {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    pub(crate) fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }

    /// Length-prefixed, so that `["ab", "c"]` and `["a", "bc"]` differ.
    pub(crate) fn str(&mut self, value: &str) {
        self.u64(value.len() as u64);
        self.bytes(value.as_bytes());
    }

    pub(crate) fn strs<S: AsRef<str>>(&mut self, values: &[S]) {
        self.u64(values.len() as u64);
        for value in values {
            self.str(value.as_ref());
        }
    }

    pub(crate) fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::Fnv;

    #[test]
    fn matches_the_published_fnv1a_vector() {
        let mut h = Fnv::new();
        h.bytes(b"a");
        assert_eq!(h.finish(), 0xaf63_dc4c_8601_ec8c);
    }

    #[test]
    fn string_boundaries_change_the_hash() {
        let mut a = Fnv::new();
        a.strs(&["ab", "c"]);
        let mut b = Fnv::new();
        b.strs(&["a", "bc"]);
        assert_ne!(a.finish(), b.finish());
    }
}

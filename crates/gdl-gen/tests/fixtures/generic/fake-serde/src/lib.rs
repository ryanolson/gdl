//! Traits of serde's names, which a crate may take as `serde`: a name
//! trick the source reader does not chase (ruling 195).

/// Not serde's.
pub trait Serialize {}

/// Not serde's.
pub trait Deserialize<'de> {}

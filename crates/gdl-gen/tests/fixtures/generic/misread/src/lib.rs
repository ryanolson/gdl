//! A carrier whose "serde" impls are another crate's traits, under serde's
//! name: the source reader takes it for a wire type, which it is not.

/// A carrier.
#[derive(gdl_message::Message)]
pub struct Misread<T>(pub T);

impl<T> serde::Serialize for Misread<T> {}

impl<'de, T> serde::Deserialize<'de> for Misread<T> {}

/// The instantiation a graph carries across a cut.
#[gdl_message::export]
pub type MisreadByte = Misread<u8>;

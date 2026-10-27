//! An instantiation the reader takes for local and a build for wire: the
//! serde impls of `Wrap` bound their parameter on `Clone`, a bound the
//! reader does not judge. `cargo gdl extract --probe` records the build's
//! reading.

/// A generic message whose serde impls ask more of `T` than serde does.
#[derive(gdl_message::Message)]
pub struct Wrap<T> {
    /// The value.
    pub value: T,
}

impl<T: Clone + serde::Serialize> serde::Serialize for Wrap<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(serializer)
    }
}

impl<'de, T: Clone + serde::Deserialize<'de>> serde::Deserialize<'de> for Wrap<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(|value| Self { value })
    }
}

/// `String` is `Clone`: the build finds `Wrap<String>` a wire type.
#[gdl_message::export]
pub type Texts = Wrap<String>;

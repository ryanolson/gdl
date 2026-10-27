//! A generic message and an instantiation of it, from a registry.

/// A generic message.
#[derive(gdl_message::Message)]
pub struct Wrap<T> {
    /// The value.
    pub value: T,
}

/// An instantiation, which a probe would read.
#[gdl_message::export]
pub type Texts = Wrap<String>;

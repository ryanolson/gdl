//! Message types for gdl graphs, without the gdl runtime.
//!
//! A protocols crate derives [`Message`] on its message types and depends
//! on this crate alone. Each derived type joins the [`Catalogue`] of every
//! program that links its crate, which is how a graph file's message names
//! resolve to types. `gdl` re-exports this crate's items.

mod catalogue;
mod shape;

pub use catalogue::{Catalogue, MessageInfo};
pub use shape::{Message, Shape};

/// Offer a type to graph files by its name.
///
/// ```ignore
/// #[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
/// pub struct Prompt {
///     pub text: String,
/// }
/// ```
///
/// The derive implements [`Message`] and [`Shape`], and adds the type to
/// the [`Catalogue`] of every program that links its crate, with its
/// package, name, definition path, and whether it is a wire type. The type
/// must be `Send + 'static` and not generic: a graph names it by name
/// alone. The expansion names this crate as `::gdl_message`, so a crate
/// that derives `Message` depends on it directly.
pub use gdl_macros::Message;

/// Support for the derive's expansion. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::catalogue::registry::{Entry, GDL_MESSAGE_CATALOGUE};
    pub use linkme;

    /// Whether `T` is a wire type: `Serialize` and `DeserializeOwned`.
    ///
    /// The derive expands it where `T` is concrete, by autoref: the bounded
    /// impl is on `Probe<T>` and the fallback on `&Probe<T>`, so method
    /// lookup on `&Probe<T>` tries the bounded impl first.
    #[macro_export]
    #[doc(hidden)]
    macro_rules! __gdl_wire {
        ($ty:ty) => {{
            #[allow(unused_imports)]
            use ::gdl_message::__private::{ViaLocal as _, ViaWire as _};
            (&::gdl_message::__private::Probe::<$ty>::new()).__gdl_wire()
        }};
    }

    pub struct Probe<T>(core::marker::PhantomData<fn() -> T>);

    impl<T> Probe<T> {
        pub const fn new() -> Self {
            Self(core::marker::PhantomData)
        }
    }

    impl<T> Default for Probe<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    pub trait ViaWire {
        fn __gdl_wire(&self) -> bool;
    }

    impl<T: serde::Serialize + serde::de::DeserializeOwned> ViaWire for Probe<T> {
        fn __gdl_wire(&self) -> bool {
            true
        }
    }

    pub trait ViaLocal {
        fn __gdl_wire(&self) -> bool;
    }

    impl<T> ViaLocal for &Probe<T> {
        fn __gdl_wire(&self) -> bool {
            false
        }
    }
}

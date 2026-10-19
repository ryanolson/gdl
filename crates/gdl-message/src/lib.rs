//! Message types for gdl graphs, without the gdl runtime.
//!
//! A protocols crate derives [`Message`] on its message types and depends
//! on this crate alone. Each derived type joins the [`Catalogue`] of every
//! program that links its crate, which is how a graph file's message names
//! resolve to types. `gdl` re-exports this crate's items.

mod catalogue;
mod shape;

pub use catalogue::{Catalogue, ExportInfo, MessageInfo, NodeInfo, Origin, PortInfo, RoleInfo};
pub use shape::{Identity, Message, Shape};

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
/// that derives `Message` this way depends on it directly; `gdl::Message`
/// is the same derive for a crate that depends on gdl instead.
pub use gdl_macros::Message;

/// Offer messages under this crate's own paths: on a `pub use` of message
/// types, it keeps the `use` and registers each name, with the identity of
/// the message it names.
///
/// ```ignore
/// mod defs { #[derive(gdl_message::Message)] pub struct Prompt; }
/// #[gdl_message::export]
/// pub use defs::Prompt;
/// ```
///
/// A crate that lists this one offers `Prompt` at this path, though its
/// definition is in a private module. Each name must be a message, and a
/// glob names none.
pub use gdl_macros::export;

/// Support for the markers' expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::catalogue::registry::{
        Entry, Export, GDL_EXPORT_CATALOGUE, GDL_MESSAGE_CATALOGUE, GDL_NODE_CATALOGUE, Node, Port,
        Role, port,
    };
    pub use crate::shape::{Identity, Message as MessageTrait, Shape};
    pub use linkme;

    /// Whether a type is a wire type: `Serialize` and `DeserializeOwned`.
    ///
    /// The derive calls it where the type is concrete, by autoref: the
    /// bounded impl is on `WireProbe<T>` and the fallback on `&WireProbe<T>`,
    /// so method lookup on `&WireProbe<T>` tries the bounded impl first.
    pub struct WireProbe<T>(core::marker::PhantomData<fn() -> T>);

    impl<T> WireProbe<T> {
        pub const fn new() -> Self {
            Self(core::marker::PhantomData)
        }
    }

    impl<T> Default for WireProbe<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    pub trait WireYes {
        fn __gdl_wire(&self) -> bool;
    }

    impl<T: serde::Serialize + serde::de::DeserializeOwned> WireYes for WireProbe<T> {
        fn __gdl_wire(&self) -> bool {
            true
        }
    }

    pub trait WireNo {
        fn __gdl_wire(&self) -> bool;
    }

    impl<T> WireNo for &WireProbe<T> {
        fn __gdl_wire(&self) -> bool {
            false
        }
    }
}

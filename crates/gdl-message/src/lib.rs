//! Message types for gdl graphs, without the gdl runtime.
//!
//! A protocols crate derives [`Message`] on its message types and depends
//! on this crate alone. Each derived type joins the [`Catalogue`] of every
//! program that links its crate, which is how a graph file's message names
//! resolve to types. `gdl` re-exports this crate's items.

mod catalogue;
mod shape;

pub use catalogue::{
    Catalogue, EndpointInfo, ExportInfo, MessageInfo, NodeInfo, Origin, PortInfo, RoleInfo,
};
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
/// must be `Send + 'static`. The expansion names this crate as
/// `::gdl_message`, so a crate that derives `Message` this way depends on
/// it directly; `gdl::Message` is the same derive for a crate that depends
/// on gdl instead.
///
/// A generic type, with type parameters only, registers nothing itself: a
/// graph names a message by name alone, so each instantiation is a
/// message where an [`export`]ed `pub type` names it.
///
/// `#[gdl(codec = "msgpack")]` selects the default named-field MessagePack
/// encoding. `msgpack_array` selects positional arrays, whose fields must
/// not be conditionally omitted. `json` selects JSON. The encoding applies
/// to the complete value; nested messages do not select another codec.
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
/// definition is in a private module. Each name must be a message that is
/// `Send + 'static`, and a glob names none.
///
/// On a `pub type` that names an instantiation of a generic message, it
/// offers that instantiation under the alias's path and name:
///
/// ```ignore
/// #[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
/// #[gdl(codec = "json")]
/// pub struct Carried<P, B> { pub payload: P, pub builder: B }
///
/// #[gdl_message::export]
/// pub type PromptCarried = Carried<Prompt, Plain>;
/// ```
///
/// The instantiation is one message whatever names it, identified by its
/// type; its codec is the generic type's, and it is a wire type when that
/// instantiation implements `Serialize` and `DeserializeOwned`. The alias
/// takes no parameters of its own. `gdl::export` also offers an
/// instantiation of a generic node.
pub use gdl_macros::export;

/// Support for the markers' expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::catalogue::registry::{
        Alias, Endpoint, Entry, Export, GDL_ALIAS_CATALOGUE, GDL_EXPORT_CATALOGUE,
        GDL_MESSAGE_CATALOGUE, GDL_NODE_CATALOGUE, Node, Port, Role, port,
    };
    pub use crate::shape::{Identity, Message as MessageTrait, Shape};
    pub use gdl_model::{ConfigField, Encoding};
    pub use linkme;

    /// A message type's identity and declared encoding: what
    /// `derive(Message)` implements on a type, and on every instantiation
    /// of a generic one, which has no `Message` impl since it has no one
    /// name, and no constant identity.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is not a message",
        label = "an export offers a type that derives `Message`, or an instantiation of a generic one"
    )]
    pub trait Offered {
        /// The declared encoding.
        const ENCODING: Encoding;
        /// The message's identity.
        fn identity() -> Identity;
    }

    /// Compiles only for a type that can cross tasks, as every message
    /// must. An export calls it on each name it offers: an instantiation
    /// of a generic message has no `Message` impl to require it, and a
    /// `pub use` can name one through an alias that does not.
    pub const fn sent<T: Send + 'static>() {}

    /// What a generic type's derive is: a message or a node.
    pub enum Kind {
        /// `derive(Message)`.
        Message,
        /// `derive(gdl::Node)`.
        Node,
    }

    /// An instantiation of a generic message or node, which an exported
    /// `pub type` names: implemented by the derive on the generic type for
    /// every instantiation, so the alias's registration reads it where the
    /// type is concrete.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is not an instantiation of a generic message or node",
        label = "an exported alias names an instantiation of a type that derives `Message` or `gdl::Node`",
        note = "a message that is not generic is offered under another path with `#[export] pub use`"
    )]
    pub trait Generic {
        /// Which derive it is, for `gdl_message::export` to refuse a node
        /// before anything runs.
        const KIND: Kind;
        /// What the derive states of this instantiation.
        fn instantiation() -> Instantiation;
    }

    /// What a generic type's derive states of one instantiation: the rest
    /// of what an alias registers is probed where the alias is.
    pub enum Instantiation {
        /// `derive(Message)`: the message's identity, which names its
        /// type, and its declared encoding.
        Message {
            /// The identity.
            identity: Identity,
            /// The declared encoding.
            encoding: Encoding,
        },
        /// `derive(gdl::Node)`: its graph configuration fields.
        Node {
            /// The fields, as the node's `ConfigSchema` states them.
            config: fn() -> Vec<ConfigField>,
        },
    }

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

    pub trait DeclaredEncoding {
        fn __gdl_encoding(&self) -> Encoding;
    }

    impl<T: Offered> DeclaredEncoding for WireProbe<T> {
        fn __gdl_encoding(&self) -> Encoding {
            T::ENCODING
        }
    }

    pub trait DefaultEncoding {
        fn __gdl_encoding(&self) -> Encoding;
    }

    impl<T> DefaultEncoding for &WireProbe<T> {
        fn __gdl_encoding(&self) -> Encoding {
            Encoding::default()
        }
    }
}

/// The encoding of a concrete message type, including an unmarked serde type.
#[doc(hidden)]
#[macro_export]
macro_rules! message_encoding {
    ($ty:ty) => {{
        #[allow(unused_imports)]
        use $crate::__private::{DeclaredEncoding as _, DefaultEncoding as _};
        (&$crate::__private::WireProbe::<$ty>::new()).__gdl_encoding()
    }};
}

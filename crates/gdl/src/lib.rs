//! A graph definition language and runtime for distributed request/response
//! streaming pipelines.
//!
//! A graph is a source and a nested chain of hops. Each hop has a forward
//! chain that transforms the request, an inner end (an [`Operator`] or a
//! [`Sink`]), and a backward chain that transforms the response stream. An
//! operator sees both directions and calls the rest of the graph through
//! [`Next`]; forward and backward nodes see one direction each.
//!
//! The design and its rulings are in `agent-docs/plan.md`.

mod compose;
mod config;
mod cx;
mod decl;
mod dist;
mod error;
mod identity;
mod items;
#[cfg(feature = "json-lines")]
mod json;
mod process;
mod role;
mod service;
mod streamed;
mod wire;

pub use compose::{BackwardChain, ForwardChain, Hop, Inner, OperatorStage, SinkStage, Source};
pub use config::build_node;
pub use cx::{Cx, Deadline, RequestId};
pub use decl::{Absent, BackwardDecl, ForwardDecl, NodeDecl, OperatorDecl, SinkDecl, SourceDecl};
pub use dist::{
    BwdEnd, BwdStep, DBackward, DForward, DHop, DInner, DOperator, DSink, Deployment, Entries,
    FwdEnd, FwdStep, Register, Roster, RuntimeConfig, Segment, Served, bind_node,
};
#[cfg(feature = "fault-injection")]
pub use dist::{Faults, Gate};
#[cfg(feature = "hub")]
pub use dist::{HubSession, bind_container_node};
pub use error::{Error, ErrorKind, Result};
pub use figment;
pub use gdl_macros::{Config, Endpoints};
pub use gdl_model::{
    ConfigField, ConfigSchema, ConfigValue, CrossingReport, DeploymentSpec, GraphConfig,
    HUB_CONTROL_PORT, HUB_PUBLIC_PORT, HopInfo, HopLayout, ItemInfo, Layout, Manifest, NodeInfo,
    Placement, PlacementError, PlacementErrors, RequestRouter, Role, Run, SegmentRegistration,
    TypeInfo, WireReason, layout, resolve_config,
};
pub use identity::{IdentityNode, IdentityOperator};
pub use items::{Items, items};
#[cfg(feature = "json-lines")]
pub use json::serve_json_lines;
pub use process::{Drains, Hosted, Launch, Process, ProcessConfig, segment_main};
pub use role::{Backward, Build, Forward, Frontend, Next, Operator, Sink};
pub use service::{Client, Endpoints, Join, Service};
pub use streamed::Streamed;
/// The velo version this runtime is built on: a deployment's nodes are
/// velo nodes, and generated segment crates reach velo through here so they
/// cannot pick a different version.
pub use velo;
pub use wire::{Codec, RequestCodec, StreamCodec};

/// Declare a graph: its nodes, their message types, and its topology.
///
/// ```ignore
/// gdl::graph! {
///     name = "chat", schema = 1;
///     source   A: Req -> stream Chunk;
///     operator B: (Req -> stream Chunk) over (Pre -> stream Out);
///     forward  C: Pre -> Pre;
///     sink     D: Pre -> stream Raw;
///     backward F: Raw -> Tok;
///     backward E: Tok -> Out;
///     backward G: Chunk -> Chunk;
///     topology: A => B { C => D => F => E } => G;
/// }
/// ```
///
/// A request position (a source's, a sink's, either request of an operator,
/// and both sides of a forward node) also takes a streamed request: `Head +
/// stream Item`, or `stream Item` for a head of `()`. Its port type is
/// [`Streamed<Head, Item>`](Streamed). Both ends of a forward edge must agree on
/// whether it is streamed, and response types are never streamed.
///
/// In the topology, each hop lists its forward nodes, then its inner end (a
/// sink, or an operator with its own hop in braces), then its backward nodes
/// in the order the response passes through them. The expansion defines:
///
/// - `nodes`: a marker type per node, so `Absent<nodes::X>` fills a slot
///   this segment does not host;
/// - `Bindings`: one slot per node besides the source, filled with
///   `Bindings::new().c(...).d(...)`; each setter states its node's role and
///   types, so a mismatched implementation fails to compile there;
/// - `manifest()`: the graph as data, for placement validation;
/// - `assemble(&deployment, bindings)`: the hops as one segment runs them;
///   it fails if the deployment was made for another contract, or if a node
///   the segment hosts has no binding;
/// - `local(bindings)`: the whole graph as nested calls in one process;
/// - `SourceReq` and `SourceItem`: aliases for the source's request and item
///   types.
///
/// Bump `schema` whenever a message type's wire form changes.
///
/// The expansion reaches gdl through `$crate`, so a contract crate can
/// depend on gdl under another name. Names starting with `__gdl` or `__Gdl`
/// are reserved for the expansion.
#[macro_export]
macro_rules! graph {
    ($($graph:tt)*) => {
        $crate::__private::graph! { $crate; $($graph)* }
    };
}

/// Message types, from `gdl-message`. A protocols crate depends on that
/// crate alone; a node crate can reach the same items here.
pub use gdl_message::{Catalogue, Identity, MessageInfo, Shape};

/// The `Message` trait, and the derive of the same name: offer a type to
/// graphs by its name. This derive names gdl in its expansion, so a crate
/// that depends on gdl needs nothing else; `gdl_message::Message` is the
/// same derive for a crate that depends on `gdl-message` alone.
pub use gdl_macros::GdlMessage as Message;
#[doc(inline)]
pub use gdl_message::__private::MessageTrait as Message;

/// Offer messages under this crate's own paths: on a `pub use` of message
/// types, it keeps the `use` and registers each name with the identity of
/// the message it names. The same attribute as `gdl_message::export`,
/// named through gdl.
pub use gdl_macros::gdl_export as export;

/// Mark the type that implements a node.
///
/// ```ignore
/// #[derive(gdl::Node)]
/// pub struct Engine { .. }
/// impl gdl::Sink for Engine { type Req = Words; type Item = Token; .. }
/// ```
///
/// The node's name is the type's name. The type must be `pub`, not generic,
/// and reachable at the crate root under its name, because generated
/// segments name it there. The derive registers the type with each role
/// trait it implements and the messages at its ports, for
/// `cargo gdl extract --verify`; the source reader finds the same impl in
/// the crate's source. A node implements exactly one role trait.
pub use gdl_macros::Node;

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::wire::probe::{Probe, ViaLocal, ViaWire};
    pub use gdl_macros::graph;
    pub use gdl_message::__private::*;
    pub use gdl_model::__private::*;

    use crate::{Backward, Forward, Frontend, Operator, Sink};

    /// The type whose role traits `derive(Node)` probes. For each role, the
    /// bounded impl is on `RoleProbe<T>` and the fallback on
    /// `&RoleProbe<T>`, so method lookup on `&RoleProbe<T>` finds the
    /// bounded impl when the type implements the role with a message at
    /// every port.
    pub struct RoleProbe<T>(core::marker::PhantomData<fn() -> T>);

    impl<T> RoleProbe<T> {
        pub const fn new() -> Self {
            Self(core::marker::PhantomData)
        }
    }

    impl<T> Default for RoleProbe<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    macro_rules! probe {
        ($yes:ident, $no:ident, $method:ident, $role:ident, $name:literal, [$($port:ident),+]) => {
            pub trait $yes {
                fn $method(&self) -> Option<Role>;
            }

            impl<T: $role> $yes for RoleProbe<T>
            where
                $(T::$port: Shape,)+
            {
                fn $method(&self) -> Option<Role> {
                    Some(Role {
                        entries: Vec::new(),
                        role: $name,
                        ports: vec![$((stringify!($port), port::<T::$port>())),+],
                    })
                }
            }

            pub trait $no {
                fn $method(&self) -> Option<Role>;
            }

            impl<T> $no for &RoleProbe<T> {
                fn $method(&self) -> Option<Role> {
                    None
                }
            }
        };
    }

    pub trait ServiceYes {
        fn __gdl_service(&self) -> Option<Role>;
    }
    impl<T: crate::Service> ServiceYes for RoleProbe<T> {
        fn __gdl_service(&self) -> Option<Role> {
            Some(Role {
                role: "source",
                ports: Vec::new(),
                entries: <T::Clients as crate::Endpoints>::entries(),
            })
        }
    }
    pub trait ServiceNo {
        fn __gdl_service(&self) -> Option<Role>;
    }
    impl<T> ServiceNo for &RoleProbe<T> {
        fn __gdl_service(&self) -> Option<Role> {
            None
        }
    }

    probe!(
        FrontendYes,
        FrontendNo,
        __gdl_frontend,
        Frontend,
        "source",
        [Req, Item]
    );
    probe!(SinkYes, SinkNo, __gdl_sink, Sink, "sink", [Req, Item]);
    probe!(
        ForwardYes,
        ForwardNo,
        __gdl_forward,
        Forward,
        "forward",
        [In, Out]
    );
    probe!(
        BackwardYes,
        BackwardNo,
        __gdl_backward,
        Backward,
        "backward",
        [In, Out]
    );
    probe!(
        OperatorYes,
        OperatorNo,
        __gdl_operator,
        Operator,
        "operator",
        [UpReq, UpItem, DownReq, DownItem]
    );
}

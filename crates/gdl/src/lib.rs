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
mod cx;
mod decl;
mod diagram;
mod dist;
mod error;
mod hash;
mod items;
#[cfg(feature = "json-lines")]
mod json;
mod manifest;
mod placement;
mod role;
mod streamed;
mod wire;

pub use compose::{BackwardChain, ForwardChain, Hop, Inner, OperatorStage, SinkStage, Source};
pub use cx::{Cx, Deadline, RequestId};
pub use decl::{
    Absent, BackwardDecl, ForwardDecl, NodeDecl, OperatorDecl, Role, SinkDecl, SourceDecl,
};
pub use dist::{
    BwdEnd, BwdStep, DBackward, DForward, DHop, DInner, DOperator, DSink, Deployment, Entries,
    FwdEnd, FwdStep, Register, Roster, RuntimeConfig, Segment, Served, bind_node,
};
#[cfg(feature = "fault-injection")]
pub use dist::{Faults, Gate};
pub use error::{Error, ErrorKind, Result};
pub use items::{Items, items};
#[cfg(feature = "json-lines")]
pub use json::serve_json_lines;
pub use manifest::{HopInfo, ItemInfo, Manifest, NodeInfo, TypeInfo};
pub use placement::{
    CrossingReport, HopLayout, Layout, Placement, PlacementError, PlacementErrors, Run, WireReason,
    layout,
};
pub use role::{Backward, Build, Forward, Next, Operator, Sink};
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

/// Message types, from `gdl-message`: a protocols crate depends on that
/// crate alone and derives `gdl_message::Message`.
pub use gdl_message::{Catalogue, Message, MessageInfo, Shape};

/// Mark the type that implements a node a crate declares in its nodes
/// file.
///
/// ```ignore
/// // Cargo.toml: [package.metadata.gdl] nodes = "gdl/nodes.gdl"
/// // gdl/nodes.gdl: sink Engine: Words -> stream Token;
/// #[gdl::node]
/// pub struct Engine { .. }
/// impl gdl::Sink for Engine { type Req = Words; type Item = Token; .. }
/// ```
///
/// When the crate compiles, the type must be declared in the nodes file
/// under its own name, be reachable at the crate root under that name, and
/// implement the declared role with the declared message at each port. The
/// generator reads the nodes file without compiling the crate; this check
/// keeps the file true. The expansion names gdl as `::gdl`, so a node crate
/// depends on gdl under that name.
pub use gdl_macros::node;

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    /// Whether port type `T` carries the message names `head` and `item`:
    /// what `#[gdl::node]` asserts for each port, at compile time.
    pub const fn shape_is<T: gdl_message::Shape>(head: &str, item: Option<&str>) -> bool {
        same(T::HEAD, head)
            && match (T::ITEM, item) {
                (None, None) => true,
                (Some(a), Some(b)) => same(a, b),
                _ => false,
            }
    }

    const fn same(a: &str, b: &str) -> bool {
        let (a, b) = (a.as_bytes(), b.as_bytes());
        if a.len() != b.len() {
            return false;
        }
        let mut i = 0;
        while i < a.len() {
            if a[i] != b[i] {
                return false;
            }
            i += 1;
        }
        true
    }

    pub use crate::wire::probe::{Probe, ViaLocal, ViaWire};
    pub use gdl_macros::graph;
}

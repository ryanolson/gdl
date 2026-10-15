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
mod dist;
mod error;
mod hash;
mod items;
mod manifest;
mod placement;
mod role;
mod wire;

pub use compose::{BackwardChain, ForwardChain, Hop, Inner, OperatorStage, SinkStage, Source};
pub use cx::{Cx, Deadline, RequestId};
pub use decl::{
    Absent, BackwardDecl, ForwardDecl, NodeDecl, OperatorDecl, Role, SinkDecl, SourceDecl,
};
#[cfg(feature = "fault-injection")]
pub use dist::Faults;
pub use dist::{
    BwdEnd, BwdStep, DBackward, DForward, DHop, DInner, DOperator, DSink, Deployment, Entries,
    FwdEnd, FwdStep, Register, Roster, RuntimeConfig, Segment, bind_node,
};
pub use error::{Error, ErrorKind, Result};
pub use items::{Items, items};
pub use manifest::{HopInfo, Manifest, NodeInfo, TypeInfo};
pub use placement::{
    CrossingReport, HopLayout, Layout, Placement, PlacementError, PlacementErrors, Run, WireReason,
    layout,
};
pub use role::{Backward, Build, Forward, Next, Operator, Sink};
/// The velo version this runtime is built on: a deployment's nodes are
/// velo nodes, and generated segment crates reach velo through here so they
/// cannot pick a different version.
pub use velo;
pub use wire::Codec;

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
/// - `local(bindings)`: the whole graph as nested calls in one process.
///
/// Bump `schema` whenever a message type's wire form changes.
pub use gdl_macros::graph;

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::wire::probe::{Probe, ViaLocal, ViaWire};
}

//! The gdl runtime: it runs the nodes a crate defines with `gdl` in
//! segments, carries requests and response streams between segments over
//! velo, and brings a deployment up and down.
//!
//! A node crate depends on `gdl` alone. A segment binary, a test that runs a
//! graph, and the generated segment crates depend on this crate, which
//! re-exports the `gdl` it was built with as [`gdl`].
//!
//! The design is described in the book under `docs/src`, starting at `docs/src/introduction.md`.

mod binary;
mod config;
mod decl;
mod dist;
#[cfg(feature = "json-lines")]
mod json;
mod process;
mod wire;

pub use binary::{Builder, Failure, SegmentBinary, Serving, run_segment, segment_main};
pub use config::build_node;
pub use decl::{Absent, BackwardDecl, ForwardDecl, NodeDecl, OperatorDecl, SinkDecl, SourceDecl};
#[doc(hidden)]
pub use dist::EncodedItem;
pub use dist::{
    BwdEnd, BwdStep, DBackward, DForward, DHop, DInner, DOperator, DSink, Decline, Deployment,
    Entries, FwdEnd, FwdStep, Register, Roster, RuntimeConfig, Segment, SegmentLoad, Served,
    bind_node,
};
#[cfg(feature = "fault-injection")]
pub use dist::{Faults, Gate};
#[cfg(feature = "hub")]
pub use dist::{FollowedPool, HubSession, Pools};
pub use figment;
/// The definitions this runtime runs: generated segment crates and the
/// `graph!` expansion name node traits through here, so they cannot pick a
/// different `gdl`. The derives (`Node`, `Message`, `Config`, `Metrics`,
/// `Endpoints`, and `export`) expand to `::gdl::...`, so a crate that uses
/// one depends on `gdl` itself, as node crates do.
pub use gdl;
#[doc(hidden)]
pub use gdl::message_encoding;
pub use gdl_model::{
    CrossingReport, DeploymentSpec, DirectoryBackend, Encoding, GraphConfig, HUB_CONTROL_PORT,
    HUB_PUBLIC_PORT, HopInfo, HopLayout, ItemInfo, Layout, Manifest, NodeInfo, Placement,
    PlacementError, PlacementErrors, PoolSettings, RequestRouter, Role, Run, SELECTORS,
    SegmentRegistration, SelectorInfo, TypeInfo, WireReason, layout, resolve_config,
};
#[cfg(feature = "hub")]
pub use gdl_pools::PoolState;
#[cfg(feature = "json-lines")]
#[doc(hidden)]
pub use json::probe as __json;
#[cfg(feature = "json-lines")]
pub use json::{Json, JsonItem, JsonRequest, reads_json, serve_json_lines, writes_json};
pub use process::{Drains, Hosted, Launch, Process, ProcessConfig};
/// The velo version this runtime is built on: a deployment's nodes are
/// velo nodes, and generated segment crates reach velo through here so they
/// cannot pick a different version.
pub use velo;
pub use wire::{Codec, RequestCodec, StreamCodec};

/// Declare a graph: its nodes, their message types, and its topology.
///
/// ```ignore
/// gdl_runtime::graph! {
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
/// [`Streamed<Head, Item>`](gdl::Streamed). Both ends of a forward edge must agree on
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
/// Bump `schema` whenever a message type's serde representation changes.
/// Declared codec changes update the contract hash automatically.
///
/// The expansion reaches the runtime through `$crate`, so a contract crate
/// can depend on gdl-runtime under another name. Names starting with `__gdl` or `__Gdl`
/// are reserved for the expansion.
#[macro_export]
macro_rules! graph {
    ($($graph:tt)*) => {
        $crate::__private::graph! { $crate; $($graph)* }
    };
}

/// The library of one generated segment crate: its placement, the checks
/// that each message it sends across a cut is a wire type, `start`, and the
/// `Binary` its `main` runs. `gdl-gen` writes the call, from what it
/// computes for the segment; the input is not a stable interface, since
/// the generator and the runtime come from one revision of gdl. A message
/// that is not a wire type fails to build at its crossing's line in the
/// call. See `docs/src/concepts/generation.md`.
#[macro_export]
macro_rules! segment {
    ($($segment:tt)*) => {
        $crate::__private::segment! { $crate; $($segment)* }
    };
}

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::wire::probe::{Probe, ViaLocal, ViaWire};
    pub use gdl_macros::{graph, segment};
}

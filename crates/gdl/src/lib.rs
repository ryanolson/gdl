//! Node definitions for gdl graphs: what a crate needs to implement nodes,
//! messages, and routing selectors, without the runtime that runs them.
//!
//! A graph is a source and a nested chain of hops. Each hop has a forward
//! chain that transforms the request, an inner end (an [`Operator`] or a
//! [`Sink`]), and a backward chain that transforms the response stream. An
//! operator sees both directions and calls the rest of the graph through
//! [`Next`]; forward and backward nodes see one direction each.
//!
//! A graph's nodes also compose here, as nested calls in one process
//! ([`Source`], [`Hop`]), which is how a node crate tests them. The runtime
//! that places nodes in segments and carries requests between them is
//! `gdl-runtime`. A node crate depends on this crate alone, so it
//! does not link the runtime's transport.
//!
//! The design is described in the book under `docs/src`, starting at `docs/src/introduction.md`.

// `derive(gdl::Projection)` names gdl as `::gdl`, which its own tests use.
#[cfg(test)]
extern crate self as gdl;

mod codec;
mod compose;
mod cx;
mod error;
mod followed;
mod gate;
mod identity;
#[doc(hidden)]
mod items;
mod kv;
mod lifecycle;
#[doc(hidden)]
mod meter;
mod metrics;
mod policy;
mod properties;
mod records;
mod role;
mod select;
mod service;
mod streamed;
mod unary;

pub use compose::{
    BackwardChain, BackwardHalf, ForwardChain, ForwardHalf, Hop, Inner, Join, Kept, OperatorStage,
    SinkStage, Source, around,
};
pub use cx::{Cx, Deadline, RequestId};
pub use error::{Error, ErrorKind, Result};
pub use followed::{
    ClassPools, FollowedPools, PropertyIndex, PropertyIndexes, PropertyKinds, PropertyPool,
};
pub use gate::{Gate, OfferGate};
pub use gdl_macros::{Config, Endpoints, Metrics};
pub use gdl_model::{
    ConfigField, ConfigSchema, ConfigValue, Encoding, EntryState, MetricField, MetricKind,
    PropertyEntry, PropertyKey,
};
pub use identity::{IdentityNode, IdentityOperator};
pub use items::{Items, items};
pub use kv::{InstanceKv, OfferView};
pub use lifecycle::{Fence, Handoff, Leave, LeaveCause, Lifecycle, Routing, Unconfirmed};
pub use metrics::{GaugeReadings, Metrics, ScrapeGaugeVec};
pub use policy::{
    ChoiceLease, Damped, Decline, HomeFirst, HomeFirstAt, KvLoad, Load, LoadSummary, NoPool, Pool,
    PoolChoice, PoolCondition, PoolEvidence, PoolPolicies, PoolPolicy, PoolRequest, PoolState,
    PropertyState, RoundRobin,
};
/// The prometheus version node metrics are built with: a node crate names
/// its metric types through here, so they are the types the runtime
/// registers and serves.
pub use prometheus;
pub use properties::{InstanceProperties, PropertyKind, PropertyType};
pub use records::{
    Checked, Expected, HubRecords, PoolRecords, Record, RecordsUnavailable, Written,
};
#[doc(hidden)]
pub use records::{RecordAnswer, RecordLink, RecordOp, RecordReply};
pub use role::{Around, Backward, Build, Forward, Frontend, Next, Operator, Sink, Unary};
#[doc(hidden)]
pub use select::JoinStamps;
pub use select::{
    Affinity, AffinityConfig, Choice, Encoded, Head, Instance, InstanceId, Instances,
    JoinedThrough, LeastLoaded, Projected, Projection, Request, Selector, Selectors,
};
pub use service::{Client, Endpoints, Service};
pub use streamed::Streamed;
pub use unary::UnaryClient;

/// Message types, from `gdl-message`. A protocols crate depends on that
/// crate alone; a node crate can reach the same items here.
pub use gdl_message::{Catalogue, Identity, MessageInfo, Shape};
#[doc(hidden)]
pub use gdl_message::{message_encoding, message_pinned};

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
///
/// On a `pub type` that names an instantiation of a generic message or a
/// generic node, it offers the instantiation under the alias's name:
///
/// ```ignore
/// #[gdl::export]
/// pub type PromptCarried = Carried<Prompt, Plain>;
/// #[gdl::export]
/// pub type PlainRelay = Relay<Plain>;
/// ```
///
/// A node's alias is at the crate root, where generated segments name it,
/// as a node type is.
pub use gdl_macros::gdl_export as export;

/// Mark the type that implements a node.
///
/// ```ignore
/// #[derive(gdl::Node)]
/// pub struct Engine { .. }
/// impl gdl::Sink for Engine { type Req = Words; type Item = Token; .. }
/// ```
///
/// The node's name is the type's name. The type must be `pub` and
/// reachable at the crate root under its name, because generated segments
/// name it there. The derive registers the type with each role trait it
/// implements and the messages at its ports, for
/// `cargo gdl extract --verify`; the source reader finds the same impl in
/// the crate's source. A node implements exactly one role trait.
///
/// A generic type, with type parameters only, registers nothing itself: a
/// graph names one type per node, so each instantiation is a node where an
/// [`export`]ed `pub type` names it, under the alias's name. Its
/// `config` and `metrics` must be every such instantiation's `Build`'s.
pub use gdl_macros::Node;

/// A part of a request's head that a router reads: implements
/// [`Projection`] for a struct of some fields of the edge type named in
/// `#[gdl(of = <type>)]`, and checks at compile time that the edge type
/// has each one, under its name and of its type.
pub use gdl_macros::Projection;

/// What `gdl-runtime` uses of the definitions' internals: the request
/// context's scopes, the bounds on a stream, and the per-segment figures a
/// request's visit records. Not a stable interface.
#[doc(hidden)]
pub mod __runtime {
    pub use crate::codec::{TAIL_ROOM, decoder, encoder, encoder_into, encoder_sized};
    pub use crate::compose::race_done;
    pub use crate::cx::{CancelOnDrop, Scope, wall_now_nanos};
    pub use crate::followed::{PoolSource, PropertyKindsNow};
    pub use crate::items::{
        CHECK_INTERVAL, Fuse, bound_measured, failed, fuse, fused, until_error,
    };
    pub use crate::lifecycle::{LeaveHook, LeaveParts};
    pub use crate::meter::{
        END_TO_END, Ending, Figures, Meters, Owner, REQUEST, RESPONSE, Spans, Visit, ended,
        histogram, note,
    };
    pub use crate::policy::{ChoiceHeld, ErasedPolicy, TypedPolicy, Window, pool_id};
    pub use crate::properties::{Presence, Want};
    pub use crate::select::{Erased, Lease, Picked, Typed};
}

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::metrics::{Construct, MetricsDerived, check_labels, metric};
    pub use crate::select::{ProjectionDerived, SameType, field};
    pub use gdl_message::__private::*;
    pub use gdl_model::__private::*;
    pub use gdl_model::ENDPOINT_LABEL;

    use crate::{Around, Backward, Forward, Frontend, Operator, Sink, Unary};

    /// A message a generated segment sends or receives across a cut: a
    /// wire type. Each segment asserts it of every message that crosses
    /// from or into it, at compile time, so that one the source reader took
    /// for a wire type, and that is not, fails the segment's build with
    /// this message and the crossing's name, rather than deep in the
    /// runtime's codecs or at deployment.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` crosses between segments but is not a wire type",
        label = "this crossing carries `{Self}`",
        note = "a message that crosses between segments must implement `serde::Serialize` and `serde::de::DeserializeOwned`: the source reader took it for one; check its serde impls with `cargo gdl extract --verify`, or place both ends of the crossing in one segment"
    )]
    pub trait Crossing: seal::Wire {}

    // Not recommended, so that a type that fails it is reported as not a
    // `Crossing`, with the message above, once, rather than as each serde
    // trait it lacks, with a list of the types that have them.
    #[diagnostic::do_not_recommend]
    impl<T: serde::Serialize + serde::de::DeserializeOwned> Crossing for T {}

    /// Compiles only for a [`Crossing`]: what a segment's assertion calls.
    pub const fn crosses<T: Crossing>() {}

    /// A hop's response that returns to its outer end through an anchor,
    /// though both ends are in one segment: the hop is not local, since
    /// its forward path leaves the segment and comes back, so the inner
    /// end's output is sent to the outer end's anchor, and must be a wire
    /// type. Asserted as [`Crossing`] is, with advice that fits the case.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` returns through an anchor but is not a wire type",
        label = "this hop's response carries `{Self}`",
        note = "the hop's ends share this segment, but its forward path leaves the segment and comes back, so its response returns through an anchor, which needs `serde::Serialize` and `serde::de::DeserializeOwned`: the source reader took it for one; check its serde impls with `cargo gdl extract --verify`, or place the whole hop in one segment"
    )]
    pub trait Anchored: seal::Wire {}

    // Not recommended, as for `Crossing`.
    #[diagnostic::do_not_recommend]
    impl<T: serde::Serialize + serde::de::DeserializeOwned> Anchored for T {}

    /// A unary node's request or response on a call from one segment to
    /// another: the call encodes both, so each must be a wire type.
    /// Asserted as [`Crossing`] is, in the caller's segment and in the
    /// service's, with advice that fits a call.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is carried by a call to another segment but is not a wire type",
        label = "this call carries `{Self}`",
        note = "a call to a unary node in another segment encodes its request and its response, which need `serde::Serialize` and `serde::de::DeserializeOwned`: the source reader took it for one; check its serde impls with `cargo gdl extract --verify`, or list the unary node in the caller's segment"
    )]
    pub trait Called: seal::Wire {}

    // Not recommended, as for `Crossing`.
    #[diagnostic::do_not_recommend]
    impl<T: serde::Serialize + serde::de::DeserializeOwned> Called for T {}

    /// Compiles only for a [`Called`]: what a segment's assertion calls.
    pub const fn calls<T: Called>() {}

    /// The seal on [`Crossing`], [`Anchored`] and [`Called`]: a module that no other
    /// crate can name, so that only their blanket impls implement them. A
    /// hand impl for a type that is not a wire type would pass the check
    /// that a segment's build makes, and fail at run time instead.
    mod seal {
        #[diagnostic::on_unimplemented(
            message = "`{Self}` implements a crossing check by hand",
            note = "`Crossing` and `Anchored` hold for each type that implements `serde::Serialize` and `serde::de::DeserializeOwned`, and for no other: implement those instead"
        )]
        pub trait Wire {}

        // Not recommended, as for `Crossing`: one error per hand impl.
        #[diagnostic::do_not_recommend]
        impl<T: serde::Serialize + serde::de::DeserializeOwned> Wire for T {}
    }

    /// Compiles only for an [`Anchored`]: what a segment's assertion calls.
    pub const fn anchors<T: Anchored>() {}

    /// A streamed request's head and item, which cross apart: a segment
    /// names each through this, from the port type its contract declares.
    pub trait Parts {
        /// The head.
        type Head;
        /// The item.
        type Item;
    }

    impl<H, T> Parts for crate::Streamed<H, T> {
        type Head = H;
        type Item = T;
    }

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

    /// The metrics a node's build registers: `Build::Metrics`'s
    /// definitions when the type implements `Build`, and none otherwise,
    /// probed as the roles are. Registered whether or not the derive names
    /// a metrics type, so that `extract --verify` sees a build register
    /// metrics the source reader cannot.
    pub trait MetricsYes {
        fn __gdl_metrics(&self) -> Vec<gdl_model::MetricField>;
    }
    impl<T: crate::Build> MetricsYes for RoleProbe<T> {
        fn __gdl_metrics(&self) -> Vec<gdl_model::MetricField> {
            <T::Metrics as crate::Metrics>::definitions()
        }
    }
    pub trait MetricsNo {
        fn __gdl_metrics(&self) -> Vec<gdl_model::MetricField>;
    }
    impl<T> MetricsNo for &RoleProbe<T> {
        fn __gdl_metrics(&self) -> Vec<gdl_model::MetricField> {
            Vec::new()
        }
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
        AroundYes,
        AroundNo,
        __gdl_around,
        Around,
        "around",
        [UpReq, UpItem, DownReq, DownItem]
    );
    probe!(
        OperatorYes,
        OperatorNo,
        __gdl_operator,
        Operator,
        "operator",
        [UpReq, UpItem, DownReq, DownItem]
    );
    probe!(UnaryYes, UnaryNo, __gdl_unary, Unary, "unary", [Req, Resp]);
}

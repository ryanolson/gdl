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
//! The design and its rulings are in `agent-docs/plan.md`.

mod compose;
mod cx;
mod error;
mod identity;
#[doc(hidden)]
mod items;
#[doc(hidden)]
mod meter;
mod metrics;
mod role;
mod select;
mod service;
mod streamed;

pub use compose::{
    BackwardChain, ForwardChain, Hop, Inner, Join, OperatorStage, SinkStage, Source,
};
pub use cx::{Cx, Deadline, RequestId};
pub use error::{Error, ErrorKind, Result};
pub use gdl_macros::{Config, Endpoints, Metrics};
pub use gdl_model::{ConfigField, ConfigSchema, ConfigValue, Encoding, MetricField, MetricKind};
pub use identity::{IdentityNode, IdentityOperator};
pub use items::{Items, items};
pub use metrics::Metrics;
/// The prometheus version node metrics are built with: a node crate names
/// its metric types through here, so they are the types the runtime
/// registers and serves.
pub use prometheus;
pub use role::{Backward, Build, Forward, Frontend, Next, Operator, Sink};
pub use select::{
    Affinity, AffinityConfig, Choice, Encoded, Head, Instance, InstanceId, Instances, LeastLoaded,
    Request, Selector, Selectors,
};
pub use service::{Client, Endpoints, Service};
pub use streamed::Streamed;

#[doc(hidden)]
pub use gdl_message::message_encoding;
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

/// What `gdl-runtime` uses of the definitions' internals: the request
/// context's scopes, the bounds on a stream, and the per-segment figures a
/// request's visit records. Not a stable interface.
#[doc(hidden)]
pub mod __runtime {
    pub use crate::cx::wall_now_nanos;
    pub use crate::items::{CHECK_INTERVAL, bound_measured, failed, fuse, until_error};
    pub use crate::meter::{
        END_TO_END, Ending, Figures, Meters, Owner, REQUEST, RESPONSE, Visit, ended, note,
    };
    pub use crate::select::{Erased, Lease, Picked, Typed};
}

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::metrics::{Construct, MetricsDerived, metric};
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
        OperatorYes,
        OperatorNo,
        __gdl_operator,
        Operator,
        "operator",
        [UpReq, UpItem, DownReq, DownItem]
    );
}

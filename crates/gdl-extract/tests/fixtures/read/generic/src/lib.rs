//! A generic carrier and a generic node, each written once, and the
//! instantiations this crate names. The generic types themselves are not
//! offered: a graph names one type per message and per node.

use read_protocols::Words;
use serde::{Deserialize, Serialize};

mod carrier;

pub use carrier::Carried;

/// A builder with serde impls.
#[derive(Serialize, Deserialize, Default)]
pub struct Plain;

/// A builder without them.
#[derive(Default)]
pub struct Opaque;

/// A carrier whose builder serde skips: an instantiation is a wire type
/// whatever its builder is.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Tagged<P, B> {
    /// What it carries.
    pub payload: P,
    /// Skipped.
    #[serde(skip)]
    pub builder: B,
}

/// An instantiation whose arguments are wire types.
#[gdl::export]
pub type PlainCarried = Carried<Words, Plain>;

/// One with an argument that is not.
#[gdl::export]
pub type OpaqueCarried = carrier::Carried<read_protocols::Words, Opaque>;

/// A skipped argument does not make it local.
#[gdl::export]
pub type OpaqueTagged = Tagged<Words, Opaque>;

/// An alias without the marker names a type, and offers nothing.
pub type Unmarked = Carried<Words, Plain>;

/// An instantiation exported by `pub use` of an alias that does not offer it.
#[gdl::export]
pub use Unmarked as Reused;

/// The settings of every relay.
#[derive(Deserialize, gdl::Config)]
pub struct RelaySettings {
    /// Whether it relays.
    #[gdl(graph, default = true)]
    pub enabled: bool,
}

/// What every relay reports.
#[derive(gdl::Metrics)]
pub struct RelayMetrics {
    /// Requests relayed.
    relayed_total: gdl::prometheus::IntCounter,
}

/// A generic forward node.
#[derive(gdl::Node)]
#[gdl(config = RelaySettings, metrics = RelayMetrics)]
pub struct Relay<B>(std::marker::PhantomData<B>);

impl<Builder: Send + Sync + 'static> gdl::Forward for Relay<Builder> {
    type In = Carried<Words, Builder>;
    type Out = carrier::Carried<Words, Builder>;
}

/// A generic sink with an impl for one instantiation alone.
#[derive(gdl::Node)]
pub struct Store<B>(std::marker::PhantomData<B>);

impl gdl::Sink for Store<Plain> {
    type Req = PlainCarried;
    type Item = Words;
}

/// An instantiation of the node.
#[gdl::export]
pub type PlainRelay = Relay<Plain>;

/// Another, of the same implementation.
#[gdl::export]
pub type OpaqueRelay = Relay<Opaque>;

/// The instantiation the impl is for.
#[gdl::export]
pub type PlainStore = Store<Plain>;

/// One no impl is for.
#[gdl::export]
pub type OpaqueStore = Store<Opaque>;

/// A concrete node whose port names an instantiation by its alias.
#[derive(gdl::Node)]
pub struct Unwrap;

impl gdl::Forward for Unwrap {
    type In = PlainCarried;
    type Out = Words;
}

/// A node alias that is not at the crate root.
pub mod nested {
    /// Not reachable as `read_generic::Hidden`.
    #[gdl::export]
    pub type Hidden = super::Relay<super::Plain>;
}

/// An alias of an argument the reader cannot name: left out, alone.
#[gdl::export]
pub type Shared = Carried<std::sync::Arc<Words>, Plain>;

/// The relay's settings under another name.
pub type Settings = RelaySettings;

/// A node whose derive names its settings through an alias: a problem
/// that names the alias.
#[derive(gdl::Node)]
#[gdl(config = Settings)]
pub struct Renamed;

impl gdl::Forward for Renamed {
    type In = PlainCarried;
    type Out = Words;
}

/// A node whose port carries the carrier with a tuple argument: a problem
/// that names the tuple.
#[derive(gdl::Node)]
pub struct Paired;

impl gdl::Forward for Paired {
    type In = Carried<(Words, Words), Plain>;
    type Out = Words;
}

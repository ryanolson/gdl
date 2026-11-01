//! The list of a pool hub's members, for a controller outside the pool
//! that chooses which instance to remove: each one's segment, address,
//! phase, and, for an entry instance, its latest load report.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use velo::PeerInfo;

/// The instance list's route, under the extension's, on the hub's control
/// listener: `GET`, with no credential, answers [`Instances`]. It reads
/// what the hub already holds, at the time of the request: its members,
/// the phases they announced, and the entry instances' load reports.
/// Answered `503 Service Unavailable` before the hub has started, or when
/// the hub's own service directory does not answer within 5 s.
pub const INSTANCES_ROUTE: &str = "/instances";

/// The body of [`INSTANCES_ROUTE`]'s answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instances {
    /// Every member of the hub, in no order. The list is a snapshot: a
    /// registration that ends while it is made is left out, even when its
    /// instance's id registers again meanwhile.
    pub instances: Vec<PoolInstance>,
}

/// One member of a pool's hub.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolInstance {
    /// Its segment.
    pub segment: String,
    /// Its velo peer: its instance id and its worker address, encoded as
    /// velo encodes it, as `/v1/ext/gdl/members` and the pool's watch
    /// carry it.
    pub peer: PeerInfo,
    /// The socket addresses its velo TCP transports advertise, each once,
    /// read from every entry of the peer's worker address, whatever its
    /// key: under Kubernetes, its pod's IP, on the port velo listens on.
    /// An entry that is not a TCP endpoint is skipped; empty when none is.
    #[serde(default)]
    pub addresses: Vec<SocketAddr>,
    /// The furthest phase it has announced.
    pub phase: InstancePhase,
    /// The latest load report of an entry instance that is usable: ready,
    /// not retired, and reporting in its current registration within three
    /// summary periods. Shown only when [`PoolInstance::phase`] is
    /// `ready`. `None` for an interior instance, and for an entry instance
    /// that has not reported, or is not usable.
    #[serde(default)]
    pub load: Option<InstanceLoad>,
}

/// The furthest phase a member has announced to its hub, as a service
/// ([`gdl_model::SegmentPhase`]). The order below is the order in which
/// an instance takes them, and the hub reads them in it, so that for one
/// registration a listed phase never goes backwards while the instance's
/// own announcements stand. An instance whose retirement announcement
/// went unanswered withdraws it, and is listed `ready` again, as the hub
/// holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstancePhase {
    /// Registered, and not yet ready: it may be loading its nodes.
    Joining,
    /// It routes, and takes work.
    Ready,
    /// An instance of a draining pool: it takes no more work from outside
    /// the pool, and sends no more into it.
    Detached,
    /// It has been stopped, and drains what it holds.
    Retired,
    /// A source instance that has finished its requests.
    Drained,
}

/// An entry instance's latest load report, as its hub holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceLoad {
    /// The roots it held admitted.
    pub admitted: u64,
    /// Its admission limit.
    pub limit: u64,
    /// The hand-offs it declined, in the reports no load summary has
    /// counted yet, or in its latest report once one has.
    pub refusals: u64,
    /// How long ago the hub took the report, in milliseconds.
    pub age_ms: u64,
    /// What its nodes said of its KV cache ([`gdl_pools::KvLoad`]): none
    /// from an instance whose nodes set nothing, or from a hub of a build
    /// from before the numbers.
    #[serde(flatten)]
    pub kv: gdl_pools::KvLoad,
}

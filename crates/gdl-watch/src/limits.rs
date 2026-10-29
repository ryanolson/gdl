//! What a watch may cost its reader, and why a watch that began ended
//! other than cleanly.

use std::time::Duration;

/// What one watch may cost its reader before the client ends it, so that
/// a peer that misbehaves, by a defect or because anything can answer on
/// a hub's address, cannot hold a reader's memory without bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchLimits {
    /// The most bytes of one event, from its first byte to the end of its
    /// blank line, counted across the chunks it arrives in.
    pub event: usize,
    /// The most bytes of a watch's snapshot, from its first byte to the
    /// end of its `synced`.
    pub snapshot: usize,
    /// How long from when a watch is asked for its snapshot may take to
    /// complete, its `synced` read, whatever bytes come meanwhile. An
    /// event is timed as it is read: one that came in time but is read
    /// after the bound, from the reader's buffer, is late.
    pub synced_within: Duration,
}

impl WatchLimits {
    /// The default limit on one event: 4 MiB.
    ///
    /// The largest event a pool's hub sends is an instance's: `ready` on a
    /// pool's watch, `exposed` on the relay. Each carries the instance's
    /// peer as it registered with the hub, whose worker address is bytes
    /// the hub does not bound, written in JSON as an array of numbers. The
    /// hub takes a registration as a JSON body, with that same encoding of
    /// the peer, of at most 2 MiB (axum's default body limit, which
    /// velo-hub keeps), so no instance a hub admits has an event over 2
    /// MiB and the few names around the peer. A velo node with one TCP
    /// transport has a `ready` event of 995 bytes. Every other event names
    /// only pools, classes, regions, and segments, each at most 63 or 64
    /// characters, and numbers: with every name at its longest and every
    /// number at its largest, a snapshot's `follow` event is 463 bytes, its
    /// `pool` event 381, and a `load` event 182 on a pool's watch and 364
    /// on the relay. So 4 MiB refuses nothing a hub sends, with room for a
    /// larger address, and holds a watch's buffer to 4 MiB and one chunk.
    pub const EVENT: usize = 4 << 20;

    /// The default limit on a snapshot's bytes: 64 MiB.
    ///
    /// A snapshot holds an event for each instance it shows, and a few for
    /// each pool: a pool's watch its entry instances, the relay every
    /// entry instance of every pool its hub follows. An instance's event
    /// is about 1 KiB with one TCP transport: a real hub's snapshot of a
    /// pool of 64 entry instances is 64,468 bytes, and its follower's
    /// relay of it 68,644. So 64 MiB holds a snapshot of some 60,000
    /// instances, more entry instances than one hub follows, and bounds
    /// what a reader holds of a snapshot that is not complete.
    pub const SNAPSHOT: usize = 64 << 20;

    /// The default bound on a snapshot's time: 30 s from when the watch
    /// is asked for.
    ///
    /// It covers the connection (at most 5 s), a pool's hub's wait for its
    /// first record before it answers a watch (at most 5 s), and the
    /// snapshot itself, which a hub sends from what it holds in memory: a
    /// real hub's snapshot of a pool of 64 entry instances completes in 8
    /// ms over loopback, and its follower's relay of it in 14 ms, and the
    /// 20 s left carry a snapshot at the limit at 3.2 MB/s. It is also the idle timeout, so a hub that
    /// stalls is told the same way before and after its snapshot. A watch
    /// whose snapshot is complete is never ended by it.
    pub const SYNCED_WITHIN: Duration = Duration::from_secs(30);
}

impl Default for WatchLimits {
    fn default() -> Self {
        Self {
            event: Self::EVENT,
            snapshot: Self::SNAPSHOT,
            synced_within: Self::SYNCED_WITHIN,
        }
    }
}

/// Why a watch that began ended other than cleanly: the item a watch
/// ends with when it does not just end. A watch that ends cleanly, as
/// when its hub ends it, yields no more items, and this is never yielded
/// but last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchFailure {
    /// The connection failed, or went idle for longer than three of the
    /// hub's keep-alives.
    Connection(String),
    /// An event that is not JSON, is of a known kind whose fields do not
    /// parse, or, on the relay, is of a kind this build does not know and
    /// names no follow.
    Unparsed,
    /// An event longer than the limit ([`WatchLimits::event`]): its bytes
    /// are not read past it.
    EventTooLarge {
        /// The limit, in bytes.
        limit: usize,
    },
    /// A snapshot longer than the limit ([`WatchLimits::snapshot`]).
    SnapshotTooLarge {
        /// The limit, in bytes.
        limit: usize,
    },
    /// A snapshot not complete within the bound
    /// ([`WatchLimits::synced_within`]) of when the watch was asked for.
    SnapshotLate {
        /// The bound.
        within: Duration,
    },
}

impl std::fmt::Display for WatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection(error) => write!(f, "the watch's connection failed: {error}"),
            Self::Unparsed => f.write_str("the watch sent an event that does not parse"),
            Self::EventTooLarge { limit } => {
                write!(f, "the watch sent an event longer than {limit} bytes")
            }
            Self::SnapshotTooLarge { limit } => {
                write!(f, "the watch sent a snapshot longer than {limit} bytes")
            }
            Self::SnapshotLate { within } => write!(
                f,
                "the watch's snapshot was not complete within {} ms",
                within.as_millis()
            ),
        }
    }
}

impl std::error::Error for WatchFailure {}

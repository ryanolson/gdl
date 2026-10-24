//! What a watch may cost its reader, and why a watch that began ended
//! other than cleanly.

/// What one watch may cost its reader before the client ends it, so that
/// a peer that misbehaves, by a defect or because anything can answer on
/// a hub's address, cannot hold a reader's memory without bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchLimits {
    /// The most bytes of one event, from its first byte to the end of its
    /// blank line, counted across the chunks it arrives in.
    pub event: usize,
}

impl WatchLimits {
    /// The default limit on one event: 4 MiB.
    ///
    /// The largest event a cell's hub sends is an instance's: `ready` on a
    /// cell's watch, `exposed` on the relay. Each carries the instance's
    /// peer as it registered with the hub, whose worker address is bytes
    /// the hub does not bound, written in JSON as an array of numbers. The
    /// hub takes a registration as a JSON body, with that same encoding of
    /// the peer, of at most 2 MiB (axum's default body limit, which
    /// velo-hub keeps), so no instance a hub admits has an event over 2
    /// MiB and the few names around the peer. A velo node with one TCP
    /// transport has a `ready` event of 995 bytes. Every other event names
    /// only cells, classes, regions, and segments, each at most 63 or 64
    /// characters, and numbers: with every name at its longest and every
    /// number at its largest, a snapshot's `follow` event is 463 bytes, its
    /// `cell` event 381, and a `load` event 182 on a cell's watch and 364
    /// on the relay. So 4 MiB refuses nothing a hub sends, with room for a
    /// larger address, and holds a watch's buffer to 4 MiB and one chunk.
    pub const EVENT: usize = 4 << 20;
}

impl Default for WatchLimits {
    fn default() -> Self {
        Self { event: Self::EVENT }
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
}

impl std::fmt::Display for WatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection(error) => write!(f, "the watch's connection failed: {error}"),
            Self::Unparsed => f.write_str("the watch sent an event that does not parse"),
            Self::EventTooLarge { limit } => {
                write!(f, "the watch sent an event longer than {limit} bytes")
            }
        }
    }
}

impl std::error::Error for WatchFailure {}

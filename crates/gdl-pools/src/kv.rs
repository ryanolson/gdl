//! What a pool's entry instances say of their KV cache.

use serde::{Deserialize, Serialize};

/// What an entry instance, or a pool, says of its KV cache: five numbers,
/// each of which it may leave out. KV sizes are in tokens. Node code sets
/// them (gdl's `InstanceKv`); gdl carries and sums them, and checks none.
///
/// Each number is held as a plain word, with `u64::MAX` for none, so that
/// a pool row of a policy that never reads them costs 40 bytes, not 80;
/// each is read as an option. On the wire each is a field of its own,
/// left out when none, so a reader of an earlier build ignores them and a
/// reader of this build reads an earlier report as having none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "KvWire", into = "KvWire")]
pub struct KvLoad {
    capacity: u64,
    active: u64,
    committed: u64,
    reserved: u64,
    eviction_age_ms: u64,
}

/// How a number is held when it is none.
const NONE: u64 = u64::MAX;

fn some(value: u64) -> Option<u64> {
    (value != NONE).then_some(value)
}

impl KvLoad {
    /// No number.
    pub const NONE: Self = Self {
        capacity: NONE,
        active: NONE,
        committed: NONE,
        reserved: NONE,
        eviction_age_ms: NONE,
    };

    /// Usable KV, in tokens.
    #[inline]
    pub fn capacity(&self) -> Option<u64> {
        some(self.capacity)
    }

    /// KV of the requests in flight, in tokens.
    #[inline]
    pub fn active(&self) -> Option<u64> {
        some(self.active)
    }

    /// KV of the sessions active within the pool's cache target, resident
    /// or evicted, in tokens.
    #[inline]
    pub fn committed(&self) -> Option<u64> {
        some(self.committed)
    }

    /// The expected growth of the live sessions' KV, in tokens.
    #[inline]
    pub fn reserved(&self) -> Option<u64> {
        some(self.reserved)
    }

    /// How old KV is when it is evicted, in milliseconds: of a pool, the
    /// least over its instances. None from an instance that evicted
    /// nothing in its window.
    #[inline]
    pub fn eviction_age_ms(&self) -> Option<u64> {
        some(self.eviction_age_ms)
    }

    /// These numbers with `capacity`. `u64::MAX` is none.
    #[inline]
    pub const fn with_capacity(mut self, capacity: u64) -> Self {
        self.capacity = capacity;
        self
    }

    /// These numbers with `active`. `u64::MAX` is none.
    #[inline]
    pub const fn with_active(mut self, active: u64) -> Self {
        self.active = active;
        self
    }

    /// These numbers with `committed`. `u64::MAX` is none.
    #[inline]
    pub const fn with_committed(mut self, committed: u64) -> Self {
        self.committed = committed;
        self
    }

    /// These numbers with `reserved`. `u64::MAX` is none.
    #[inline]
    pub const fn with_reserved(mut self, reserved: u64) -> Self {
        self.reserved = reserved;
        self
    }

    /// These numbers with `eviction_age_ms`. `u64::MAX` is none.
    #[inline]
    pub const fn with_eviction_age_ms(mut self, eviction_age_ms: u64) -> Self {
        self.eviction_age_ms = eviction_age_ms;
        self
    }

    /// Whether no number is set.
    #[inline]
    pub fn is_none(&self) -> bool {
        *self == Self::NONE
    }

    /// The numbers of a pool's instances as the pool's. A report with no
    /// number is left out: it is of a build from before the numbers, or
    /// its nodes set none. Over the rest, each size is the sum, short of
    /// `u64::MAX`, only when every one of them reports it, and none
    /// otherwise, so that any two sizes cover the same instances and their
    /// ratio means something. The eviction age is the least of those
    /// reported, since an instance that evicted nothing reports none.
    pub fn summed<'a>(loads: impl IntoIterator<Item = &'a KvLoad>) -> Self {
        let sum = |total: u64, value: u64| match (total, value) {
            // Unknown for one reporting instance, so unknown for all.
            (_, NONE) | (NONE, _) => NONE,
            (total, value) => total.saturating_add(value).min(NONE - 1),
        };
        let mut summed: Option<Self> = None;
        for load in loads.into_iter().filter(|load| !load.is_none()) {
            summed = Some(match summed {
                None => *load,
                Some(total) => Self {
                    capacity: sum(total.capacity, load.capacity),
                    active: sum(total.active, load.active),
                    committed: sum(total.committed, load.committed),
                    reserved: sum(total.reserved, load.reserved),
                    // None is the largest word, so the least is reported.
                    eviction_age_ms: total.eviction_age_ms.min(load.eviction_age_ms),
                },
            });
        }
        summed.unwrap_or(Self::NONE)
    }
}

impl Default for KvLoad {
    fn default() -> Self {
        Self::NONE
    }
}

/// [`KvLoad`] as it travels: each number its own field, left out when
/// none, under the names the pools publish.
#[derive(Serialize, Deserialize)]
struct KvWire {
    #[serde(
        default,
        rename = "kv_capacity",
        skip_serializing_if = "Option::is_none"
    )]
    capacity: Option<u64>,
    #[serde(default, rename = "kv_active", skip_serializing_if = "Option::is_none")]
    active: Option<u64>,
    #[serde(
        default,
        rename = "kv_committed",
        skip_serializing_if = "Option::is_none"
    )]
    committed: Option<u64>,
    #[serde(
        default,
        rename = "kv_reserved",
        skip_serializing_if = "Option::is_none"
    )]
    reserved: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    eviction_age_ms: Option<u64>,
}

impl From<KvWire> for KvLoad {
    fn from(wire: KvWire) -> Self {
        Self {
            capacity: wire.capacity.unwrap_or(NONE),
            active: wire.active.unwrap_or(NONE),
            committed: wire.committed.unwrap_or(NONE),
            reserved: wire.reserved.unwrap_or(NONE),
            eviction_age_ms: wire.eviction_age_ms.unwrap_or(NONE),
        }
    }
}

impl From<KvLoad> for KvWire {
    fn from(kv: KvLoad) -> Self {
        Self {
            capacity: kv.capacity(),
            active: kv.active(),
            committed: kv.committed(),
            reserved: kv.reserved(),
            eviction_age_ms: kv.eviction_age_ms(),
        }
    }
}

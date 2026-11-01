//! What a node in an entry segment says of its instance's KV cache, for the
//! runtime to report with the instance's load to its pool's hub, which sums
//! it into the pool's load summary that every chooser's pool policy reads
//! ([`crate::Load`]). And the view a chooser put on an offer, which the
//! entry's node reads from the request's context.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use gdl_pools::KvLoad;

tokio::task_local! {
    /// The handle of the segment whose nodes are being built.
    static BUILDING: InstanceKv;
}

/// A handle by which a node says what its instance's KV cache holds
/// ([`KvLoad`]). Cheap to clone; every clone sets the same numbers.
///
/// A node takes it once, in [`Build::build`](crate::Build::build), with
/// [`InstanceKv::current`], and calls [`InstanceKv::set`] whenever its
/// numbers change. The runtime reads them once a load summary period and
/// reports them for an entry instance in a pool, and for no other. Setting
/// them is five relaxed stores, with no lock and no allocation, so a node
/// may set them on every request. A report reads the five one by one, so
/// it can pair numbers of two sets made at once with its read.
#[derive(Clone)]
pub struct InstanceKv {
    /// The five numbers, as [`KvLoad`] holds them: `u64::MAX` for none.
    numbers: Arc<[AtomicU64; 5]>,
}

impl InstanceKv {
    /// A handle whose numbers nothing reads until the runtime takes it.
    pub fn new() -> Self {
        Self {
            numbers: Arc::new(std::array::from_fn(|_| AtomicU64::new(u64::MAX))),
        }
    }

    /// The handle of the segment whose nodes the runtime is building: call
    /// it in [`Build::build`](crate::Build::build). Outside a build, a
    /// handle whose numbers nothing reads.
    pub fn current() -> Self {
        BUILDING.try_with(Self::clone).unwrap_or_default()
    }

    /// Say that the instance's KV cache holds `kv`: it replaces the numbers
    /// set before, so a number it leaves out is none.
    pub fn set(&self, kv: KvLoad) {
        let numbers = [
            kv.capacity(),
            kv.active(),
            kv.committed(),
            kv.reserved(),
            kv.eviction_age_ms(),
        ];
        for (held, number) in self.numbers.iter().zip(numbers) {
            held.store(number.unwrap_or(u64::MAX), Ordering::Relaxed);
        }
    }

    /// The numbers last set: none of them before a node sets any.
    pub fn get(&self) -> KvLoad {
        let [capacity, active, committed, reserved, eviction_age_ms] =
            std::array::from_fn(|at| self.numbers[at].load(Ordering::Relaxed));
        KvLoad::NONE
            .with_capacity(capacity)
            .with_active(active)
            .with_committed(committed)
            .with_reserved(reserved)
            .with_eviction_age_ms(eviction_age_ms)
    }

    /// Run `build` with this handle as the one [`InstanceKv::current`]
    /// gives.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

impl Default for InstanceKv {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InstanceKv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("InstanceKv").field(&self.get()).finish()
    }
}

/// The view of a pool a chooser decided on, which it put on its offer
/// because its pool policy asked ([`PoolChoice::with_view`]): the epoch of
/// the pool's hub that the chosen row was a follow of, and the number of
/// the load summary the row held ([`LoadSummary::sequence`]). The pair,
/// compared in that order, never goes back, also across a restart of the
/// pool's hub. The entry's node reads it from the request's context,
/// `cx.ext::<OfferView>()`, and finds none on an offer that carries none.
///
/// [`PoolChoice::with_view`]: crate::PoolChoice::with_view
/// [`LoadSummary::sequence`]: crate::LoadSummary::sequence
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OfferView {
    /// The epoch of the pool's hub.
    pub epoch: u64,
    /// The number of the load summary.
    pub sequence: u64,
    /// The fullest fill of the class's pools in the chooser's table the
    /// choice was made from ([`KvLoad::fill`]), in millionths: the largest
    /// over the pools that report capacity above 0 and committed KV. `None`
    /// when no pool does, or from a chooser of a build before it. A pool
    /// that budgets by it ([`KvLoad::budget`]) takes `None` as its own
    /// fill, never as no bound and never as no room.
    pub fill_max_ppm: Option<u32>,
}

impl OfferView {
    /// [`fill_max_ppm`](OfferView::fill_max_ppm) as a share.
    pub fn fill_max(&self) -> Option<f64> {
        self.fill_max_ppm.map(|ppm| f64::from(ppm) / 1_000_000.0)
    }
}

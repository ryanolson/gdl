/// Why an entry declined an offer: what the chooser that sent it from
/// another class does next, and what a pool policy reads of the pools that
/// declined a request ([`PoolRequest::declined`](super::PoolRequest::declined)).
/// The runtime sends it after the refusal in the entry's reply, so that a
/// build that does not know a reason reads the refusal as
/// [`Decline::Full`]. A segment declines every offer by its own policy
/// with `Segment::decline_offers_as`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Decline {
    /// The pool has no room now, or this instance retired: the chooser
    /// damps the pool, and offers it requests again after the window.
    #[default]
    Full,
    /// The pool takes no work from other pools until its state changes,
    /// as a draining pool does: the chooser passes it over until its
    /// follow changes.
    Gone,
    /// This instance cannot serve the request, and another entry instance
    /// of its pool may, as when the pool's instances serve different
    /// properties. The chooser offers the request to each other entry
    /// instance of the pool once, and to another pool only when every one
    /// declined. It damps nothing, since the decline says nothing of the
    /// pool's room.
    Instance,
    /// The entry does not have the property the request wants: the
    /// chooser chose the pool from a view of its properties that is out of
    /// date. The chooser offers the request to another pool, not to this
    /// pool's other entry instances, and damps nothing, so that the pool
    /// still takes the requests it serves.
    NotPresent,
    /// The pool's own gate declined a first turn against the budget it
    /// holds for the view the offer carries ([`crate::OfferGate`]): it has
    /// admitted enough since that load summary, or the view is too old.
    /// The pool's policy is asked whether to keep the request with it
    /// ([`crate::PoolPolicy::requeue`]), as for a full pool. Otherwise the
    /// request goes to another pool, and the chooser damps nothing: the
    /// pool's next summary is a new budget. A chooser of a build before
    /// this reason reads it as [`Decline::Full`], and damps the pool for
    /// its first window, doubling after.
    Budget,
}

//! Every forward hand-off a router sends is an offer: the entry it reaches
//! may decline it, and then the router chooses again inside the same call.
//! An entry declines when it answers, before it has admitted anything,
//! that it has no room or that the hand-off is not of its deployment; a
//! hand-off never left when velo refused to send it. Only those move a
//! request on. Anything else after the send may have run the request
//! there, so it is the request's own error, and is never offered again.
//!
//! The first offer costs what a hand-off did before hand-offs were offers:
//! what a failover keeps (the instances that declined, the last miss) is
//! made only once an offer missed, in a call boxed on that path alone
//! ([`Runtime::offer_again`]). A chooser's hand-off into another class
//! fails over between cells instead (`cells::offer`), with the same
//! reading of an answer.
//!
//! This module also says which of a segment's hops bring it offers from
//! another class, which a draining cell's entry instance declines.

use futures::future::BoxFuture;
use gdl::{Cx, Error, ErrorKind, Result};
use gdl_model::ClassBoundary;
use velo::InstanceId;

use super::envelope::{self, Accepted, Forward, WireCx};
use super::fabric::Offered;
use super::roster::{Picker, Unchosen, selector_id};
use super::runtime::{Handed, Held, Runtime, Within};

/// What an offer's answer means.
pub(crate) enum Answer {
    /// The entry took the request.
    Accepted(Accepted),
    /// The entry did not take it: it may be offered again.
    Missed(Missed),
    /// The request's error: it is not offered again.
    Failed(Error),
}

/// Why an entry did not take a request, with its error.
pub(crate) enum Missed {
    /// The entry declined it.
    Declined(Error),
    /// The hand-off never left this instance: no entry said anything.
    Unsent(Error),
}

impl Missed {
    /// What a request that `self` was the last word on fails with, now
    /// that `then` came of its next offer: the last refusal, unless no
    /// entry has refused it yet. A hand-off that never left is not an
    /// entry's answer, so it never replaces one.
    pub(crate) fn then(self, then: Missed) -> Missed {
        match (self, then) {
            (Missed::Declined(refusal), Missed::Unsent(_)) => Missed::Declined(refusal),
            (_, then) => then,
        }
    }

    pub(crate) fn into_error(self) -> Error {
        match self {
            Missed::Declined(error) | Missed::Unsent(error) => error,
        }
    }
}

/// Whether the entry's answer `error` declines the offer: no room
/// (admission or egress refused, or the instance retired), or the
/// hand-off is not of the entry's deployment. The entry gives either
/// only before it has admitted anything for the request: every refusal in
/// its forward entry returns before the request's task is spawned, and
/// after it only acceptance is answered. Any other answer (the request's
/// deadline, a codec error) is the request's own.
pub(crate) fn declines(error: &Error) -> bool {
    matches!(error.kind(), ErrorKind::Overloaded | ErrorKind::Placement)
}

/// What an entry's answer means.
pub(crate) fn answered(answer: Result<Accepted>) -> Answer {
    match answer {
        Ok(accepted) => Answer::Accepted(accepted),
        Err(error) if declines(&error) => Answer::Missed(Missed::Declined(error)),
        Err(error) => Answer::Failed(error),
    }
}

/// What became of an offer velo carried, into the class `class` when it
/// crossed into another. An unknown outcome is the request's error, of the
/// kind `OutcomeUnknown`, never `Transport`: the hand-off may have been
/// taken and run, so a caller that tried it again could run it twice.
pub(crate) fn delivered(offered: Offered, class: Option<&str>) -> Answer {
    match offered {
        Offered::Answered(reply) => match envelope::decode::<Result<Accepted, Error>>(&reply) {
            Ok(answer) => answered(answer),
            Err(error) => Answer::Failed(error),
        },
        Offered::Unsent(error) => Answer::Missed(Missed::Unsent(error)),
        Offered::Unknown(error) => {
            let into = class.map_or(String::new(), |class| format!(" into class `{class}`"));
            Answer::Failed(Error::new(
                ErrorKind::OutcomeUnknown,
                format!(
                    "the hand-off{into} may have been taken, so it is not offered again: {}",
                    error.message()
                ),
            ))
        }
    }
}

/// A forward hand-off an entry refused, before it admitted anything: why,
/// and the envelope back, so that a sender that called the entry directly
/// can offer the request again without having copied it first. The
/// envelope's context may have been taken; the sender makes it again from
/// the request's own. Boxed, and only on a refusal: an accepted hand-off's
/// answer stays as small as it was.
pub(crate) struct Refused {
    pub(crate) error: Error,
    pub(crate) envelope: Box<Forward>,
}

/// What came of one offer to an instance that did not fail the request:
/// on a miss, the envelope back, to be offered again.
pub(crate) enum Offer {
    /// Taken, with when it left, if the caller asked.
    Accepted(Accepted, Option<tokio::time::Instant>),
    /// Not taken; the envelope's context may have been read, and is made
    /// again before the next offer.
    Missed(Missed, Box<Forward>),
}

impl Offer {
    /// What this instance's entry, called directly at `sent`, answered.
    pub(crate) fn entered(
        answer: std::result::Result<Accepted, Refused>,
        sent: Option<tokio::time::Instant>,
    ) -> Result<Offer> {
        match answer {
            Ok(accepted) => Ok(Offer::Accepted(accepted, sent)),
            Err(Refused { error, envelope }) if declines(&error) => {
                Ok(Offer::Missed(Missed::Declined(error), envelope))
            }
            Err(refused) => Err(refused.error),
        }
    }

    /// What became of `envelope`, sent over velo at `sent`: boxed only on
    /// a miss.
    pub(crate) fn delivered(
        offered: Offered,
        sent: Option<tokio::time::Instant>,
        envelope: Forward,
    ) -> Result<Offer> {
        match delivered(offered, None) {
            Answer::Accepted(accepted) => Ok(Offer::Accepted(accepted, sent)),
            Answer::Missed(missed) => Ok(Offer::Missed(missed, Box::new(envelope))),
            Answer::Failed(error) => Err(error),
        }
    }
}

impl Runtime {
    /// Choose an instance of `segment` for `envelope` as `within` says, by
    /// a policy or a selector, telling a selector the instances that
    /// `declined` the request so far: the instance, and the selector's
    /// lease, with the meta it chose added to `envelope`'s context; or,
    /// apart, that the routing table had nothing to route to, and the
    /// request's own error.
    pub(crate) async fn choose(
        &self,
        segment: &str,
        within: &Within,
        cx: &Cx,
        envelope: &mut Forward,
        declined: &[gdl::InstanceId],
    ) -> std::result::Result<(InstanceId, Held), Unchosen> {
        match within {
            Within::Policy(policy) => Ok((
                self.routes()
                    .pick(segment, Picker::Request, *policy, self.random())
                    .map_err(Unchosen::Unroutable)?,
                Held::default(),
            )),
            Within::Selector {
                selector, encoding, ..
            } => {
                let (instance, picked) = self
                    .routes()
                    .choose(
                        segment,
                        &**selector,
                        cx,
                        &envelope.payload,
                        *encoding,
                        declined,
                    )
                    .await?;
                // A request that ended while its selector chose is not
                // handed on; its lease goes with `picked`. The caller's
                // select sees the end only at its next await, and the
                // send can leave before that.
                cx.check().map_err(Unchosen::Failed)?;
                envelope.cx.add_meta(picked.meta);
                Ok((instance, Held::selector(picked.lease)))
            }
        }
    }

    /// This instance's entry for `envelope`, called directly: velo is not
    /// relied on to deliver a message to its own sender. Not `async`, so
    /// that the envelope goes straight into the entry's boxed future, and
    /// an entry that declines gives it back.
    pub(crate) fn enter(
        &self,
        envelope: Forward,
    ) -> Result<BoxFuture<'static, std::result::Result<Accepted, Refused>>> {
        let entries = self.entries()?;
        let entry = entries.forward(envelope.hop, envelope.pos)?;
        Ok(entry(envelope))
    }

    /// Offer `envelope` to `instance` over velo: its encoding, then the
    /// send, and, when `timed`, when it left, after the encoding. Not
    /// `async`, so that a caller's future holds the send's alone: the
    /// envelope is only borrowed, and stays the caller's to offer again.
    pub(crate) fn deliver(
        &self,
        instance: InstanceId,
        envelope: &Forward,
        timed: bool,
    ) -> Result<(
        impl Future<Output = Offered> + Send + '_,
        Option<tokio::time::Instant>,
    )> {
        let bytes = envelope::encode(envelope)?;
        let sent = timed.then(tokio::time::Instant::now);
        let offered = self.fabric.offer(
            instance,
            &self.names().forward,
            bytes,
            self.config.setup_timeout,
        );
        Ok((offered, sent))
    }

    /// One offer of a failover, which holds its envelope boxed: to this
    /// instance's entry by a direct call, or to another over velo. The
    /// first offer is made inline in `send_forward` from the same parts.
    async fn offer(
        &self,
        instance: InstanceId,
        envelope: Box<Forward>,
        timed: bool,
    ) -> Result<Offer> {
        if instance == self.fabric.instance() {
            let entering = self.enter(*envelope)?;
            let sent = timed.then(tokio::time::Instant::now);
            return Offer::entered(entering.await, sent);
        }
        let (offering, sent) = self.deliver(instance, &envelope, timed)?;
        Offer::delivered(offering.await, sent, *envelope)
    }

    /// After `first` did not take `envelope` (`missed`), choose an
    /// instance of `segment` again as `within` says, from the routing table as
    /// it is then, and offer the request to it, until one takes it, the
    /// request fails or ends, or `instance_failovers` more were offered
    /// it. Nothing is excluded: the same instance may be chosen again, and
    /// a selector is told which instances declined, in order. When no
    /// failover is left, the request fails with the last refusal, or, when
    /// no instance refused it, with the last hand-off that never left.
    ///
    /// The request's deadline and cancel are checked before each choice.
    /// Each choice starts from the request's own context, `cx`, which
    /// `envelope`'s was made from, so a declined selector's meta is not
    /// sent on; a declined choice's lease ended before this call.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn offer_again(
        &self,
        segment: &str,
        within: &Within,
        cx: &Cx,
        envelope: Box<Forward>,
        timed: bool,
        first: InstanceId,
        missed: Missed,
    ) -> Result<Handed> {
        let mut envelope = envelope;
        let mut declined = Vec::new();
        if let Missed::Declined(_) = missed {
            declined.push(selector_id(first));
        }
        let mut missed = missed;
        for _ in 0..self.config.instance_failovers {
            cx.check()?;
            envelope.cx = WireCx::of(cx);
            let (instance, held) = match self
                .choose(segment, within, cx, &mut envelope, &declined)
                .await
            {
                Ok(chosen) => chosen,
                // Nothing is left to route to: the request fails with its
                // last refusal, as when no failover is left. A table
                // without the segment's instances is not the request's
                // fault, and the refusal says why it was not taken.
                Err(Unchosen::Unroutable(_)) => break,
                Err(Unchosen::Failed(error)) => return Err(error),
            };
            match self.offer(instance, envelope, timed).await? {
                Offer::Accepted(accepted, sent) => {
                    return Ok(Handed {
                        accepted,
                        sent,
                        held,
                    });
                }
                Offer::Missed(then, back) => {
                    drop(held);
                    if let Missed::Declined(_) = then {
                        declined.push(selector_id(instance));
                    }
                    missed = missed.then(then);
                    envelope = back;
                }
            }
        }
        Err(missed.into_error())
    }
}

/// A class boundary, as the classification reads it: the hop it falls on.
pub(crate) trait Boundary {
    /// The index of the hop the boundary falls on.
    fn hop(&self) -> usize;
}

impl Boundary for ClassBoundary {
    fn hop(&self) -> usize {
        self.hop
    }
}

/// For each of `hops` hops, whether its forward hand-offs that reach this
/// segment are offers: the hop crosses one of `boundaries` at its inner
/// end, and `inner_here` says this segment holds that end. The hop's
/// other stages are all in the class it leaves (the boundary rule), so
/// every hand-off of it that reaches the segment comes from another class.
/// One pass over the boundaries, so that a graph with many of them costs
/// in proportion to its size.
pub(crate) fn offered_hops<'a, B: Boundary + 'a>(
    boundaries: impl IntoIterator<Item = &'a B>,
    hops: usize,
    inner_here: impl Fn(usize) -> bool,
) -> Vec<bool> {
    let mut offered = vec![false; hops];
    for boundary in boundaries {
        let hop = boundary.hop();
        if let Some(offer) = offered.get_mut(hop) {
            *offer = inner_here(hop);
        }
    }
    offered
}

#[cfg(test)]
mod failover;

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{Boundary, Missed, offered_hops};
    use gdl::{Error, ErrorKind};

    /// A request fails with its last refusal; a hand-off that never left
    /// is the error only while no entry has refused it.
    #[test]
    fn the_last_refusal_outlasts_a_hand_off_that_never_left() {
        let full = || Missed::Declined(Error::new(ErrorKind::Overloaded, "full"));
        let unsent = || Missed::Unsent(Error::new(ErrorKind::Transport, "unsent"));
        let kind = |missed: Missed| missed.into_error().kind();
        assert_eq!(kind(full().then(unsent())), ErrorKind::Overloaded);
        assert_eq!(
            kind(full().then(unsent()).then(unsent())),
            ErrorKind::Overloaded
        );
        assert_eq!(kind(unsent().then(full())), ErrorKind::Overloaded);
        assert_eq!(kind(unsent().then(unsent())), ErrorKind::Transport);
        let retired = Missed::Declined(Error::new(ErrorKind::Placement, "other deployment"));
        assert_eq!(kind(full().then(retired)), ErrorKind::Placement);
    }

    /// A boundary that counts each time its hop is read.
    struct Counted<'a> {
        hop: usize,
        reads: &'a Cell<usize>,
    }

    impl Boundary for Counted<'_> {
        fn hop(&self) -> usize {
            self.reads.set(self.reads.get() + 1);
            self.hop
        }
    }

    /// How many reads of a boundary classifying `hops` hops takes, with a
    /// boundary at every other hop, and what it finds.
    fn classified(hops: usize) -> (usize, Vec<bool>) {
        let reads = Cell::new(0);
        let boundaries: Vec<Counted<'_>> = (0..hops)
            .step_by(2)
            .map(|hop| Counted { hop, reads: &reads })
            .collect();
        let offered = offered_hops(&boundaries, hops, |hop| hop % 4 == 0);
        (reads.get(), offered)
    }

    /// Each hop that crosses a boundary at an inner end this segment holds
    /// is an offer, and no other; classifying them reads each boundary a
    /// bounded number of times, however it holds them, so twice the hops
    /// cost twice the reads, not four times.
    #[test]
    fn classifying_offers_reads_each_boundary_once() {
        let (_, offered) = classified(8);
        assert_eq!(
            offered,
            [true, false, false, false, true, false, false, false]
        );
        let (small, _) = classified(512);
        let (large, _) = classified(1024);
        assert!(
            large <= small * 3,
            "{small} reads for 512 hops, {large} for 1024"
        );
    }
}

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
//! An entry that declines says why, as a chooser in another class reads
//! it ([`Decline`]): its cell is full, its cell is gone, or this instance
//! cannot serve the request and another of its cell may. The reason
//! follows the refusal in the reply, where a build that does not know it
//! never reads it ([`refusal`]).
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
    /// The entry declined it, saying why.
    Declined(
        Error,
        #[cfg_attr(
            not(feature = "hub"),
            allow(dead_code, reason = "only a chooser, under `hub`, reads why")
        )]
        Decline,
    ),
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
            (declined @ Missed::Declined(..), Missed::Unsent(_)) => declined,
            (_, then) => then,
        }
    }

    pub(crate) fn into_error(self) -> Error {
        match self {
            Missed::Declined(error, _) | Missed::Unsent(error) => error,
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

/// Why an entry declined an offer, for a chooser that sent it from
/// another class: whether to damp only this cell, to pass it over until
/// its follow changes, or to offer the request to the cell's other entry
/// instances. A segment declines offers by its own policy with
/// [`Segment::decline_offers_as`](super::Segment::decline_offers_as).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Decline {
    /// The cell has no room now, or this instance retired: the chooser
    /// damps the cell, and offers it requests again after the window.
    #[default]
    Full,
    /// The cell takes no work from other cells until its state changes,
    /// as a draining cell does: the chooser passes it over until its
    /// follow changes.
    Gone,
    /// This instance cannot serve the request, and another entry instance
    /// of its cell may: for example, it does not have the request's model.
    /// The chooser offers the request to each other entry instance of the
    /// cell once, and to another cell only when every one declined. It
    /// damps nothing, since the decline says nothing of the cell's room.
    Instance,
}

/// The reasons as they follow a refusal in the reply.
const GONE: &str = "gone";
const INSTANCE: &str = "instance";

/// What an entry that declined a forward hand-off answers: the refusal,
/// encoded as every build reads it, then, for any reason but
/// [`Decline::Full`], the reason. A build that does not know the reason
/// reads the refusal and stops there, as `rmp_serde` reads one value and
/// leaves the bytes after it, so it takes the refusal as a full cell's:
/// a chooser before [`Decline::Instance`] damps the cell for one window
/// and offers the request to another cell.
pub(crate) fn refusal(error: &Error, decline: Decline) -> Result<Vec<u8>> {
    let mut reply = envelope::encode(&Err::<Accepted, &Error>(error))?;
    match decline {
        Decline::Full => {}
        Decline::Gone => reply.extend(envelope::encode(&GONE)?),
        Decline::Instance => reply.extend(envelope::encode(&INSTANCE)?),
    }
    Ok(reply)
}

/// The reason that follows the refusal in `reply`: [`Decline::Full`]
/// when none does, as from a build before the reason, or one this build
/// does not know. Read only once an entry declined, so that an accepted
/// hand-off's answer is decoded once, as before.
fn decline(reply: &[u8]) -> Decline {
    let mut after = reply;
    if rmp_serde::decode::from_read::<_, serde::de::IgnoredAny>(&mut after).is_err() {
        return Decline::Full;
    }
    match rmp_serde::from_slice::<&str>(after) {
        Ok(GONE) => Decline::Gone,
        Ok(INSTANCE) => Decline::Instance,
        _ => Decline::Full,
    }
}

/// What became of an offer velo carried, into the class `class` when it
/// crossed into another. An unknown outcome is the request's error, of the
/// kind `OutcomeUnknown`, never `Transport`: the hand-off may have been
/// taken and run, so a caller that tried it again could run it twice.
pub(crate) fn delivered(offered: Offered, class: Option<&str>) -> Answer {
    match offered {
        Offered::Answered(reply) => match envelope::decode::<Result<Accepted, Error>>(&reply) {
            Ok(Ok(accepted)) => Answer::Accepted(accepted),
            Ok(Err(error)) if declines(&error) => {
                Answer::Missed(Missed::Declined(error, decline(&reply)))
            }
            Ok(Err(error)) => Answer::Failed(error),
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
    /// Why, for a chooser in another class, when `error` declines.
    pub(crate) decline: Decline,
    pub(crate) envelope: Box<Forward>,
}

/// Why an entry refused a forward hand-off before it admitted anything,
/// with the reason a chooser in another class reads: [`Decline::Full`]
/// for every refusal but a gone cell's.
pub(crate) struct Refusal {
    pub(crate) error: Error,
    pub(crate) decline: Decline,
}

impl From<Error> for Refusal {
    fn from(error: Error) -> Self {
        Self {
            error,
            decline: Decline::Full,
        }
    }
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
            Err(Refused {
                error,
                decline,
                envelope,
            }) if declines(&error) => Ok(Offer::Missed(Missed::Declined(error, decline), envelope)),
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
        envelope: &mut Forward,
        timed: bool,
    ) -> Result<(
        impl Future<Output = Offered> + Send + '_,
        Option<tokio::time::Instant>,
    )> {
        let bytes = envelope.wire()?;
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
        mut envelope: Box<Forward>,
        timed: bool,
    ) -> Result<Offer> {
        if instance == self.fabric.instance() {
            let entering = self.enter(*envelope)?;
            let sent = timed.then(tokio::time::Instant::now);
            return Offer::entered(entering.await, sent);
        }
        let (offering, sent) = self.deliver(instance, &mut envelope, timed)?;
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
        if let Missed::Declined(..) = missed {
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
                    if let Missed::Declined(..) = then {
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

    use super::{Answer, Boundary, Decline, Missed, delivered, offered_hops, refusal};
    use crate::dist::envelope::{Accepted, decode, encode};
    use crate::dist::fabric::Offered;
    use gdl::{Error, ErrorKind};

    /// A request fails with its last refusal; a hand-off that never left
    /// is the error only while no entry has refused it.
    #[test]
    fn the_last_refusal_outlasts_a_hand_off_that_never_left() {
        let full = || Missed::Declined(Error::new(ErrorKind::Overloaded, "full"), Decline::Full);
        let unsent = || Missed::Unsent(Error::new(ErrorKind::Transport, "unsent"));
        let kind = |missed: Missed| missed.into_error().kind();
        assert_eq!(kind(full().then(unsent())), ErrorKind::Overloaded);
        assert_eq!(
            kind(full().then(unsent()).then(unsent())),
            ErrorKind::Overloaded
        );
        assert_eq!(kind(unsent().then(full())), ErrorKind::Overloaded);
        assert_eq!(kind(unsent().then(unsent())), ErrorKind::Transport);
        let retired = Missed::Declined(
            Error::new(ErrorKind::Placement, "other deployment"),
            Decline::Full,
        );
        assert_eq!(kind(full().then(retired)), ErrorKind::Placement);
    }

    /// What a chooser makes of an entry's `reply`.
    fn read(reply: Vec<u8>) -> Answer {
        delivered(Offered::Answered(reply), Some("model_x"))
    }

    /// The kind and the reason of a decline.
    fn declined(answer: Answer) -> (ErrorKind, Decline) {
        match answer {
            Answer::Missed(Missed::Declined(error, decline)) => (error.kind(), decline),
            _ => panic!("not a decline"),
        }
    }

    /// A refusal as gone, or as the instance's, carries its reason after
    /// the refusal. A build before the reason reads the refusal alone, as
    /// the full cell's it always read, and so does this build when no
    /// reason follows, or one it does not know, or bytes it cannot read. A
    /// build that knows gone but not the instance's reason reads the
    /// latter as full. A full cell's refusal is sent byte for byte as
    /// before the reason.
    #[test]
    fn a_reason_after_a_refusal_is_read_here_and_skipped_by_an_older_build() {
        let error = Error::new(ErrorKind::Overloaded, "draining");
        let before = encode(&Err::<Accepted, Error>(error.clone())).expect("encode");
        let full = refusal(&error, Decline::Full).expect("encode");
        let gone = refusal(&error, Decline::Gone).expect("encode");
        let instance = refusal(&error, Decline::Instance).expect("encode");
        assert_eq!(full, before, "a full cell's refusal changed on the wire");
        assert!(gone.starts_with(&before) && gone.len() > before.len());
        assert!(instance.starts_with(&before) && instance.len() > before.len());
        // How every build before the reason reads a reply.
        for reply in [&full, &gone, &instance] {
            let older: Result<Accepted, Error> = decode(reply).expect("an older build reads it");
            assert_eq!(older.expect_err("a refusal"), error);
        }
        // How a build that knows gone, and not the instance's reason,
        // reads each: an unknown reason is a full cell's.
        let known = |reply: &[u8]| {
            let mut after = reply;
            rmp_serde::decode::from_read::<_, serde::de::IgnoredAny>(&mut after)
                .expect("the refusal");
            match rmp_serde::from_slice::<&str>(after) {
                Ok("gone") => Decline::Gone,
                _ => Decline::Full,
            }
        };
        assert_eq!(known(&instance), Decline::Full);
        assert_eq!(known(&gone), Decline::Gone);
        assert_eq!(
            declined(read(instance)),
            (ErrorKind::Overloaded, Decline::Instance)
        );
        assert_eq!(declined(read(gone)), (ErrorKind::Overloaded, Decline::Gone));
        assert_eq!(declined(read(full)), (ErrorKind::Overloaded, Decline::Full));
        let mut later = before.clone();
        later.extend(encode(&"throttled").expect("encode"));
        assert_eq!(
            declined(read(later)),
            (ErrorKind::Overloaded, Decline::Full)
        );
        let mut unreadable = before;
        // A marker msgpack never uses.
        unreadable.push(0xc1);
        assert_eq!(
            declined(read(unreadable)),
            (ErrorKind::Overloaded, Decline::Full)
        );
        let placement = Error::new(ErrorKind::Placement, "other deployment");
        let gone = refusal(&placement, Decline::Gone).expect("encode");
        assert_eq!(declined(read(gone)), (ErrorKind::Placement, Decline::Gone));
        // Only a decline has a reason: after an acceptance or the
        // request's own error, the bytes are not read.
        let mut accepted = encode(&Ok::<Accepted, Error>(None)).expect("encode");
        accepted.extend(encode(&"gone").expect("encode"));
        assert!(matches!(read(accepted), Answer::Accepted(None)));
        let deadline = refusal(&Error::deadline_exceeded(), Decline::Gone).expect("encode");
        assert!(matches!(read(deadline), Answer::Failed(_)));
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

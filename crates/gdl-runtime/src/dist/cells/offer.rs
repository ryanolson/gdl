//! A chooser's hand-off into another class is an offer: the cell chosen
//! for it may decline it, and then another cell is chosen inside the same
//! call. Only a proven refusal moves the request on. A cell declined it
//! when its entry answered, before taking anything, that it has no room
//! or is not of this deployment; it never left when velo refused to send
//! it. Anything else after the send may have run the request there, so
//! it is the request's error, and is not tried again.
//!
//! The cells a request was offered to are a local of [`Runtime::offer`],
//! empty, and so unallocated, until a cell declines: never on the wire,
//! never kept per request, and gone when the call returns.

use gdl::{Cx, Error, ErrorKind, Result};

use super::{Crossing, Within};
use crate::dist::envelope::{self, Accepted, Forward};
use crate::dist::fabric::Offered;
use crate::dist::runtime::{Handed, Runtime};

impl Runtime {
    /// Hand `envelope` into the class `crossing` enters: choose a cell
    /// and an instance of it, offer it the request, and when the cell
    /// declines, or the hand-off provably never left, choose again among
    /// the cells not yet offered it, up to `cell_failovers` times. A
    /// declined choice's lease, and the meta its selector added, are let
    /// go before the next choice. When every cell that may take it
    /// declined, the request fails with the last refusal; when none could
    /// from the start, with `Routing`.
    ///
    /// A chooser never chooses itself: the entry segment is in another
    /// class, so the hand-off always crosses velo.
    pub(crate) async fn offer(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        mut envelope: Forward,
        timed: bool,
    ) -> Result<Handed> {
        let mut offered: Vec<u64> = Vec::new();
        let mut declined: Option<Error> = None;
        loop {
            let chosen = match crossing.within() {
                Within::Policy(policy) => crossing
                    .pick(*policy, self.random(), &offered)
                    .map(|(instance, cell)| (instance, cell, None, None)),
                Within::Selector { selector, .. } => crossing
                    .choose(&**selector, cx, &envelope.payload, &offered)
                    .await?
                    .map(|(instance, cell, picked)| {
                        let added = envelope.cx.add_meta_undoably(picked.meta);
                        (instance, cell, picked.lease, Some(added))
                    }),
            };
            let Some((instance, cell, lease, added)) = chosen else {
                return Err(declined.unwrap_or_else(|| crossing.no_cell()));
            };
            let bytes = envelope::encode(&envelope)?;
            let sent = timed.then(tokio::time::Instant::now);
            let offer = self.fabric.offer(
                instance,
                &self.names().forward,
                bytes,
                self.config.setup_timeout,
            );
            let refusal = match offer.await {
                Offered::Answered(reply) => {
                    match envelope::decode::<Result<Accepted, Error>>(&reply)? {
                        Ok(accepted) => {
                            return Ok(Handed {
                                accepted,
                                sent,
                                lease,
                            });
                        }
                        Err(error) if declines(&error) => error,
                        Err(error) => return Err(error),
                    }
                }
                Offered::Unsent(error) => error,
                Offered::Unknown(error) => {
                    return Err(Error::new(
                        ErrorKind::OutcomeUnknown,
                        format!(
                            "the hand-off into class `{}` may have been taken, so it is not offered again: {}",
                            crossing.class(),
                            error.message()
                        ),
                    ));
                }
            };
            drop(lease);
            if let Some(added) = added {
                envelope.cx.undo_meta(added);
            }
            if offered.len() >= self.config.cell_failovers {
                return Err(refusal);
            }
            offered.push(cell);
            declined = Some(refusal);
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
fn declines(error: &Error) -> bool {
    matches!(error.kind(), ErrorKind::Overloaded | ErrorKind::Placement)
}

use std::fmt;

use super::{Deadline, Scope};
use crate::error::{Error, Result};

/// What can end a request early, its cancel and its deadline, apart from
/// the rest of its context.
///
/// [`Cx::interrupts`](crate::Cx::interrupts) takes it. It holds the
/// context's scope and deadline only, never its extensions or metadata, so
/// a stream that outlives the node's call can hold it to check for a cancel
/// or a passed deadline without keeping the request's other values alive. Taking or cloning it
/// clones one `Arc` and allocates nothing.
///
/// ```
/// use gdl::{Cx, ErrorKind};
///
/// let cx = Cx::new();
/// let interrupts = cx.interrupts();
/// assert!(interrupts.check().is_ok());
/// cx.cancel();
/// assert_eq!(interrupts.check().unwrap_err().kind(), ErrorKind::Cancelled);
/// ```
#[derive(Clone)]
pub struct Interrupts {
    scope: Scope,
    deadline: Option<Deadline>,
}

impl Interrupts {
    pub(super) fn new(scope: Scope, deadline: Option<Deadline>) -> Self {
        Self { scope, deadline }
    }

    /// `Ok` while the request may continue; the reason otherwise, as
    /// [`Cx::check`](crate::Cx::check) reports it: a passed deadline ahead
    /// of a cancel.
    ///
    /// ```
    /// use std::time::Duration;
    /// use gdl::{Cx, ErrorKind};
    ///
    /// let cx = Cx::new().with_timeout(Duration::ZERO);
    /// let interrupts = cx.interrupts();
    /// cx.cancel();
    /// assert_eq!(interrupts.check().unwrap_err().kind(), ErrorKind::DeadlineExceeded);
    /// ```
    pub fn check(&self) -> Result<()> {
        check(&self.scope, self.deadline)
    }
}

impl fmt::Debug for Interrupts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Interrupts")
            .field("deadline", &self.deadline)
            .field("cancelled", &self.scope.is_cancelled())
            .finish()
    }
}

/// The one rule both [`Cx::check`](crate::Cx::check) and
/// [`Interrupts::check`] apply. A passed deadline is reported ahead of a
/// cancel: when the deadline ends a stream, dropping that stream cancels the
/// attempt too, and the deadline is the cause worth reporting.
#[inline]
pub(super) fn check(scope: &Scope, deadline: Option<Deadline>) -> Result<()> {
    if deadline.is_some_and(Deadline::is_expired) {
        return Err(Error::deadline_exceeded());
    }
    if scope.is_cancelled() {
        return Err(Error::cancelled());
    }
    Ok(())
}

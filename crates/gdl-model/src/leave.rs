//! When an instance, or a pool's hub, that knows when it will be stopped
//! (a batch job's walltime) begins to leave, so that it has drained
//! before then. See docs/src/concepts/lifecycle.md.

use std::time::Duration;

use crate::Error;

/// When something must be gone, in milliseconds since the Unix epoch, and
/// how long before that, past its drain bound, it means to be gone: an
/// instance's `[runtime] leave_by` and `leave_margin_ms`, or a pool hub's
/// `[pool]` keys of the same names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaveBy {
    /// When it must be gone, in milliseconds since the Unix epoch.
    pub unix_ms: u64,
    /// How long before `unix_ms` its drain must have ended: room for its
    /// exit after the drain's bound, and for a clock that differs from
    /// the one that set `unix_ms`.
    pub margin: Duration,
}

impl LeaveBy {
    /// The margin when `leave_margin_ms` is not set: 30 s, which covers
    /// the exit after a drain's bound (a second or two) many times over,
    /// and a clock out by seconds.
    pub const DEFAULT_MARGIN: Duration = Duration::from_secs(30);

    /// When it must be gone, as a deadline crosses segments: nanoseconds
    /// since the Unix epoch.
    pub fn unix_nanos(&self) -> u64 {
        self.unix_ms.saturating_mul(1_000_000)
    }

    /// The wall clock `wall`, read `elapsed` ago on the monotonic clock,
    /// as it reads now. A caller reads the monotonic clock, then the wall
    /// clock, then the monotonic clock again, and passes this to
    /// [`LeaveBy::begin_in`] with the time between the two monotonic
    /// reads: the leave point then counts from the first read, as
    /// `Deadline::from_wall` counts a deadline, so a delay between the
    /// reads makes it earlier, and one that leaves it behind the second
    /// read is a start with no time left.
    pub fn wall_since(wall: u64, elapsed: Duration) -> u64 {
        wall.saturating_add(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
    }

    /// How long from now it begins to leave: when it must be gone, mapped
    /// onto this host's clock as a deadline from another segment is (gdl's
    /// `Deadline::from_wall`: the time left on the wall clock `wall_now`,
    /// in nanoseconds since the Unix epoch, less `skew`, and none left
    /// within `skew` of now), less `drain` and the margin. "Now" is when
    /// `wall_now` holds: read the clocks as [`LeaveBy::wall_since`] says,
    /// and add the answer to the second monotonic read. Refused, in one line
    /// that names the value, `drain`, and the margin, when there is no time
    /// left to start in. `table` and `drain_key` name the settings the line
    /// names: `runtime` and `drain_ms`, or `pool` and `stop_wait_ms`.
    pub fn begin_in(
        &self,
        wall_now: u64,
        skew: Duration,
        drain: Duration,
        table: &str,
        drain_key: &str,
    ) -> Result<Duration, Error> {
        let skew = u64::try_from(skew.as_nanos()).unwrap_or(u64::MAX);
        let left = self
            .unix_nanos()
            .saturating_sub(wall_now)
            .saturating_sub(skew);
        let remaining = (left > 0).then(|| Duration::from_nanos(left));
        let lead = drain.saturating_add(self.margin);
        match remaining {
            Some(remaining) if remaining > lead => Ok(remaining - lead),
            _ => {
                let when = match remaining {
                    Some(remaining) => format!("is {} ms away", remaining.as_millis()),
                    None => "is past".to_string(),
                };
                Err(Error::new(format!(
                    "`{table}.leave_by` {} {when}, too close to drain within `{table}.{drain_key}` {} and be gone `{table}.leave_margin_ms` {} before it",
                    self.unix_ms,
                    drain.as_millis(),
                    self.margin.as_millis()
                )))
            }
        }
    }
}

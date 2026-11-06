//! When an instance, or a pool's hub, that knows when it will be stopped
//! begins to leave: the time left on its own clock, less the clock skew,
//! its drain bound, and its margin. A value too close for that is refused
//! at its start in one line.

use std::time::Duration;

use gdl_model::{DEFAULT_CLOCK_SKEW, LeaveBy};

const MS: fn(u64) -> Duration = Duration::from_millis;

/// One millisecond, in nanoseconds.
const NS_PER_MS: u64 = 1_000_000;

fn leave_by(unix_ms: u64, margin_ms: u64) -> LeaveBy {
    LeaveBy {
        unix_ms,
        margin: MS(margin_ms),
    }
}

/// The time left is read against the wall clock less the skew, as a
/// deadline from another segment is, then the drain and the margin come
/// off it.
#[test]
fn it_begins_to_leave_the_skew_its_drain_and_its_margin_before_it_must_be_gone() {
    let at = leave_by(1_800_000_010_000, 500);
    let now = 1_800_000_000_000 * NS_PER_MS;
    let begin = at.begin_in(now, MS(50), MS(3_000), "runtime", "drain_ms");
    assert_eq!(begin.expect("in time"), MS(10_000 - 50 - 3_000 - 500));
}

#[test]
fn one_too_close_for_its_drain_and_margin_is_refused_naming_the_three_numbers() {
    let at = leave_by(1_800_000_003_000, 500);
    // Exactly the drain and the margin leaves nothing to start in.
    for left_ms in [3_500, 1] {
        let now = (1_800_000_003_000 - left_ms) * NS_PER_MS;
        let refused = at
            .begin_in(now, Duration::ZERO, MS(3_000), "runtime", "drain_ms")
            .expect_err("too close")
            .to_string();
        for named in [
            "`runtime.leave_by` 1800000003000",
            &format!("is {left_ms} ms away"),
            "`runtime.drain_ms` 3000",
            "`runtime.leave_margin_ms` 500",
        ] {
            assert!(refused.contains(named), "{refused}");
        }
        assert!(!refused.contains('\n'), "{refused}");
    }
}

/// Past, or within the skew of now, as a deadline that close counts as
/// passed.
#[test]
fn one_already_past_or_within_the_skew_is_refused_naming_the_three_numbers() {
    let at = leave_by(1_700_000_000_000, 30_000);
    for now_ms in [1_700_000_001_000, 1_700_000_000_000, 1_699_999_999_990] {
        let refused = at
            .begin_in(
                now_ms * NS_PER_MS,
                MS(50),
                MS(660_000),
                "pool",
                "stop_wait_ms",
            )
            .expect_err("past")
            .to_string();
        for named in [
            "`pool.leave_by` 1700000000000",
            "is past",
            "`pool.stop_wait_ms` 660000",
            "`pool.leave_margin_ms` 30000",
        ] {
            assert!(refused.contains(named), "{now_ms}: {refused}");
        }
    }
}

/// The wall-clock form a deadline crosses in: nanoseconds, saturating
/// rather than wrapping for a value no clock reaches.
#[test]
fn its_wall_clock_form_is_in_nanoseconds_and_saturates() {
    assert_eq!(
        leave_by(1_800_000_000_000, 0).unix_nanos(),
        1_800_000_000_000_000_000
    );
    assert_eq!(leave_by(u64::MAX, 0).unix_nanos(), u64::MAX);
}

#[test]
fn the_defaults_are_a_thirty_second_margin_and_a_fifty_millisecond_skew() {
    assert_eq!(LeaveBy::DEFAULT_MARGIN, Duration::from_secs(30));
    assert_eq!(DEFAULT_CLOCK_SKEW, Duration::from_millis(50));
}

/// A wall clock read a while ago, as it reads now: moved on by what the
/// monotonic clock counted since, saturating rather than wrapping.
#[test]
fn a_wall_clock_read_earlier_is_moved_on_by_the_time_since() {
    assert_eq!(LeaveBy::wall_since(1_000, MS(2)), 1_000 + 2 * NS_PER_MS);
    assert_eq!(LeaveBy::wall_since(u64::MAX - 1, MS(1)), u64::MAX);
}

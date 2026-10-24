use std::collections::BTreeSet;
use std::time::Duration;

use tokio::time::Instant;

use super::{Backoff, HEALTHY};

const BASE: Duration = Duration::from_millis(100);
const CAP: Duration = Duration::from_secs(1);

/// The step of the `n`th wait after a failure, from the first, while no
/// watch was healthy: the base doubled `n` times, up to the cap.
fn step(n: u32) -> Duration {
    BASE.saturating_mul(2u32.saturating_pow(n)).min(CAP)
}

fn within(wait: Duration, step: Duration) -> bool {
    wait >= step / 2 && wait <= step
}

/// Each wait lies between half its step and the whole step, and the
/// steps double from the base to the cap and stay there: the ranges of
/// successive steps do not overlap below the cap, so a wait in its own
/// range is a step that doubled. Over many seeds, so that no draw in
/// particular is relied on.
#[test]
fn waits_double_from_the_base_to_the_cap() {
    let now = Instant::now();
    for seed in 0..256 {
        let mut backoff = Backoff::seeded(BASE, CAP, seed);
        for n in 0..12 {
            let wait = backoff.wait(None, now);
            assert!(
                within(wait, step(n)),
                "seed {seed}: wait {n} was {wait:?}, its step {:?}",
                step(n)
            );
        }
    }
}

/// A watch that completed its snapshot and then ran for the healthy span
/// returns the step to the base: its end waits within the base, and so
/// does a failure right after it, from which the step doubles again. One
/// that ran for less advances the step as any failure does, and so does
/// one that never completed its snapshot, however long it ran.
#[test]
fn a_healthy_span_returns_the_step_to_the_base() {
    let start = Instant::now();
    for seed in 0..64 {
        let mut backoff = Backoff::seeded(BASE, CAP, seed);
        for _ in 0..8 {
            backoff.wait(None, start);
        }
        let opened = start + CAP;
        let short = backoff.wait(Some(opened), opened + HEALTHY - Duration::from_millis(1));
        assert!(
            within(short, CAP),
            "seed {seed}: a short watch waited {short:?}"
        );
        let unsynced = backoff.wait(None, opened + HEALTHY * 4);
        assert!(
            within(unsynced, CAP),
            "seed {seed}: a watch with no snapshot waited {unsynced:?}"
        );
        let healthy = backoff.wait(Some(opened), opened + HEALTHY);
        assert!(
            within(healthy, BASE),
            "seed {seed}: a healthy end waited {healthy:?}"
        );
        let after = opened + HEALTHY + BASE;
        for n in 0..4 {
            let wait = backoff.wait(None, after);
            assert!(
                within(wait, step(n)),
                "seed {seed}: wait {n} after the reset was {wait:?}"
            );
        }
    }
}

/// A cap below the base, as a configured base can be, does not shorten
/// the base: the waits stay within it.
#[test]
fn a_cap_below_the_base_keeps_the_base() {
    let now = Instant::now();
    let mut backoff = Backoff::seeded(CAP, BASE, 7);
    for _ in 0..8 {
        let wait = backoff.wait(None, now);
        assert!(within(wait, CAP), "waited {wait:?}");
    }
}

/// Many watchers that fail at the same instant do not all wait the same:
/// each is seeded apart, so their first waits spread over the window
/// between half the base and the base. Uniform draws of 64 over 50 ms
/// collide, or span less than a quarter of it, with a vanishing chance;
/// no particular draw is relied on.
#[test]
fn watchers_that_fail_together_spread_over_the_window() {
    let now = Instant::now();
    let waits: Vec<Duration> = (0..64)
        .map(|_| Backoff::new(BASE, CAP).wait(None, now))
        .collect();
    assert!(waits.iter().all(|wait| within(*wait, BASE)), "{waits:?}");
    let distinct: BTreeSet<Duration> = waits.iter().copied().collect();
    assert!(
        distinct.len() >= 32,
        "only {} distinct waits",
        distinct.len()
    );
    let spread = *distinct.last().expect("waits") - *distinct.first().expect("waits");
    assert!(spread >= BASE / 8, "the waits span only {spread:?}");
}

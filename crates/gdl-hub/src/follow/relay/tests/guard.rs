//! The hub's half of the epoch guard: seeded, random sequences of what a
//! parent hub's follower tells its relay of one pool, checked after every
//! step. Records of the pool's hubs arrive, at the same epoch or a later
//! one, fresh, lapsed, draining, or scaled to 0; the follower begins a
//! follow of the current record, which installs or never does, drains,
//! and ends; the pool withdraws, by its hub or an operator past it, and a
//! later hub lists it again.
//!
//! The invariant: the relay's listing of the pool pairs its condition and
//! its drain with the hub of its epoch. Its epoch is the newest hub heard
//! of; it is `missing` only if that hub's link was lost, or its record
//! lapsed, since it was last heard from; scaled to 0 only if that hub's
//! record says so; and draining only if that hub said so.
//!
//! On a failure it prints the seed and the steps. `GDL_EPOCH_GUARD_SEEDS`
//! sets how many seeds run, and `GDL_EPOCH_GUARD_SEED` runs one.

use std::collections::BTreeSet;

use gdl_pools::{PoolCondition, PoolRecord, PoolState, Withdrawal};
use gdl_watch::PoolWatchEvent;

use super::{held, installed_at};
use crate::follow::relay::Relay;

const SEEDS: u64 = 256;
const STEPS: usize = 300;

/// splitmix64, as the follower's half uses.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// What has happened to each of the pool's hubs, by epoch: the truth the
/// listing is checked against.
#[derive(Default)]
struct Truth {
    /// The epoch of the newest record or link heard of, while listed.
    newest: Option<u64>,
    /// The hubs whose record lapsed since it was last written fresh.
    lapsed: BTreeSet<u64>,
    /// The hubs whose link was lost since one was last installed.
    lost: BTreeSet<u64>,
    /// The hubs whose last record says scaled to 0.
    scaled: BTreeSet<u64>,
    /// The hubs that said they drain.
    drained: BTreeSet<u64>,
}

/// The pool's record of `epoch`, at write `seq`.
fn record(epoch: u64, seq: u64, state: PoolState, scaled: bool) -> PoolRecord {
    serde_json::from_value(serde_json::json!({
        "name": "m",
        "class": "model_x",
        "region": "r",
        "home": "global",
        "contract": 1,
        "placement": 1,
        "state": state,
        "presence": if scaled { "scaled_to_zero" } else { "serving" },
        "epoch": epoch,
        "seq": seq,
        "hub": "hub:1"
    }))
    .expect("a record")
}

/// The relay's listing of the pool: its epoch, condition, and drain.
fn listing(relay: &Relay) -> Option<(u64, PoolCondition, bool)> {
    relay
        .lock()
        .listings
        .listing("model_x", "m", std::time::Instant::now())
        .map(|listing| (listing.epoch, listing.condition, listing.draining))
}

/// The broken invariant, if `listed` does not belong to one hub.
fn check(truth: &Truth, listed: Option<(u64, PoolCondition, bool)>) -> Result<(), String> {
    let Some((epoch, condition, draining)) = listed else {
        return match truth.newest {
            None => Ok(()),
            Some(newest) => Err(format!("not listed, though hub {newest} is")),
        };
    };
    if truth.newest != Some(epoch) {
        return Err(format!(
            "listed at {epoch}, the newest hub is {:?}",
            truth.newest
        ));
    }
    let fits = match condition {
        PoolCondition::ScaledToZero => truth.scaled.contains(&epoch),
        PoolCondition::Missing => {
            !truth.scaled.contains(&epoch)
                && (truth.lapsed.contains(&epoch) || truth.lost.contains(&epoch))
        }
        PoolCondition::Live => {
            !truth.scaled.contains(&epoch)
                && !truth.lapsed.contains(&epoch)
                && !truth.lost.contains(&epoch)
        }
    };
    if !fits || (draining && !truth.drained.contains(&epoch)) {
        return Err(format!(
            "listed ({epoch}, {condition:?}, draining {draining}): hub {epoch} lapsed {}, lost {}, scaled {}, drained {}",
            truth.lapsed.contains(&epoch),
            truth.lost.contains(&epoch),
            truth.scaled.contains(&epoch),
            truth.drained.contains(&epoch),
        ));
    }
    Ok(())
}

/// Run `seed` for `steps`: the steps up to the first broken invariant.
fn run(seed: u64, steps: usize) -> Result<(), (Vec<String>, String)> {
    let mut rng = Rng(seed);
    let relay = held();
    let mut truth = Truth::default();
    let (mut epoch, mut seq) = (10_u64, 1_u64);
    // The current follow: its generation, the epoch of the record it
    // began from, and whether it installed.
    let mut follow: Option<(u64, u64, bool)> = None;
    let mut withdrawn = false;
    let mut trace: Vec<String> = Vec::new();
    for _ in 0..steps {
        let what = match rng.below(9) {
            // A record: the same hub's next write, or a later hub's.
            0 | 1 => {
                if withdrawn || rng.below(3) == 0 {
                    epoch += 1 + rng.below(2);
                    seq = 1;
                    withdrawn = false;
                } else {
                    seq += 1;
                }
                let state = if rng.below(5) == 0 {
                    PoolState::Draining
                } else {
                    PoolState::Ready
                };
                let scaled = rng.below(8) == 0;
                let lapsed = rng.below(6) == 0;
                relay.record(
                    &record(epoch, seq, state, scaled),
                    lapsed.then(std::time::Instant::now),
                );
                // The follower ends a follow of an earlier hub, or of a
                // pool scaled to 0, as it decides on the record.
                if let Some((generation, of, _)) = follow
                    && (of < epoch || scaled)
                {
                    relay.end("model_x", "m", generation);
                    truth.lost.insert(of);
                    follow = None;
                }
                truth.newest = Some(truth.newest.map_or(epoch, |newest| newest.max(epoch)));
                if lapsed {
                    truth.lapsed.insert(epoch);
                } else {
                    truth.lapsed.remove(&epoch);
                }
                if scaled {
                    truth.scaled.insert(epoch);
                } else {
                    truth.scaled.remove(&epoch);
                }
                if state == PoolState::Draining {
                    truth.drained.insert(epoch);
                }
                format!("record ({epoch}, {seq}), {state:?}, scaled {scaled}, lapsed {lapsed}")
            }
            // The current record's lease passes.
            2 if truth.newest == Some(epoch) => {
                relay.lapsed("model_x", "m", std::time::Instant::now());
                truth.lapsed.insert(epoch);
                format!("record of {epoch} lapses")
            }
            // A follow of the current record begins, ending any other.
            3 if truth.newest.is_some() => {
                if let Some((_, of, _)) = follow {
                    truth.lost.insert(of);
                }
                let (generation, _) = relay.begin("model_x", "m", epoch);
                follow = Some((generation, epoch, false));
                format!("follow {generation} of {epoch} begins")
            }
            // The follow installs, at the epoch of its record.
            4 => match follow {
                Some((generation, of, false)) => {
                    relay.install(generation, installed_at(of, PoolState::Ready));
                    follow = Some((generation, of, true));
                    truth.lost.remove(&of);
                    format!("follow {generation} of {of} installs")
                }
                _ => "nothing to install".into(),
            },
            // The installed follow's link says it drains.
            5 => match follow {
                Some((generation, of, true)) => {
                    relay.apply(
                        "model_x",
                        "m",
                        generation,
                        PoolWatchEvent::State {
                            state: PoolState::Draining,
                            unfollowed: false,
                            retire: None,
                        },
                    );
                    truth.drained.insert(of);
                    format!("follow {generation} of {of} drains")
                }
                _ => "nothing to drain".into(),
            },
            // The follow's link ends, installed or not.
            6 => match follow.take() {
                Some((generation, of, installed)) => {
                    relay.end("model_x", "m", generation);
                    truth.lost.insert(of);
                    format!("follow {generation} of {of} ends, installed {installed}")
                }
                None => "no follow to end".into(),
            },
            // The pool withdraws, by its hub or an operator past it.
            7 if truth.newest.is_some() && rng.below(3) == 0 => {
                let at = epoch + rng.below(2);
                relay.withdrawn(&Withdrawal {
                    class: "model_x".into(),
                    pool: "m".into(),
                    epoch: at,
                    seq: seq + 1,
                    properties_version: 0,
                });
                epoch = at;
                withdrawn = true;
                truth = Truth {
                    drained: std::mem::take(&mut truth.drained),
                    ..Truth::default()
                };
                format!("withdrawn at {at}")
            }
            _ => "nothing".into(),
        };
        trace.push(what);
        if let Err(broke) = check(&truth, listing(&relay)) {
            return Err((trace, broke));
        }
    }
    Ok(())
}

fn seeds() -> Vec<u64> {
    if let Some(seed) = std::env::var("GDL_EPOCH_GUARD_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
    {
        return vec![seed];
    }
    let count = std::env::var("GDL_EPOCH_GUARD_SEEDS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(SEEDS);
    (0..count).collect()
}

/// The guard, over a fixed set of seeds; see the module's documentation.
#[test]
fn every_listing_pairs_its_condition_with_its_hub() {
    for seed in seeds() {
        if let Err((trace, broke)) = run(seed, STEPS) {
            let steps: Vec<String> = trace
                .iter()
                .enumerate()
                .map(|(step, what)| format!("{step:4}: {what}"))
                .collect();
            panic!(
                "seed {seed}: {broke}\nsteps:\n{}\nrerun with GDL_EPOCH_GUARD_SEED={seed}",
                steps.join("\n")
            );
        }
    }
}

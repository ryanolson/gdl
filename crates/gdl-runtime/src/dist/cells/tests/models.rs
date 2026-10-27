//! The model index a chooser keeps for each class it sends to: every cell
//! its hub lists, followed or not, with each model's state, the cell's
//! condition, and whether this chooser offers it work. A node reads it
//! with `ClassCells::models`, and a cell policy with
//! `CellRequest::models`, from the same table as the cells. Until the
//! hub's snapshot says its listings are whole, the index is not synced,
//! so a reader tells a model not known yet from one no cell lists.

use std::sync::Arc;

use gdl::{CellCondition, CellState, FollowedCells, ModelState};
use gdl_cells::{LoadSummary, ModelEntry};
use gdl_watch::{FollowEvent, Listing, ModelList};
use tokio::time::Instant;

use super::super::Cells;
use super::{M1, M2, follow, follower};
use crate::dist::counting::{counted, counting};

fn listing(condition: CellCondition, models: &[(&str, ModelState)], version: u64) -> Listing {
    Listing {
        region: "eu-west-1".into(),
        condition,
        models: ModelList {
            entries: models
                .iter()
                .map(|(model, state)| ModelEntry {
                    model: (*model).into(),
                    routing_key: None,
                    state: *state,
                    since: version,
                })
                .collect(),
            version,
            revoked: Vec::new(),
        },
    }
}

fn listed(cell: &str, listing: Option<Listing>) -> FollowEvent {
    FollowEvent::Listed {
        class: "model_x".into(),
        cell: cell.into(),
        generation: 0,
        listing,
    }
}

fn handle(cells: &Cells) -> FollowedCells {
    let handle = FollowedCells::new();
    assert!(handle.bind(Arc::new(cells.clone())), "bound once");
    handle
}

/// What a node reads of `model`: whether the index is synced, and each
/// cell that lists it, with the model's state, the cell's condition, and
/// whether it is offered.
fn sets(
    node: &FollowedCells,
    model: &str,
) -> (bool, Vec<(String, ModelState, CellCondition, bool)>) {
    node.read("model_x", |cells| {
        let index = cells.models();
        let sets = index
            .sets(model, None)
            .unwrap_or_default()
            .iter()
            .map(|set| {
                (
                    set.cell().to_string(),
                    set.state(),
                    set.condition(),
                    set.is_offered(),
                )
            })
            .collect();
        (index.is_synced(), sets)
    })
    .expect("the chooser sends to model_x")
}

/// A snapshot's listings make the index: a followed cell's from its
/// follow, another's from its own event, the marker's flag whether it is
/// synced. Before any snapshot the index is not synced; after one that
/// says the hub's listings are whole, a model no cell lists is unknown.
#[test]
fn a_snapshot_makes_the_index_and_says_whether_it_is_synced() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    assert_eq!(sets(&node, "qwen-32b"), (false, vec![]), "no snapshot yet");

    let mut snapshot = follows.snapshot();
    let mut first = follow(M1, 1, CellState::Ready);
    if let FollowEvent::Follow { listing: shown, .. } = &mut first {
        *shown = Some(listing(
            CellCondition::Live,
            &[("qwen-32b", ModelState::Ready)],
            3,
        ));
    }
    snapshot.apply(first, now);
    snapshot.apply(
        listed(
            M2,
            Some(listing(
                CellCondition::Missing,
                &[
                    ("qwen-32b", ModelState::Ready),
                    ("llama-70b", ModelState::Unavailable),
                ],
                5,
            )),
        ),
        now,
    );
    snapshot.apply(FollowEvent::Synced { listed: false }, now);
    follows.resnapshot(snapshot, now);
    let (synced, qwen) = sets(&node, "qwen-32b");
    assert!(!synced, "the hub's listings were not whole");
    assert_eq!(
        qwen,
        [
            (M1.to_string(), ModelState::Ready, CellCondition::Live, true),
            (
                M2.to_string(),
                ModelState::Ready,
                CellCondition::Missing,
                false
            ),
        ]
    );

    let mut snapshot = follows.snapshot();
    snapshot.apply(follow(M1, 1, CellState::Ready), now);
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    assert_eq!(
        sets(&node, "qwen-32b"),
        (true, vec![]),
        "a snapshot holds every listing: one it does not hold is gone"
    );
    assert_eq!(sets(&node, "mistral"), (true, vec![]), "unknown");
}

/// A model that moves: cell A drains it before cell B is `ready`. The
/// index shows A's `drained` and B's `starting`, so a gate answers 503,
/// not 404, through the move.
#[test]
fn a_model_that_moves_shows_both_cells() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    let mut snapshot = follows.snapshot();
    snapshot.apply(FollowEvent::Synced { listed: true }, now);
    follows.resnapshot(snapshot, now);
    follows.apply(
        listed(
            M1,
            Some(listing(
                CellCondition::Live,
                &[("qwen-32b", ModelState::Draining)],
                2,
            )),
        ),
        now,
    );
    follows.apply(
        listed(
            M1,
            Some(listing(
                CellCondition::Live,
                &[("qwen-32b", ModelState::Drained)],
                3,
            )),
        ),
        now,
    );
    follows.apply(
        listed(
            M2,
            Some(listing(
                CellCondition::Live,
                &[("qwen-32b", ModelState::Starting)],
                1,
            )),
        ),
        now,
    );
    let (synced, qwen) = sets(&node, "qwen-32b");
    assert!(synced);
    assert_eq!(
        qwen,
        [
            (
                M1.to_string(),
                ModelState::Drained,
                CellCondition::Live,
                false
            ),
            (
                M2.to_string(),
                ModelState::Starting,
                CellCondition::Live,
                false
            ),
        ]
    );
    follows.apply(listed(M1, None), now);
    assert_eq!(
        sets(&node, "qwen-32b").1,
        [(
            M2.to_string(),
            ModelState::Starting,
            CellCondition::Live,
            false
        )],
        "a cell that withdrew is no longer listed"
    );
}

/// The index is made again when what it lists or what the chooser offers
/// changes, not when a cell's summary does, which a table is stored for
/// every period; a cell policy reads the same index as a node.
#[test]
fn the_index_is_made_again_only_when_it_may_have_changed() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Ready), now);
    follows.apply(
        listed(
            M1,
            Some(listing(
                CellCondition::Live,
                &[("qwen-32b", ModelState::Ready)],
                1,
            )),
        ),
        now,
    );
    let index = || Arc::clone(&cells.table("f").expect("a table").models);
    let before = index();
    assert!(
        before
            .sets("qwen-32b", None)
            .is_some_and(|sets| sets[0].is_offered())
    );
    follows.apply(
        FollowEvent::Load {
            class: "model_x".into(),
            cell: M1.into(),
            generation: 1,
            load: Some(LoadSummary::default()),
        },
        now,
    );
    assert!(Arc::ptr_eq(&before, &index()), "a summary changes no index");
    follows.apply(
        FollowEvent::Offering {
            class: "model_x".into(),
            cell: M1.into(),
            generation: 1,
            revision: 2,
            offered: false,
        },
        now,
    );
    let after = index();
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(
        after
            .sets("qwen-32b", None)
            .is_some_and(|sets| !sets[0].is_offered())
    );
}

/// A listing the relay sends while the cell is read from a watch of its
/// own is still applied: no watch of one follow carries one.
#[test]
fn a_listing_reaches_a_cell_read_from_its_own_watch() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Ready), now);
    follows.unknown(gdl_watch::UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: M1.into(),
        generation: 1,
    });
    assert!(!follows.asked().is_empty(), "read from a watch of its own");
    follows.take(
        listed(
            M1,
            Some(listing(
                CellCondition::Live,
                &[("qwen-32b", ModelState::Ready)],
                1,
            )),
        ),
        now,
    );
    assert_eq!(
        sets(&node, "qwen-32b").1,
        [(
            M1.to_string(),
            ModelState::Ready,
            CellCondition::Live,
            false
        )],
        "listed, and not offered while unread"
    );
}

/// Looking a model up in the index allocates nothing.
#[test]
fn a_model_lookup_allocates_nothing() {
    let (mut follows, cells, _, _) = follower();
    let node = handle(&cells);
    let now = Instant::now();
    follows.apply(follow(M1, 1, CellState::Ready), now);
    follows.apply(
        listed(
            M1,
            Some(listing(
                CellCondition::Live,
                &[
                    ("qwen-32b", ModelState::Ready),
                    ("llama-70b", ModelState::Starting),
                ],
                1,
            )),
        ),
        now,
    );
    // Once, uncounted: the first load of a thread may take arc-swap's
    // per-thread slot.
    let _ = sets(&node, "qwen-32b");
    counting(true);
    let before = counted();
    let mut found = 0;
    for _ in 0..100 {
        found += node
            .read("model_x", |cells| {
                let index = cells.models();
                usize::from(
                    index.sets("qwen-32b", None).is_some_and(|sets| {
                        sets.iter().any(|set| {
                            set.is_offered()
                                && cells.cell(set.cell()).is_some_and(gdl::Cell::takes_work)
                        })
                    }) || index.sets("mistral", None).is_none(),
                )
            })
            .unwrap_or(0);
    }
    let allocations = counted() - before;
    counting(false);
    assert_eq!(found, 100);
    assert_eq!(allocations, 0, "a lookup allocated");
}

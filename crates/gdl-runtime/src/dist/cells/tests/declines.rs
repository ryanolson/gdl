use gdl_cells::CellState;
use tokio::time::Instant;

use super::super::cell_id;
use super::super::lease::CellLease;
use super::{M1, exposed, follow, follower, live, peer, table};

/// A reply that reads a table without its cell says nothing about a later follow.
#[test]
fn a_full_reply_with_no_shown_cell_preserves_a_newer_gone_mark() {
    let (mut follows, cells, _, _) = follower();
    let now = Instant::now();
    let x = peer();
    live(
        &mut follows,
        now,
        [follow(M1, 1, CellState::Ready), exposed(M1, 1, &x)],
    );
    let before = cells.table("f").expect("the offer's table");
    let _lease = CellLease::take(&before.cells[0].leases).expect("offered");
    let cell = cell_id("model_x", M1);

    follows.resnapshot(follows.snapshot(), now);
    let absent = cells.table("f").expect("the empty table");
    assert!(absent.cells.iter().all(|shown| shown.id != cell));

    // The full reply pauses after its table load. A new follow then says gone.
    live(
        &mut follows,
        now,
        [follow(M1, 2, CellState::Ready), exposed(M1, 2, &x)],
    );
    let current = cells.table("f").expect("the new follow's table");
    cells.damping().gone(&current.views[0]);
    cells.damping().decline_shown(&before.views[0], now);
    assert!(table(&cells, M1).expect("followed").gone, "the control");

    // Resume Crossing::declined's None arm with the table it already loaded.
    let shown = absent
        .cells
        .iter()
        .zip(&absent.views)
        .find(|(shown, _)| shown.id == cell);
    match shown {
        Some((_, view)) => cells.damping().decline_shown(view, now),
        None => cells.damping().decline(cell, now),
    }
    let shown = table(&cells, M1).expect("followed");
    assert!(
        shown.gone,
        "a full reply that read no cell erased the new follow's gone mark: {shown:?}"
    );
}

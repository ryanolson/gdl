//! A chooser follows the cells its hub follows: one watch of its hub's
//! relay, read into a table per entry segment and followed cell, whose
//! instances it registers with velo before they are in a table.

mod common;

use common::WAIT;
use common::cells::{
    Instance, Resolving, cell_hub, followed, global_cell, global_hub, launch, model_hub, pings,
    settings, sorted, unresolved_cell_hub, until,
};
use common::proxy::{Mode, Proxy};
use futures::StreamExt;
use gdl_model::SegmentPhase;
use gdl_runtime::CellState;
use gdl_watch::FollowEvent;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// A global cell's chooser, with two home model cells ready, holds a table
/// per cell with each cell's ready entry instances; a retired one is
/// marked, one that leaves is removed, a cell whose hub stops is removed,
/// and the cell's new hub process is a new generation's table with
/// nothing of the old one in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tables_follow_the_relay() {
    let directory = Resolving::new();
    let (global, _) = cell_hub(global_hub(), &directory).await;
    let (m1, _) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (m2, _) = cell_hub(model_hub(M2, "m2-hub:1337"), &directory).await;
    let (f1, f1b, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let (f2, _d2) = (
        Instance::ready(&m2, "f").await,
        Instance::ready(&m2, "d").await,
    );
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &global_cell(),
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let cells = chooser
        .cells()
        .expect("a global instance in a cell is a chooser");
    assert_eq!(cells.segments().collect::<Vec<_>>(), ["f"]);
    // A cell is followed from when its quorum is met, `degraded` until the
    // quorum has held.
    until(
        &cells,
        "both model cells ready with their entries",
        |cells| {
            let ready = |cell, eligible: &[_]| {
                followed(cells, "f", cell)
                    .is_some_and(|m| m.state == CellState::Ready && m.eligible == eligible)
            };
            ready(M1, &sorted(&[&f1, &f1b])) && ready(M2, &[f2.id()])
        },
    )
    .await;
    let tables = cells.followed("f");
    assert_eq!(
        tables.iter().map(|m| m.cell.as_str()).collect::<Vec<_>>(),
        [M1, M2]
    );
    for table in &tables {
        assert_eq!(table.class, "model_x");
        assert_eq!(table.state, CellState::Ready);
        assert!(table.retired.is_empty());
    }

    let mut f1 = f1;
    f1.announce(SegmentPhase::Retired).await;
    until(&cells, "the retired entry marked", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.retired == [f1.id()] && m.eligible == [f1b.id()])
    })
    .await;
    f1.leave().await;
    until(&cells, "the entry that left removed", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.retired.is_empty() && m.eligible == [f1b.id()])
    })
    .await;

    let old = followed(&cells, "f", M2).expect("followed");
    m2.shutdown().await.expect("valid test fixture");
    until(&cells, "the stopped cell's table removed", |cells| {
        followed(cells, "f", M2).is_none()
    })
    .await;
    // The cell's hub starts again: a new process, so a higher epoch, at a
    // new address, with new instances.
    let (m2, _) = cell_hub(model_hub(M2, "m2-hub-2:1337"), &directory).await;
    let (f2b, _d2b) = (
        Instance::ready(&m2, "f").await,
        Instance::ready(&m2, "d").await,
    );
    until(&cells, "the new process's table", |cells| {
        let table = followed(cells, "f", M2);
        assert!(
            table
                .as_ref()
                .is_none_or(|m| !m.eligible.contains(&f2.id()) && !m.retired.contains(&f2.id())),
            "an instance of the old generation came back: {table:?}"
        );
        table.is_some_and(|m| m.eligible == [f2b.id()])
    })
    .await;
    let new = followed(&cells, "f", M2).expect("followed");
    assert!(new.generation > old.generation, "{old:?} then {new:?}");
    drop(chooser);
    for hub in [global, m1, m2] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry instance in a table is reachable by its instance id from the
/// chooser's node; one that leaves is out of the table at once, still
/// registered until `drain_ms` after, then forgotten. The drain is 2 s, so
/// that the test sees the registration outlive the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_is_registered_with_velo_while_a_table_holds_it() {
    let directory = Resolving::new();
    let (global, _) = cell_hub(global_hub(), &directory).await;
    let (m1, _) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (f1, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let extra = format!("{}[runtime]\ndrain_ms = 2000\n", global_cell());
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &extra,
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let node = chooser.node();
    let cells = chooser.cells().expect("a chooser");
    until(&cells, "the entry in the table", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.eligible == [f1.id()])
    })
    .await;
    let id = f1.id();
    assert!(cells.is_registered(id));
    assert!(
        pings(&node, id).await,
        "an entry in a table is not reachable"
    );

    let _kept = f1.leave().await;
    until(&cells, "the entry out of the table", |cells| {
        followed(cells, "f", M1).is_none_or(|m| m.eligible.is_empty())
    })
    .await;
    assert!(
        cells.is_registered(id),
        "unregistered as it left the table, before the drain"
    );
    until(&cells, "the entry unregistered", |cells| {
        !cells.is_registered(id)
    })
    .await;
    assert!(!pings(&node, id).await, "velo still knows the entry");
    drop(chooser);
    for hub in [global, m1] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// One velo instance that is a member of the chooser's own hub and an
/// entry of a followed cell has two owners of its registration: when it
/// leaves the chooser's hub, the membership's drain does not unregister
/// what a cell's table still holds. A second instance, a member of the
/// chooser's hub alone, leaves after it, and its unregistration shows that
/// the membership's drain of the first has run. The drain is 500 ms, and
/// the membership looks at its drains once a second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_membership_does_not_unregister_what_a_table_holds() {
    let directory = Resolving::new();
    let (global, _) = cell_hub(global_hub(), &directory).await;
    let (m1, _) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (f1, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let extra = format!("{}[runtime]\ndrain_ms = 500\n", global_cell());
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &extra,
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let node = chooser.node();
    let cells = chooser.cells().expect("a chooser");
    until(&cells, "the entry in the table", |cells| {
        followed(cells, "f", M1)
            .is_some_and(|m| m.state == CellState::Ready && m.eligible == [f1.id()])
    })
    .await;
    // The same instance joins the chooser's own hub, as does one that is
    // only there; the chooser's membership follows both.
    let mut local = f1.also(&global, "f").await;
    let mut witness = Instance::join(&global, "f").await;
    for member in [&mut local, &mut witness] {
        member.announce(SegmentPhase::Serving).await;
        member.announce(SegmentPhase::Ready).await;
    }
    // The membership registers what is ready in `gdl-ready-f`, one watch,
    // in order: once the witness is reachable, it has taken the entry too.
    let deadline = tokio::time::Instant::now() + WAIT;
    while !pings(&node, witness.id()).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the membership never registered the witness"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let witness_id = witness.id();
    local.leave().await;
    let _witness_node = witness.leave().await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while pings(&node, witness_id).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the membership never unregistered an instance that left"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        followed(&cells, "f", M1).is_some_and(|m| m.eligible == [f1.id()]),
        "the cell's table changed"
    );
    assert!(cells.is_registered(f1.id()));
    assert!(
        pings(&node, f1.id()).await,
        "the membership unregistered an entry a cell's table holds"
    );
    drop(chooser);
    for hub in [global, m1] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// An entry that leaves its cell's table, then the chooser's own hub,
/// stays registered past the chooser's drain until the membership's own
/// drain, which began later, ends. The drain is 1 s, and the entry leaves
/// the hub half a second after the table, so that the two drains end apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_membership_drain_keeps_its_peer() {
    let directory = Resolving::new();
    let (global, _) = cell_hub(global_hub(), &directory).await;
    let (m1, _) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (f1, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let extra = format!("{}[runtime]\ndrain_ms = 1000\n", global_cell());
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &extra,
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let node = chooser.node();
    let cells = chooser.cells().expect("a chooser");
    until(&cells, "the entry in the table", |cells| {
        followed(cells, "f", M1)
            .is_some_and(|m| m.state == CellState::Ready && m.eligible == [f1.id()])
    })
    .await;
    let id = f1.id();
    let mut local = f1.also(&global, "f").await;
    local.announce(SegmentPhase::Serving).await;
    local.announce(SegmentPhase::Ready).await;
    let _node = f1.leave().await;
    until(&cells, "the entry out of the table", |cells| {
        followed(cells, "f", M1).is_none_or(|m| m.eligible.is_empty())
    })
    .await;
    // The membership's drain begins after the chooser's.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    local.leave().await;
    until(&cells, "the chooser's drain over", |cells| {
        !cells.is_registered(id)
    })
    .await;
    assert!(
        pings(&node, id).await,
        "the chooser's drain cut the membership's short"
    );
    drop(chooser);
    for hub in [global, m1] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// When the hub's follow of a cell is cut and followed again, the new
/// generation holds an entry the old one held, and the drain the old one's
/// end started does not unregister it; an entry only the old generation
/// held is unregistered at that drain's end. The drain is 3 s, so that
/// the refollow is seen to come before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_newer_generation_keeps_an_entry_registered() {
    let directory = Resolving::new();
    let (global, _) = cell_hub(global_hub(), &directory).await;
    // The global hub reaches M1's hub through a proxy, so that the test can
    // cut the follow; M1 counts its own instances over its own listener.
    let (m1, _) = unresolved_cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let link = Proxy::new(m1.public_addr()).await;
    directory.resolve("m1-hub:1337", &format!("127.0.0.1:{}", link.port));
    let (f1, fx, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let extra = format!("{}[runtime]\ndrain_ms = 3000\n", global_cell());
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &extra,
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let node = chooser.node();
    let cells = chooser.cells().expect("a chooser");
    until(&cells, "both entries", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.eligible == sorted(&[&f1, &fx]))
    })
    .await;
    let first = followed(&cells, "f", M1).expect("followed").generation;

    link.set(Mode::Refuse);
    until(&cells, "the cut follow dropped", |cells| {
        followed(cells, "f", M1).is_none()
    })
    .await;
    let gone = fx.id();
    let _kept = fx.leave().await;
    link.set(Mode::Forward);
    until(&cells, "the new generation", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.generation > first && m.eligible == [f1.id()])
    })
    .await;
    assert!(
        cells.is_registered(gone),
        "the drain the old generation's end started was over before the refollow"
    );
    until(&cells, "the drain's end", |cells| {
        !cells.is_registered(gone)
    })
    .await;
    assert!(
        cells.is_registered(f1.id()),
        "the old generation's drain unregistered an entry the new one holds"
    );
    assert!(pings(&node, f1.id()).await);
    drop(chooser);
    for hub in [global, m1] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A chooser whose relay watch is cut keeps its tables, watches again,
/// and from the fresh snapshot holds what the hub's relay holds, including
/// what changed while it was cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_relay_watch_is_watched_again_from_a_snapshot() {
    let directory = Resolving::new();
    let (global, relay) = cell_hub(global_hub(), &directory).await;
    let (m1, _) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (m2, _) = cell_hub(model_hub(M2, "m2-hub:1337"), &directory).await;
    let (f1, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let (f2, _d2) = (
        Instance::ready(&m2, "f").await,
        Instance::ready(&m2, "d").await,
    );
    // The chooser reaches its hub's public listener through a proxy, so
    // that the test can cut its relay watch; its lease is kept on the
    // control port.
    let public = Proxy::new(global.public_addr()).await;
    let config = settings(
        public.port,
        global.control_addr().port(),
        5_000,
        &global_cell(),
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let cells = chooser.cells().expect("a chooser");
    until(&cells, "both cells", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.eligible == [f1.id()])
            && followed(cells, "f", M2).is_some_and(|m| m.eligible == [f2.id()])
    })
    .await;
    let mut watchers = relay.follows_watchers();
    let before = cells.followed("f");

    public.set(Mode::Refuse);
    tokio::time::timeout(WAIT, watchers.wait_for(|watching| *watching == 0))
        .await
        .expect("the relay watch was cut within the wait")
        .expect("the hub lives");
    // While the watch is down, M1 gains an entry the chooser cannot hear of.
    let f1b = Instance::ready(&m1, "f").await;
    assert_eq!(
        cells.followed("f"),
        before,
        "a cut watch changed the tables"
    );
    for table in &before {
        for id in &table.eligible {
            assert!(cells.is_registered(*id));
        }
    }

    public.set(Mode::Forward);
    until(&cells, "the change made while cut", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.eligible == sorted(&[&f1, &f1b]))
    })
    .await;
    let after = cells.followed("f");
    assert_eq!(after, expected(&mut relay.follows()).await);
    let m2_after = after.iter().find(|m| m.cell == M2).expect("M2 kept");
    let m2_before = before.iter().find(|m| m.cell == M2).expect("M2 held");
    assert_eq!(
        m2_after, m2_before,
        "a refollow was made of a cell that never changed"
    );
    drop(chooser);
    for hub in [global, m1, m2] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The tables a relay watch's snapshot makes, as a chooser of `f` keeps
/// them.
async fn expected(watch: &mut gdl_hub::FollowsWatch) -> Vec<gdl_runtime::FollowedCell> {
    let mut tables: Vec<gdl_runtime::FollowedCell> = Vec::new();
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("the snapshot within the wait")
            .expect("the relay lives");
        match event {
            FollowEvent::Synced => break,
            FollowEvent::Follow {
                class,
                cell,
                generation,
                region,
                state,
                ..
            } => tables.push(gdl_runtime::FollowedCell {
                class,
                cell,
                generation,
                region,
                state,
                eligible: Vec::new(),
                retired: Vec::new(),
            }),
            FollowEvent::Exposed { cell, peer, .. } => {
                let table = tables
                    .iter_mut()
                    .find(|m| m.cell == cell)
                    .expect("followed");
                table.eligible.push(peer.instance_id());
                table.eligible.sort_by_key(|id| id.as_u128());
            }
            FollowEvent::Retired { cell, instance, .. } => {
                let table = tables
                    .iter_mut()
                    .find(|m| m.cell == cell)
                    .expect("followed");
                table.retired.push(instance);
                table.retired.sort_by_key(|id| id.as_u128());
            }
            other => panic!("not a snapshot's event: {other:?}"),
        }
    }
    tables.sort_by(|a, b| (&a.class, &a.cell).cmp(&(&b.class, &b.cell)));
    tables
}

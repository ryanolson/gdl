//! A cell's drain at its hub: `POST /v1/ext/gdl-cell/drain` on the control
//! listener makes the cell `draining` whatever its quorum, on its health
//! endpoint, its record, and its watch at once; once the last of its
//! instances has left the hub, the hub withdraws the cell's record and ends
//! its watches. Every hub runs in process on OS-assigned ports, and every
//! wait is for something observed.

mod common;

use common::{
    CELL, GLOBAL, Instance, Records, WAIT, cell_hub, get, global_requiring, health, hub_of,
    meet_the_quorum, timing, usable,
};
use futures::{FutureExt, StreamExt};
use gdl_cells::{CellDirectory, CellEvent, CellState, MemoryDirectory, MemoryWatch};
use gdl_hub::{CellHealth, CellWatch};
use gdl_watch::CellWatchEvent;
use velo_hub::Hub;

/// Ask the hub to drain its cell: the status and the body.
async fn drain(hub: &Hub) -> (u16, String) {
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-cell/drain",
        hub.control_addr().port()
    );
    let response = tokio::time::timeout(WAIT, reqwest::Client::new().post(&url).send())
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    let status = response.status().as_u16();
    (status, response.text().await.expect("a body"))
}

/// A ready cell that is drained is `draining` on its health endpoint
/// (503), its watch, and its record by the time the drain is answered;
/// a second drain changes nothing, and a quorum that changes afterwards
/// leaves it `draining`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_cell_is_draining_at_once_whatever_its_quorum() {
    let directory = MemoryDirectory::new();
    let mut records = Records::new(&directory);
    let (hub, cell) = cell_hub(directory.clone(), timing()).await;
    let (mut instances, _) = meet_the_quorum(&hub).await;
    records.until(CellState::Ready).await;
    let mut watch = cell.watch().await.expect("the watch begins");
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
            .await
            .expect("the snapshot in time")
            .expect("the watch goes on");
        if event == CellWatchEvent::Synced {
            break;
        }
    }

    let (status, body) = drain(&hub).await;
    assert_eq!(status, 202, "{body}");
    let answered: CellHealth = serde_json::from_str(&body).expect("the cell's health");
    assert_eq!(answered.state, CellState::Draining);
    let (status, body) = get(&hub, "/v1/ext/gdl-cell/health").await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(
        watch.next().now_or_never(),
        Some(Some(CellWatchEvent::State {
            state: CellState::Draining
        })),
        "the watch did not say draining by the time the drain was answered"
    );
    let (record, _) = records.until(CellState::Draining).await;

    let (status, body) = drain(&hub).await;
    assert_eq!(status, 202, "a second drain: {body}");
    let instance = instances.pop().expect("an instance of d");
    instance.leave().await;
    let counted = health(&cell, "d at 1", |health| usable(health, "d") == 1).await;
    assert_eq!(
        counted.state,
        CellState::Draining,
        "the quorum ended the drain"
    );
    assert_eq!(
        watch.next().now_or_never(),
        None,
        "a second drain or a quorum lost changed the state"
    );
    assert!(
        records.next().now_or_never().is_none(),
        "the record was written again after {record:?}"
    );
    hub.shutdown().await.expect("the hub stops");
}

/// A cell whose class no hop enters, a global cell, cannot be drained as
/// a cell yet: the drain is refused, and the cell's state is left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_whose_class_no_hop_enters_is_not_drained() {
    let directory = MemoryDirectory::new();
    let mut records = Records::of(&directory, "global", GLOBAL);
    let (hub, cell) = hub_of(global_requiring(0), directory.clone(), timing()).await;
    let mut instance = Instance::join(&hub, "a").await;
    instance.announce(gdl_model::SegmentPhase::Ready).await;
    records.until(CellState::Ready).await;

    let (status, body) = drain(&hub).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(cell.health().borrow().state, CellState::Ready);
    hub.shutdown().await.expect("the hub stops");
}

/// The next event of the model cell `CELL` in `watch`, a directory watch
/// of its class: a record, or its withdrawal.
async fn next_of_cell(watch: &mut MemoryWatch) -> CellEvent {
    let next = async {
        loop {
            match watch.next().await {
                Some(Ok(
                    event @ (CellEvent::Current(_)
                    | CellEvent::Put { .. }
                    | CellEvent::Withdrawn { .. }),
                )) if named(&event) == CELL => return event,
                Some(Ok(_)) => {}
                other => panic!("the directory's watch ended: {other:?}"),
            }
        }
    };
    tokio::time::timeout(WAIT, next)
        .await
        .expect("timed out waiting for the cell in the directory")
}

fn named(event: &CellEvent) -> &str {
    match event {
        CellEvent::Current(record) | CellEvent::Put { record, .. } => &record.name,
        CellEvent::Withdrawn { withdrawal, .. } => &withdrawal.cell,
        _ => "",
    }
}

/// The watch's next event, within [`WAIT`]: `None` once it has ended.
async fn after(watch: &mut CellWatch) -> Option<CellWatchEvent> {
    tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("the watch moves on in time")
}

/// A draining cell's record stays while any of its instances is a member
/// of the hub, and is withdrawn, after the last write of it, once the last
/// one leaves; its watches end then, and a new one is refused. The health
/// endpoint still says the cell is draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_cell_is_withdrawn_once_its_last_instance_leaves() {
    let directory = MemoryDirectory::new();
    let mut directory_watch = directory.watch("model_x", None);
    let (hub, cell) = cell_hub(directory.clone(), timing()).await;
    let (mut instances, _) = meet_the_quorum(&hub).await;
    let mut watch = cell.watch().await.expect("the watch begins");
    while after(&mut watch).await != Some(CellWatchEvent::Synced) {}

    cell.drain().await.expect("the cell drains");
    let draining = loop {
        match next_of_cell(&mut directory_watch).await {
            CellEvent::Current(record) | CellEvent::Put { record, .. }
                if record.state == CellState::Draining =>
            {
                break record;
            }
            CellEvent::Withdrawn { .. } => panic!("withdrawn while its instances are members"),
            _ => {}
        }
    };
    let last = instances.pop().expect("an instance");
    for instance in instances {
        instance.leave().await;
    }
    assert!(
        directory_watch.next().now_or_never().is_none(),
        "the record changed while an instance is a member"
    );

    last.leave().await;
    match next_of_cell(&mut directory_watch).await {
        CellEvent::Withdrawn { withdrawal, .. } => {
            assert_eq!(withdrawal.epoch, draining.epoch);
            assert!(
                withdrawal.seq > draining.seq,
                "the withdrawal is not after the last write: {withdrawal:?} {draining:?}"
            );
        }
        other => panic!("not withdrawn: {other:?}"),
    }
    loop {
        match after(&mut watch).await {
            None => break,
            Some(CellWatchEvent::Left { .. } | CellWatchEvent::Load { .. }) => {}
            Some(other) => panic!("the watch went on with {other:?}"),
        }
    }
    assert!(
        cell.watch().await.is_err(),
        "a watch of a withdrawn cell began"
    );
    let (status, body) = get(&hub, "/v1/ext/gdl-cell/health").await;
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("draining"), "{body}");
    hub.shutdown().await.expect("the hub stops");
}

/// A cell drained with no instance at its hub is withdrawn at once, after
/// its record has said it is draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_cell_with_no_instance_is_withdrawn_at_once() {
    let directory = MemoryDirectory::new();
    let mut directory_watch = directory.watch("model_x", None);
    let (hub, cell) = cell_hub(directory.clone(), timing()).await;
    let mut records = Records::new(&directory);
    records.until(CellState::Unavailable).await;

    cell.drain().await.expect("the cell drains");
    let mut seen = Vec::new();
    loop {
        let event = next_of_cell(&mut directory_watch).await;
        let withdrawn = matches!(event, CellEvent::Withdrawn { .. });
        seen.push(match event {
            CellEvent::Current(record) | CellEvent::Put { record, .. } => Some(record.state),
            _ => None,
        });
        if withdrawn {
            break;
        }
    }
    assert_eq!(
        seen.iter().rev().nth(1),
        Some(&Some(CellState::Draining)),
        "not draining before it was withdrawn: {seen:?}"
    );
    hub.shutdown().await.expect("the hub stops");
}

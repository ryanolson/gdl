//! An instance in a cell: its launch stops at its class's boundary.

mod common;

use common::cells::{admission_hub, global_cell, launch, model_cell, settings};
use common::{Idle, WAIT};

/// A global instance in a cell launches and serves with no model cell
/// running at all: it waits for no instance of another class, at its
/// rendezvous or at its routing barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_instance_in_a_cell_serves_with_no_model_cell() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let config = settings(public, control, 2_000, &global_cell());
    let (_process, launched) = tokio::time::timeout(WAIT, launch(&config, "a"))
        .await
        .expect("the launch ended within the wait")
        .expect("a global instance in a cell launched without a model cell");
    tokio::time::timeout(WAIT, launched.serve(Idle, |_, _| async { Ok(()) }))
        .await
        .expect("the barrier passed within the wait")
        .expect("served");
    hub.shutdown().await.expect("valid test fixture");
}

/// A model cell's instances launch and serve with no global instance
/// running: they wait for each other, the segments of their own class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_model_cell_serves_with_no_global_instance() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let config = settings(public, control, 2_000, &model_cell("model-x-01-us-east-1"));
    let (entry, interior) = tokio::time::timeout(WAIT, async {
        tokio::join!(launch(&config, "f"), launch(&config, "d"))
    })
    .await
    .expect("the launches ended within the wait");
    let (_entry_process, entry) = entry.expect("the entry launched without a global instance");
    let (_interior_process, interior) = interior.expect("the interior launched");
    assert_eq!(
        entry.roster().instances("d").len(),
        1,
        "the entry met the interior"
    );
    let (entry, interior) = tokio::time::timeout(WAIT, async {
        tokio::join!(
            entry.serve(Idle, |_, _| async { Ok(()) }),
            interior.serve(Idle, |_, _| async { Ok(()) }),
        )
    })
    .await
    .expect("the barriers passed within the wait");
    entry.expect("the entry served");
    interior.expect("the interior served");
    hub.shutdown().await.expect("valid test fixture");
}

/// An instance without a cell waits, as it always has, for an instance of
/// every segment, whatever its class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_without_a_cell_waits_for_every_segment() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let config = settings(public, control, 500, "");
    let Err(error) = tokio::time::timeout(WAIT, launch(&config, "a"))
        .await
        .expect("the launch ended within the wait")
    else {
        panic!("an instance without a cell launched with no model instance");
    };
    let error = error.to_string();
    assert!(
        error.contains("f (0 of 1)") && error.contains("d (0 of 1)"),
        "{error}"
    );
    hub.shutdown().await.expect("valid test fixture");
}

/// With a cell, `[min_replicas]` names only the segments of the cell's
/// class: an instance waits for no other, so a count for one would be
/// ignored, and is refused instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_refuses_a_count_of_another_class() {
    let hub = admission_hub().await;
    let (public, control) = (hub.public_addr().port(), hub.control_addr().port());
    let extra = format!("{}[min_replicas]\nf = 2\n", global_cell());
    let config = settings(public, control, 500, &extra);
    let Err(error) = tokio::time::timeout(WAIT, launch(&config, "a"))
        .await
        .expect("the launch ended within the wait")
    else {
        panic!("a count of another class's segment was taken");
    };
    let error = error.to_string();
    assert!(
        error.contains("[min_replicas]") && error.contains("`f`") && error.contains("model_x"),
        "{error}"
    );
    hub.shutdown().await.expect("valid test fixture");
}

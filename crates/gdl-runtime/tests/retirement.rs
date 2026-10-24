//! A model cell's entry instance that retires is acknowledged by its
//! parent: each chooser of the global cell that follows the cell avoids it
//! once its tables hold it retired, and says so to its own hub; the global
//! cell's hub, once every chooser has, tells the model cell's hub, which
//! says so on the cell's watch.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed.

mod common;

use common::WAIT;
use common::cells::{
    Instance, Resolving, cell_hub, followed, global_cell, global_hub, launch, model_hub, settings,
    sorted, until,
};
use futures::StreamExt;
use gdl_model::SegmentPhase;
use gdl_runtime::CellState;
use gdl_runtime::velo::InstanceId;
use gdl_watch::CellWatchEvent;

const M1: &str = "model-x-01-us-east-1";

/// The next event of `watch` about `instance`'s retirement.
async fn retirement(watch: &mut gdl_hub::CellWatch, instance: InstanceId) -> CellWatchEvent {
    let read = async {
        loop {
            match watch.next().await.expect("the watch goes on") {
                event @ CellWatchEvent::Retired { instance: of, .. } if of == instance => {
                    return event;
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the retirement in time")
}

/// A chooser that follows the model cell avoids its retired entry
/// instance once its tables hold it retired, and the model cell's hub
/// learns of it through the global cell's hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_choosers_acknowledgement_reaches_the_retirees_cell() {
    let directory = Resolving::new();
    let (global, _) = cell_hub(global_hub(), &directory).await;
    let (m1, model) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (mut retiring, staying, _d) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
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
    until(&cells, "the model cell ready with its entries", |cells| {
        followed(cells, "f", M1).is_some_and(|m| {
            m.state == CellState::Ready && m.eligible == sorted(&[&retiring, &staying])
        })
    })
    .await;
    let mut watch = model.watch().await.expect("the cell's watch");
    retiring.announce(SegmentPhase::Retired).await;
    let id = retiring.id();
    assert_eq!(
        retirement(&mut watch, id).await,
        CellWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
            acknowledged: false,
        },
        "the global cell follows the model cell"
    );
    assert_eq!(
        retirement(&mut watch, id).await,
        CellWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
            acknowledged: true,
        }
    );
    assert!(
        followed(&cells, "f", M1).is_some_and(|m| m.retired == [id]),
        "acknowledged before the chooser's tables held it retired"
    );
}

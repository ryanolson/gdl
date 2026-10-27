//! Under the `relay` directory backend, with no directory that the cells
//! share: a model cell's hub reports its record to its parent's hub, the
//! parent follows it from what it reported and routes a request to it,
//! and the withdrawal at the end of the cell's drain removes it from the
//! parent.
//!
//! Every hub and instance runs in this process on OS-assigned ports;
//! every wait is bounded and for something observed. The drain waits for
//! the entry instance to decline offers, so the test takes a few seconds.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::boundary::{RUNTIME, SUMMARY, ask, placement, run_graph, spec_of};
use common::cells::{
    GLOBAL, REGION, followed, global_cell, hub_config, model_cell, settings, timing, until,
};
use common::{Events, WAIT, reference_graph};
use futures::StreamExt;
use gdl::FollowedCells;
use gdl_cells::{CellDirectory, CellEvent, CellRecord, DirectoryError, ResumeToken, Withdrawal};
use gdl_hub::{CellReadiness, CellTiming, GraphAdmission, HubDirectory};
use gdl_model::CellSettings;
use gdl_runtime::{CellState, Deployment, DeploymentSpec, RuntimeConfig};
use tokio::sync::watch;
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";

/// A relay directory in front of which a cell's hub is named as DNS
/// would name it: its port is known only once it serves.
#[derive(Clone)]
struct Named {
    inner: HubDirectory,
    names: Arc<watch::Sender<HashMap<String, String>>>,
}

impl CellDirectory for Named {
    type Watch = <HubDirectory as CellDirectory>::Watch;

    async fn publish(&self, mut record: CellRecord) -> Result<(), DirectoryError> {
        let mut names = self.names.subscribe();
        record.hub = names
            .wait_for(|names| names.contains_key(&record.hub))
            .await
            .expect("the names live")[&record.hub]
            .clone();
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch {
        self.inner.watch(class, from)
    }
}

/// The hub of `cell` under `relay`, reporting to `parent_hub` if given,
/// and keeping its children's records in `state_dir` if given.
async fn relay_hub(
    spec: &DeploymentSpec,
    cell: CellSettings,
    parent_hub: Option<String>,
    state_dir: Option<&std::path::Path>,
    timing: CellTiming,
) -> (Hub, Arc<CellReadiness<Named>>, HubDirectory) {
    let state_dir = state_dir.map(std::path::Path::to_owned);
    let cell = CellSettings {
        directory: gdl_model::DirectoryBackend::Relay {
            parent_hub: parent_hub.clone(),
            state_dir: state_dir.clone(),
        },
        ..cell
    };
    let directory = HubDirectory::new(parent_hub, state_dir).expect("a relay directory");
    let named = Named {
        inner: directory.clone(),
        names: Arc::new(watch::Sender::new(HashMap::new())),
    };
    let name = cell.hub.clone().expect("a hub name");
    let readiness = Arc::new(
        CellReadiness::new(spec, cell, named.clone(), timing)
            .expect("valid test fixture")
            .with_reports(&directory)
            .expect("a lease its reports cover"),
    );
    let hub = Hub::builder(hub_config())
        .extension(GraphAdmission::new(spec.clone()))
        .extension(Arc::clone(&readiness))
        .serve()
        .await
        .expect("the hub serves");
    let address = address(&hub);
    named
        .names
        .send_modify(|names| drop(names.insert(name, address)));
    (hub, readiness, directory)
}

fn address(hub: &Hub) -> String {
    format!("127.0.0.1:{}", hub.public_addr().port())
}

/// Whether the parent's directory holds a record of `cell`.
async fn holds(directory: &HubDirectory, cell: &str) -> bool {
    let mut watch = std::pin::pin!(directory.watch("model_x", None));
    let mut held = false;
    let read = async {
        while let Some(Ok(event)) = watch.next().await {
            match event {
                CellEvent::Current(record) if record.name == cell => held = true,
                CellEvent::Synced { .. } => return,
                _ => {}
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the snapshot in time");
    held
}

/// A model cell reports to its parent's hub, which follows it from its
/// report and routes a request to it; once the cell has drained, its
/// withdrawal reaches the parent, which holds it no more, and whose
/// choosers read it as withdrawn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_reports_to_its_parent_which_routes_to_it_until_it_withdraws() {
    let spec = spec_of(placement());
    let global: CellSettings = toml::from_str(&format!(
        "name = '{GLOBAL}'\nclass = 'global'\nregion = '{REGION}'\nhub = 'global-hub:1337'\n"
    ))
    .expect("valid test fixture");
    let state = tempfile::Builder::new()
        .prefix("gdl-runtime-relay-state-")
        .tempdir()
        .expect("a temporary directory");
    let (parent, _global, children) =
        relay_hub(&spec, global, None, Some(state.path()), timing()).await;

    let followed_cells = FollowedCells::new();
    let mut config = settings(
        parent.public_addr().port(),
        parent.control_addr().port(),
        5_000,
        &format!("{}{RUNTIME}", global_cell()),
    );
    config.runtime.followed = followed_cells.clone();
    let quiet = Events::quiet();
    let assemble = |deployment: &Deployment| reference_graph(deployment, &quiet);
    let runtime = config.runtime.clone();
    let chooser = run_graph(&config, "a", &spec, runtime, &assemble)
        .await
        .expect("the chooser serves");

    let model: CellSettings = toml::from_str(&format!(
        "name = '{M1}'\nclass = 'model_x'\nregion = '{REGION}'\nhome = '{GLOBAL}'\n\
         hub = 'm1-hub:1337'\nsummary_ms = {}\n[ready]\nf = 1\nd = 1\n",
        SUMMARY.as_millis()
    ))
    .expect("valid test fixture");
    let (model_hub, model, _) = relay_hub(
        &spec,
        model,
        Some(address(&parent)),
        None,
        CellTiming {
            heartbeat: SUMMARY,
            ..timing()
        },
    )
    .await;
    let config = settings(
        model_hub.public_addr().port(),
        model_hub.control_addr().port(),
        5_000,
        &format!(
            "{}summary_ms = {}\n{RUNTIME}",
            model_cell(M1),
            SUMMARY.as_millis()
        ),
    );
    // Each instance's launch waits for the others of its class.
    let instances = futures::future::join_all(["d", "f"].map(|segment| {
        let runtime: RuntimeConfig = config.runtime.clone();
        let (config, spec, assemble) = (&config, &spec, &assemble);
        async move {
            run_graph(config, segment, spec, runtime, assemble)
                .await
                .expect("a model instance serves")
        }
    }))
    .await;

    let cells = chooser.cells.clone().expect("a global instance chooses");
    until(&cells, "the parent's chooser follows the child", |cells| {
        followed(cells, "f", M1)
            .is_some_and(|m| m.state == CellState::Ready && m.eligible.len() == 1)
    })
    .await;
    assert!(holds(&children, M1).await, "the parent holds the report");
    ask(&chooser.segment).await.expect("answered by the child");
    assert_eq!(instances[1].forwards(), 1, "the child's entry took it");

    model.drain().await.expect("the cell drains");
    // The entry's bound, a setup timeout and two summary periods, unless
    // the parent lets go first.
    let bound = Duration::from_secs(3) + WAIT;
    for instance in instances {
        instance.ended(bound).await.expect("the instance drained");
    }
    let withdrawn = tokio::time::timeout(WAIT, async {
        while holds(&children, M1).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        withdrawn.is_ok(),
        "the parent still holds the drained child"
    );
    until(&cells, "the chooser reads the child withdrawn", |_| {
        followed_cells
            .read("model_x", |cells| {
                (cells.cell(M1).is_none(), cells.is_withdrawn(M1))
            })
            .expect("the chooser sends to model_x")
            == (true, true)
    })
    .await;
    for hub in [model_hub, parent] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

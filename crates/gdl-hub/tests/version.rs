//! Model versions when a relay parent cannot return the last record.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{WAIT, config, millis_now, spec};
use gdl_cells::{CellRecord, ModelEntry, ModelState, Presence};
use gdl_hub::{CellReadiness, CellTiming, GraphAdmission, HubDirectory};
use gdl_model::{CellSettings, DirectoryBackend};
use gdl_watch::{Report, ReportRefusal, ReportReply};
use tokio::sync::mpsc;
use velo_hub::Hub;
use velo_hub::axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::post};

/// A 409 can raise the writer's epoch after the restart read. The model
/// version must rise with it, also when the parent has no read route.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovered_writer_epoch_raises_the_models_version() {
    let spec = spec();
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    let epoch = millis_now() + 60_000;
    let previous = CellRecord {
        name: common::CELL.into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some(common::GLOBAL.into()),
        contract: layout.contract_hash(),
        placement: layout.placement_hash(),
        state: gdl_cells::CellState::Unavailable,
        epoch,
        seq: 10,
        hub: "model-x-01-hub:1337".into(),
        lease: Duration::from_secs(30),
        load: None,
        presence: Presence::Serving,
        models: vec![ModelEntry {
            model: "m".into(),
            routing_key: None,
            state: ModelState::Ready,
            since: epoch * (1 << 20) + 1,
        }],
        models_version: epoch * (1 << 20) + 1,
        revoked: Vec::new(),
    };

    for readable in [true, false] {
        let held = previous.clone();
        let (sent, mut accepted) = mpsc::unbounded_channel::<CellRecord>();
        let mut app = Router::new().route(
            "/v1/ext/gdl-cell/report",
            post(move |Json(report): Json<Report>| {
                let held = held.clone();
                let sent = sent.clone();
                async move {
                    let Report::Record(record) = report else {
                        panic!("the child publishes its record");
                    };
                    if record.version() <= held.version() {
                        return (
                            StatusCode::CONFLICT,
                            Json(ReportRefusal::NotNewer {
                                held: (held.epoch, held.seq),
                                offered: (record.epoch, record.seq),
                            }),
                        )
                            .into_response();
                    }
                    sent.send(record).expect("the test receives the record");
                    Json(ReportReply { interval_ms: 100 }).into_response()
                }
            }),
        );
        if readable {
            let held = previous.clone();
            app = app.route(
                "/v1/ext/gdl-cell/report/{class}/{cell}",
                velo_hub::axum::routing::get(move || {
                    let held = held.clone();
                    async move { Json(held) }
                }),
            );
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a parent port");
        let parent = listener
            .local_addr()
            .expect("the parent address")
            .to_string();
        let server = tokio::spawn(async move { velo_hub::axum::serve(listener, app).await });
        let directory = HubDirectory::new(Some(parent.clone()), None).expect("a relay directory");
        let settings = CellSettings {
            directory: DirectoryBackend::Relay {
                parent_hub: Some(parent),
                state_dir: None,
            },
            ..common::settings()
        };
        let cell = Arc::new(
            CellReadiness::new(
                &spec,
                settings,
                directory.clone(),
                CellTiming {
                    heartbeat: Duration::from_millis(100),
                    ..CellTiming::default()
                },
            )
            .expect("valid cell settings")
            .with_reports(&directory)
            .expect("a relay cell"),
        );
        let hub = Hub::builder(config())
            .extension(GraphAdmission::new(spec.clone()))
            .extension(cell)
            .serve()
            .await
            .expect("the child serves");
        let record = tokio::time::timeout(WAIT, accepted.recv())
            .await
            .expect("the parent accepts a record in time")
            .expect("the parent receives the record");
        hub.shutdown().await.expect("the child stops");
        server.abort();

        assert!(
            record.epoch > previous.epoch,
            "the writer recovers its epoch"
        );
        if readable {
            assert_eq!(record.models.len(), 1, "the history is retained");
            assert_eq!(record.models[0].state, ModelState::Unavailable);
        } else {
            assert_ne!(record.models, previous.models, "different lists");
        }
        assert!(
            record.models_version > previous.models_version,
            "a newer writer lowered models_version (readable={readable}): before=({}, {}), after=({}, {})",
            previous.epoch,
            previous.models_version,
            record.epoch,
            record.models_version,
        );
    }
}

/// When a parent has no restart read route, its retained epoch can equal
/// the new writer's millisecond clock value. Catching up its sequence
/// must not lower the model version. A greater retained epoch is a control.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repeated_writer_epoch_keeps_model_versions_increasing() {
    for equal in [false, true] {
        let held = Arc::new(std::sync::Mutex::new(None::<CellRecord>));
        let (sent, mut accepted) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/v1/ext/gdl-cell/report",
            post(move |Json(report): Json<Report>| {
                let held = Arc::clone(&held);
                let sent = sent.clone();
                async move {
                    let Report::Record(record) = report else {
                        panic!("the child publishes its record");
                    };
                    let mut held = held.lock().expect("the retained record");
                    let previous = held.get_or_insert_with(|| {
                        // Inject the predecessor at the first offer so the
                        // equal-epoch case needs no wall-clock race.
                        let epoch = record.epoch + if equal { 0 } else { 60_000 };
                        let version = epoch * (1 << 20) + 1;
                        CellRecord {
                            epoch,
                            seq: 10,
                            models: vec![ModelEntry {
                                model: "m".into(),
                                routing_key: None,
                                state: ModelState::Ready,
                                since: version,
                            }],
                            models_version: version,
                            ..record.clone()
                        }
                    });
                    if record.version() <= previous.version() {
                        return (
                            StatusCode::CONFLICT,
                            Json(ReportRefusal::NotNewer {
                                held: (previous.epoch, previous.seq),
                                offered: (record.epoch, record.seq),
                            }),
                        )
                            .into_response();
                    }
                    sent.send((previous.clone(), record))
                        .expect("the test receives the accepted record");
                    Json(ReportReply { interval_ms: 100 }).into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a parent port");
        let parent = listener
            .local_addr()
            .expect("the parent address")
            .to_string();
        let server = tokio::spawn(async move { velo_hub::axum::serve(listener, app).await });
        let directory = HubDirectory::new(Some(parent.clone()), None).expect("a relay directory");
        let cell = Arc::new(
            CellReadiness::new(
                &spec(),
                CellSettings {
                    directory: DirectoryBackend::Relay {
                        parent_hub: Some(parent),
                        state_dir: None,
                    },
                    ..common::settings()
                },
                directory.clone(),
                CellTiming {
                    heartbeat: Duration::from_millis(100),
                    ..CellTiming::default()
                },
            )
            .expect("valid cell settings")
            .with_reports(&directory)
            .expect("a relay cell"),
        );
        let hub = Hub::builder(config())
            .extension(GraphAdmission::new(spec()))
            .extension(cell)
            .serve()
            .await
            .expect("the child serves");
        let (previous, record) = tokio::time::timeout(WAIT, accepted.recv())
            .await
            .expect("the parent accepts a record in time")
            .expect("the accepted record arrives");
        hub.shutdown().await.expect("the child stops");
        server.abort();

        assert!(record.version() > previous.version(), "the record is newer");
        assert_ne!(record.models, previous.models, "the lists differ");
        assert!(
            record.models_version > previous.models_version,
            "sequence recovery lowered models_version (equal={equal}): before=({}, {}, {}), after=({}, {}, {})",
            previous.epoch,
            previous.seq,
            previous.models_version,
            record.epoch,
            record.seq,
            record.models_version,
        );
    }
}

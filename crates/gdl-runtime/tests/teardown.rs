//! Teardown under the request scope, in four placements of the reference
//! graph. B calls its `next` with its own context and asks for no scope per
//! call ([`UnscopedB`]), in each of its four ways (one call, a retry, a
//! hedge, an abandoned first call). Each request ends while it dispatches
//! or while it streams, by the caller's cancel, by the caller dropping it,
//! or by its deadline. In every case D's streams and its helper tasks end,
//! and every segment gives back its admission shares, attempts, egress
//! permits, anchors and senders.
//!
//! The velo nodes and the deployment of a placement are started once and
//! serve its 24 cases. Each wait is on an event or a count, and bounded.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use futures::StreamExt;
use gdl::{Cx, ErrorKind};
use gdl_runtime::{Placement, RuntimeConfig};

#[derive(Clone, Copy, Debug)]
enum Phase {
    /// D's `generate` holds until its context ends.
    Dispatch,
    /// D's stream never ends, an item a millisecond.
    Stream,
}

#[derive(Clone, Copy, Debug)]
enum End {
    Cancel,
    Detach,
    Deadline,
}

const MODES: [Mode; 4] = [
    Mode::Plain,
    Mode::Retry { max: 2 },
    Mode::Hedge,
    Mode::AbandonFirst { after_ms: 20 },
];
const PHASES: [Phase; 2] = [Phase::Dispatch, Phase::Stream];
const ENDS: [End; 3] = [End::Cancel, End::Detach, End::Deadline];
const DEADLINE: Duration = Duration::from_millis(400);

/// The ways one of D's calls can end, as D records them.
const ENDINGS: [&str; 7] = [
    "D:generate-ended:",
    "D:fail:",
    "D:cancelled:",
    "D:deadline:",
    "D:dropped:",
    "D:done:",
    "D:stopped:",
];

fn plan(mode: Mode, phase: Phase) -> Plan {
    Plan {
        tokens: 1,
        endless: matches!(phase, Phase::Stream),
        item_delay_ms: 1,
        hold_in_generate: matches!(phase, Phase::Dispatch),
        fail_attempts: u32::from(matches!(mode, Mode::Retry { .. })),
        helper: true,
        mode,
        ..Plan::default()
    }
}

/// How many of D's calls a request surely makes before it ends, in each
/// way. A streaming hedge drops its loser once the winner yields, which can
/// be before the loser reaches D, so only the winner's call is sure.
fn calls(mode: Mode, phase: Phase) -> usize {
    match (mode, phase) {
        (Mode::Plain, _) | (Mode::Retry { .. }, Phase::Dispatch) | (Mode::Hedge, Phase::Stream) => {
            1
        }
        _ => 2,
    }
}

/// D's calls that started for `text`, by key.
fn started(events: &Events, text: &str) -> Vec<String> {
    let prefix = format!("D:start:c({text})#");
    events
        .snapshot()
        .into_iter()
        .filter_map(|event| {
            event
                .strip_prefix("D:start:")
                .filter(|_| event.starts_with(&prefix))
                .map(str::to_string)
        })
        .collect()
}

async fn within(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !done() {
            tokio::time::sleep(ms(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

async fn placement(name: &str, blocks: &[&[&str]]) {
    let mut placement = Placement::new();
    let mut instances = Vec::new();
    for (index, nodes) in blocks.iter().enumerate() {
        let segment = format!("s{index}");
        placement = placement.segment(&segment, nodes.iter().copied());
        instances.push((segment, velo_node().await));
    }
    let events = Events::new();
    let cluster = Arc::new(deploy_with(
        &instances,
        &placement,
        &events,
        &|_| RuntimeConfig::default(),
        &|events| {
            Arc::new(UnscopedB(NodeB {
                events: Arc::clone(events),
            }))
        },
    ));
    let source = source_segment(&placement);
    let mut case = 0;
    for mode in MODES {
        for phase in PHASES {
            for end in ENDS {
                case += 1;
                let what = format!("{name}: {mode:?}, {phase:?}, {end:?}");
                let text = format!("{name}{case}");
                let root = match end {
                    End::Deadline => Cx::new().with_timeout(DEADLINE),
                    _ => Cx::new(),
                };
                let (first_tx, first_rx) = tokio::sync::oneshot::channel();
                let (cluster_in, source_in) = (Arc::clone(&cluster), source.clone());
                let call = {
                    let root = root.clone();
                    let prompt = prompt(&text, plan(mode, phase));
                    tokio::spawn(async move {
                        let segment = cluster_in.segment(&source_in);
                        let mut out = match segment.call(root, prompt).await {
                            Ok(out) => out,
                            Err(error) => return Some(Err(error.kind())),
                        };
                        let mut first = Some(first_tx);
                        let mut last = None;
                        while let Some(item) = out.next().await {
                            if let Some(first) = first.take() {
                                let _ = first.send(());
                            }
                            last = Some(item.map(|_| ()).map_err(|e| e.kind()));
                        }
                        last
                    })
                };
                // The request is where the case says it is.
                let want = calls(mode, phase);
                within(&format!("{what}: D was not reached"), || {
                    started(&events, &text).len() >= want
                })
                .await;
                if matches!(phase, Phase::Stream) {
                    tokio::time::timeout(WAIT, first_rx)
                        .await
                        .unwrap_or_else(|_| panic!("{what}: no item reached the caller"))
                        .unwrap_or_else(|_| panic!("{what}: the call ended before its first item"));
                }
                match end {
                    End::Cancel => root.cancel(),
                    End::Detach => call.abort(),
                    End::Deadline => {}
                }
                // Every call of D ended, and so did its helper.
                within(&format!("{what}: a call of D outlived the request"), || {
                    let all = events.snapshot();
                    started(&events, &text).iter().all(|key| {
                        ENDINGS
                            .iter()
                            .any(|ending| all.contains(&format!("{ending}{key}")))
                            && all.contains(&format!("D:helper-ended:{key}"))
                    })
                })
                .await;
                assert!(
                    settles(&cluster).await,
                    "{what}: a segment kept what the request took"
                );
                within(&format!("{what}: a sender was still counted"), || {
                    cluster.segments.iter().all(|(_, s)| s.senders() == 0)
                })
                .await;
                let outcome = tokio::time::timeout(WAIT, call)
                    .await
                    .unwrap_or_else(|_| panic!("{what}: the call did not end"));
                match end {
                    End::Detach => assert!(outcome.is_err(), "{what}: the aborted call finished"),
                    End::Cancel => assert_eq!(
                        outcome.expect("the call"),
                        Some(Err(ErrorKind::Cancelled)),
                        "{what}"
                    ),
                    End::Deadline => assert_eq!(
                        outcome.expect("the call"),
                        Some(Err(ErrorKind::DeadlineExceeded)),
                        "{what}"
                    ),
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_segment_tears_down_every_case() {
    placement("one", &[&REFERENCE_NODES]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_sink_tears_down_every_case() {
    placement("sink", &[&["A", "B", "C", "G"], &["D", "F", "E"]]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_segments_tear_down_every_case() {
    placement("three", &[&["A", "B", "C", "G"], &["D"], &["F", "E"]]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_forward_node_tears_down_every_case() {
    placement("forward", &[&["A", "B", "G"], &["C", "D", "F", "E"]]).await;
}

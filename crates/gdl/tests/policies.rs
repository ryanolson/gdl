//! A cell policy as a crate outside gdl writes and tests it: it reads a
//! routing hint through a typed projection of the request, and it builds
//! the inputs to its choice itself, with no hub, no velo node, and no
//! runtime.

use std::collections::BTreeSet;
use std::time::Duration;

use gdl::{
    Cell, CellPolicies, CellPolicy, CellRequest, CellState, Cx, Damped, HomeFirst, Load,
    LoadSummary, Projected, Projection,
};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// What a frontend sends into a model's class: the request it serves,
/// and the hint a router reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Routed {
    request: Internal,
    hint: Hint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Internal {
    messages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Hint {
    model: String,
    fingerprint: [u8; 16],
    first_turn: bool,
}

/// The part of [`Routed`] the policy reads: only its hint.
#[derive(Debug, Deserialize)]
struct Hinted {
    hint: Hint,
}

impl Projection for Hinted {
    type Of = Routed;
}

/// Sends a request to a cell whose name starts with its model, and,
/// among those, as `home_first` would.
struct ByModel;

impl CellPolicy for ByModel {
    type Head = Projected<Hinted>;

    fn choose(&self, request: CellRequest<'_, Projected<Hinted>>, cells: &[Cell]) -> Option<usize> {
        let model = &request.head().hint.model;
        let mut serving = cells
            .iter()
            .enumerate()
            .filter(|(_, cell)| cell.name().starts_with(model.as_str()))
            .filter(|(_, cell)| !request.is_excluded(cell) && cell.takes_work());
        serving.next().map(|(at, _)| at)
    }
}

const HOME: &str = "us-east-1";
const AWAY: &str = "eu-west-1";

/// An idle summary, arrived at `now`, of a cell with one entry instance.
fn idle(now: Instant) -> Load {
    Load::new(
        LoadSummary {
            admitted: 0,
            limit: 10,
            instances: 1,
            refusals: 0,
            period_ms: 10_000,
        },
        now,
    )
}

fn cell(name: &str, region: &str, now: Instant) -> Cell {
    Cell::new("model", name, region, CellState::Ready)
        .with_entries(1)
        .with_load(idle(now))
}

fn hinted(model: &str) -> Projected<Hinted> {
    Projected(Hinted {
        hint: Hint {
            model: model.into(),
            fingerprint: [7; 16],
            first_turn: true,
        },
    })
}

/// Two requests that differ only in their hint's model reach different
/// cells of one class.
#[test]
fn the_hint_chooses_the_cell() {
    let now = Instant::now();
    let cells = [cell("llama-1", HOME, now), cell("qwen-1", HOME, now)];
    let cx = Cx::new();
    let chosen = |model: &str| {
        let head = hinted(model);
        ByModel.choose(CellRequest::new(&cx, &head, HOME, now), &cells)
    };
    assert_eq!(chosen("llama"), Some(0));
    assert_eq!(chosen("qwen"), Some(1));
    assert_eq!(chosen("mistral"), None);
}

/// A policy reads the request's excluded cells and the cells' state as
/// the runtime gives them.
#[test]
fn a_policy_sees_what_the_runtime_gives_it() {
    let now = Instant::now();
    let cells = [
        cell("llama-1", HOME, now),
        cell("llama-2", AWAY, now),
        Cell::new("model", "llama-3", HOME, CellState::Draining).with_entries(1),
    ];
    let cx = Cx::new();
    let head = hinted("llama");
    let excluded = [cells[0].id()];
    let request = CellRequest::new(&cx, &head, HOME, now).with_excluded(&excluded);
    assert_eq!(request.region(), HOME);
    assert_eq!(request.now(), now);
    assert_eq!(ByModel.choose(request, &cells), Some(1));
    let excluded = [cells[0].id(), cells[1].id()];
    let request = CellRequest::new(&cx, &head, HOME, now).with_excluded(&excluded);
    assert_eq!(ByModel.choose(request, &cells), None, "a draining cell");
    // A cell is named the same in every table: its class and name.
    assert_eq!(
        cells[0].id(),
        Cell::new("model", "llama-1", AWAY, CellState::Degraded).id()
    );
    assert_ne!(
        cells[0].id(),
        Cell::new("other", "llama-1", HOME, CellState::Ready).id()
    );
}

/// gdl's own policies take the same inputs that a policy crate builds:
/// `home_first` takes home cells before cells elsewhere, and passes a
/// damped cell over while its window lasts.
#[test]
fn home_first_runs_on_inputs_a_policy_crate_builds() {
    let now = Instant::now();
    let cells = [
        cell("a", HOME, now),
        cell("b", HOME, now),
        cell("away", AWAY, now),
    ];
    let cx = Cx::new();
    let chosen = |damped: &Damped| -> BTreeSet<&str> {
        (0..64)
            .filter_map(|turn| {
                let request = CellRequest::new(&cx, &gdl::Encoded, HOME, now)
                    .with_damped(damped)
                    .with_turn(turn);
                HomeFirst.choose(request, &cells)
            })
            .map(|at| cells[at].name())
            .collect()
    };
    assert_eq!(chosen(&Damped::default()), BTreeSet::from(["a", "b"]));
    let damped = Damped::new([(cells[0].id(), now + Duration::from_secs(1))]);
    assert!(damped.lasts(now) && damped.is_damped(cells[0].id()));
    assert_eq!(damped.left(cells[0].id(), now), 1000);
    assert_eq!(chosen(&damped), BTreeSet::from(["b"]));
    // A summary older than three periods is stale: such a cell is not
    // chosen by `home_first`.
    let stale = [Cell::new("model", "old", HOME, CellState::Ready)
        .with_entries(1)
        .with_load(idle(now - Duration::from_secs(31)))];
    let request = CellRequest::new(&cx, &gdl::Encoded, HOME, now);
    assert_eq!(HomeFirst.choose(request, &stale), None);
    assert!(!HomeFirst.eligible(&stale[0], HOME, now));
    assert!(HomeFirst.eligible(&cells[0], HOME, now));
}

/// A policy is registered by a name a placement can use, once, and not
/// under the name of one of gdl's own.
#[test]
fn a_policy_is_registered_by_name() {
    let mut policies = CellPolicies::new();
    policies.add("by_model", ByModel).expect("registered");
    assert!(policies.contains("by_model"));
    let twice = policies
        .add("by_model", ByModel)
        .expect_err("registered twice");
    assert!(twice.to_string().contains("by_model"), "{twice}");
    for name in ["home_first", "round_robin"] {
        let error = policies.add(name, ByModel).expect_err("a built-in name");
        assert!(error.to_string().contains(name), "{error}");
    }
    for name in ["ByModel", "by__model", "by_model_", ""] {
        assert!(policies.add(name, ByModel).is_err(), "{name:?} was taken");
    }
}

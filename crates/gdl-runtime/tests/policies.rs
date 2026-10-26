//! A registered cell policy at a class cut, end to end: a frontend sends
//! a request that carries a large internal request and a small routing
//! hint into a class of two model cells, and the policy reads only the
//! hint, through a typed projection, to choose the cell. Every instance
//! and hub runs in this process, over loopback TCP.

mod common;

use std::sync::Arc;

use common::boundary::{RUNTIME, run_graph_in};
use common::cells::{
    GLOBAL, REGION, Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until,
};
use common::{WAIT, collect};
use gdl::{
    Cell, CellChoice, CellPolicies, CellPolicy, CellRequest, Cx, Items, Next, Projected,
    Projection, Sink, items,
};
use gdl_model::CellSettings;
use gdl_runtime::{
    BwdEnd, CellState, DSink, Deployment, DeploymentSpec, FwdEnd, HopInfo, Manifest, NodeInfo,
    Placement, Register, Role, TypeInfo, wire_codec,
};
use serde::{Deserialize, Serialize};

/// Every segment: `a` (S, the frontend, which chooses) in `global`, and
/// `d` (D, the model) in `model`.
const SEGMENTS: &[&str] = &["a", "d"];

const LLAMA: &str = "llama-01-us-east-1";
const QWEN: &str = "qwen-01-us-east-1";

/// What the frontend sends into the model's class: the request, and the
/// hint a router reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Routed {
    request: Internal,
    hint: Hint,
}

/// The request a model serves: here about 100 KB of messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Internal {
    messages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Hint {
    model: String,
    fingerprint: [u8; 16],
    first_turn: bool,
}

/// The part of [`Routed`] the router reads.
#[derive(Debug, Deserialize, Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: Hint,
}

/// The cell whose name starts with the request's model.
struct ByModel;

impl CellPolicy for ByModel {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: CellRequest<'_, Projected<Hinted>>,
        cells: &[Cell],
    ) -> Option<CellChoice> {
        let model = &request.head().hint.model;
        cells
            .iter()
            .position(|cell| {
                cell.name().starts_with(model.as_str())
                    && cell.takes_work()
                    && !request.is_excluded(cell)
            })
            .map(CellChoice::new)
    }
}

/// Reads the head as a projection of another type than the edge's.
#[derive(Deserialize, Projection)]
#[gdl(of = Internal)]
struct Misread {
    #[allow(dead_code, reason = "read only to be refused")]
    messages: Vec<String>,
}

struct Misreading;

impl CellPolicy for Misreading {
    type Head = Projected<Misread>;

    fn choose(&self, _: CellRequest<'_, Projected<Misread>>, _: &[Cell]) -> Option<CellChoice> {
        None
    }
}

/// D: answers with its cell's name and how many messages it was sent.
struct Answer {
    cell: String,
}

impl Sink for Answer {
    type Req = Routed;
    type Item = String;

    async fn generate(&self, _: Cx, req: Routed) -> gdl::Result<Items<String>> {
        let answer = format!("{}:{}", self.cell, req.request.messages.len());
        Ok(items(futures::stream::once(async move { Ok(answer) })))
    }
}

/// `S => D` across the class cut, whose cells of `model` are chosen by
/// `policy`.
fn spec(policy: &str) -> DeploymentSpec {
    let graph = Manifest {
        name: "routed".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("Routed", true, Default::default())],
            backward_types: vec![TypeInfo::new("String", true, Default::default())],
        }],
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model", ["d"])
        .cell_policy(
            "model",
            gdl_model::CellPolicy {
                policy: policy.into(),
            },
        );
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// The graph as every instance assembles it; D answers as `cell`.
fn graph(
    deployment: &Deployment,
    cell: &str,
) -> gdl::Result<impl Next<Routed, String> + Register + use<>> {
    deployment.hop(
        0,
        FwdEnd::new(wire_codec!(Routed)),
        DSink::new(Arc::new(Answer { cell: cell.into() })),
        BwdEnd::new(wire_codec!(String)),
    )
}

/// The settings of the hub of the model cell `name`, reached under the
/// name `hub`: one `d` makes it ready.
fn model_hub(name: &str, hub: &str) -> CellSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model'\nregion = '{REGION}'\nhome = '{GLOBAL}'\n\
         hub = '{hub}'\ndirectory = 'memory'\n[ready]\nd = 1\n"
    ))
    .expect("valid test fixture")
}

/// The chooser's settings at `hub`, with `policies` registered.
fn chooser_settings(
    hub: &velo_hub::Hub,
    policies: CellPolicies,
) -> gdl_runtime::ProcessConfig<common::cells::Nothing> {
    let mut config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &format!("{}{RUNTIME}", global_cell()),
    );
    config.runtime.cell_policies = policies;
    config
}

fn routed(model: &str) -> Routed {
    Routed {
        request: Internal {
            messages: vec!["x".repeat(1000); 100],
        },
        hint: Hint {
            model: model.into(),
            fingerprint: [7; 16],
            first_turn: true,
        },
    }
}

/// Two model cells of one class, and a frontend whose policy reads only
/// the request's hint: two requests that differ only in the hint's model
/// reach different cells, each the cell that serves the model, with the
/// whole request. A model no cell serves fails with `Routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hint_chooses_the_cell_across_the_class_cut() {
    let spec = spec("by_model");
    let directory = Resolving::new();
    let (global, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let mut policies = CellPolicies::new();
    policies.add("by_model", ByModel).expect("registered");
    let config = chooser_settings(&global, policies);
    let chooser = run_graph_in(
        &config,
        "a",
        SEGMENTS,
        &spec,
        config.runtime.clone(),
        &|deployment| graph(deployment, "frontend"),
    )
    .await
    .expect("the chooser serves");
    let mut models = Vec::new();
    for (cell, hub_name) in [(LLAMA, "llama-hub:1337"), (QWEN, "qwen-hub:1337")] {
        let (hub, _) = cell_hub_of(&spec, model_hub(cell, hub_name), &directory).await;
        let config = settings(
            hub.public_addr().port(),
            hub.control_addr().port(),
            5_000,
            &format!(
                "[cell]\nname = '{cell}'\nclass = 'model'\nregion = '{REGION}'\ndirectory = 'memory'\n{RUNTIME}"
            ),
        );
        let model = run_graph_in(
            &config,
            "d",
            SEGMENTS,
            &spec,
            config.runtime.clone(),
            &|deployment| graph(deployment, cell),
        )
        .await
        .expect("a model instance serves");
        models.push((hub, model));
    }
    let cells = chooser.cells.clone().expect("the frontend chooses");
    until(&cells, "both model cells ready", |cells| {
        [LLAMA, QWEN].iter().all(|cell| {
            followed(cells, "d", cell)
                .is_some_and(|m| m.state == CellState::Ready && m.eligible.len() == 1)
        })
    })
    .await;
    let ask = |model: &str| {
        let request = routed(model);
        let segment = Arc::clone(&chooser.segment);
        async move {
            tokio::time::timeout(WAIT, async {
                let out = segment.call(Cx::new(), request).await?;
                Ok::<_, gdl::Error>(collect(out).await)
            })
            .await
            .expect("answered in time")
        }
    };
    for (model, cell) in [("qwen", QWEN), ("llama", LLAMA), ("qwen", QWEN)] {
        let answer = ask(model).await.expect("answered");
        assert_eq!(answer.len(), 1, "{answer:?}");
        assert_eq!(
            answer[0].as_ref().expect("an answer"),
            &format!("{cell}:100"),
            "{model}"
        );
    }
    let error = ask("mistral").await.expect_err("no cell serves mistral");
    assert_eq!(error.kind(), gdl::ErrorKind::Routing, "{error}");
    assert!(error.to_string().contains("`model`"), "{error}");
    for (hub, model) in models {
        model.stop().await;
        hub.shutdown().await.expect("valid test fixture");
    }
    chooser.stop().await;
    global.shutdown().await.expect("valid test fixture");
}

/// A frontend whose placement names a cell policy that nothing registers
/// refuses to launch, before it registers with its hub, naming the policy
/// and the class; with the policy registered, it launches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unregistered_policy_is_refused_at_launch() {
    let spec = spec("by_model");
    let directory = Resolving::new();
    let (global, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let launch = |policies: CellPolicies| {
        let config = chooser_settings(&global, policies);
        let spec = &spec;
        async move {
            let process = gdl_runtime::Process::start(&config).await?;
            let launched = tokio::time::timeout(
                WAIT,
                config.launch(&process, "a", SEGMENTS, spec, async { Ok(()) }),
            )
            .await
            .expect("the launch ended in time");
            launched.map(|_| ())
        }
    };
    let Err(error) = launch(CellPolicies::new()).await else {
        panic!("a policy nothing registers was taken");
    };
    let message = error.to_string();
    assert_eq!(error.kind(), gdl::ErrorKind::Placement, "{message}");
    assert!(
        message.contains("by_model") && message.contains("[cells.model]"),
        "{message}"
    );
    let mut policies = CellPolicies::new();
    policies.add("by_model", ByModel).expect("registered");
    launch(policies)
        .await
        .expect("a registered policy launches");
    global.shutdown().await.expect("valid test fixture");
}

/// A policy whose head is a projection of another type than the one the
/// edge carries is refused when the frontend's segment starts, naming
/// the policy, its class, and both types.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_projection_of_another_type_is_refused_at_start() {
    let spec = spec("misreading");
    let directory = Resolving::new();
    let (global, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let mut policies = CellPolicies::new();
    policies.add("misreading", Misreading).expect("registered");
    let config = chooser_settings(&global, policies);
    let started = tokio::time::timeout(
        WAIT,
        run_graph_in(
            &config,
            "a",
            SEGMENTS,
            &spec,
            config.runtime.clone(),
            &|deployment| graph(deployment, "frontend"),
        ),
    )
    .await
    .expect("refused in time");
    let Err(error) = started else {
        panic!("a projection of another type was taken");
    };
    let message = error.to_string();
    assert_eq!(error.kind(), gdl::ErrorKind::Placement, "{message}");
    for part in ["misreading", "`model`", "Internal", "Routed"] {
        assert!(message.contains(part), "{part}: {message}");
    }
    global.shutdown().await.expect("valid test fixture");
}

/// The frontend as a hand-written segment binary, which registers
/// `by_model` in its `build` when `REGISTERS`, and whose serving body
/// returns at once.
struct Frontend<const REGISTERS: bool>;

impl<const REGISTERS: bool> gdl_runtime::SegmentBinary for Frontend<REGISTERS> {
    const PACKAGE: &'static str = "routed-a";
    const SEGMENT: &'static str = "a";
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "";
    type Settings = common::cells::Nothing;
    type Hosts = ();

    async fn build(
        _: common::cells::Nothing,
        builder: &gdl_runtime::Builder,
    ) -> Result<(), gdl_runtime::Failure> {
        if REGISTERS {
            builder.cell_policy("by_model", ByModel)?;
        }
        Ok(())
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        Ok(spec("by_model"))
    }

    fn start(
        _: (),
        node: gdl_runtime::velo::Velo,
        roster: &gdl_runtime::Roster,
        runtime: gdl_runtime::RuntimeConfig,
    ) -> gdl::Result<gdl_runtime::Serving> {
        let spec = spec("by_model");
        let deployment = Deployment::new(node, &spec.graph, &spec.placement, "a", roster, runtime)?;
        let root = graph(&deployment, "frontend")?;
        let segment = deployment.start(root)?;
        Ok(gdl_runtime::Serving::new(segment, |_, _| async { Ok(()) }))
    }
}

/// A segment binary hands the policies its `build` registers to the
/// launch, which checks the placement's names against them: with
/// `by_model` registered, the frontend launches and serves; without, it
/// is refused at launch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_binary_registers_a_policy_before_its_launch() {
    let spec = spec("by_model");
    let directory = Resolving::new();
    let (global, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let dir = tempfile::Builder::new()
        .prefix("gdl-policies-")
        .tempdir()
        .expect("settings directory");
    let file = dir.path().join("settings.toml");
    std::fs::write(
        &file,
        format!(
            "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n{}{RUNTIME}",
            global.public_addr().port(),
            global.control_addr().port(),
            global_cell(),
        ),
    )
    .expect("settings");
    let args = vec![file.into_os_string()];
    tokio::time::timeout(
        WAIT,
        gdl_runtime::run_segment::<Frontend<true>>(args.clone()),
    )
    .await
    .expect("the binary ended in time")
    .expect("a registered policy launches and serves");
    let error = tokio::time::timeout(WAIT, gdl_runtime::run_segment::<Frontend<false>>(args))
        .await
        .expect("the binary ended in time")
        .expect_err("a policy nothing registers");
    let message = error.to_string();
    assert!(
        message.contains("by_model") && message.contains("[cells.model]"),
        "{message}"
    );
    global.shutdown().await.expect("valid test fixture");
}

//! A registered pool policy at a class cut, end to end: a frontend sends
//! a request that carries a large internal request and a small routing
//! hint into a class of two model pools, and the policy reads only the
//! hint, through a typed projection, to choose the pool. Every instance
//! and hub runs in this process, over loopback TCP.

mod common;

use std::sync::Arc;

use common::boundary::{RUNTIME, run_graph_in};
use common::pools::{
    GLOBAL, REGION, Resolving, followed, global_hub, global_pool, pool_hub_of, settings, until,
};
use common::{WAIT, collect};
use gdl::{
    Cx, Items, Next, NoPool, Pool, PoolChoice, PoolPolicies, PoolPolicy, PoolRequest, Projected,
    Projection, Sink, items,
};
use gdl_model::PoolSettings;
use gdl_runtime::{
    BwdEnd, DSink, Deployment, DeploymentSpec, FwdEnd, HopInfo, Manifest, NodeInfo, Placement,
    PoolState, Register, Role, TypeInfo, wire_codec,
};
use serde::{Deserialize, Serialize};

/// Every segment: `a` (S, the frontend, which chooses) in `global`, and
/// `d` (D, the shard) in `model`.
const SEGMENTS: &[&str] = &["a", "d"];

const LLAMA: &str = "llama-01-us-east-1";
const QWEN: &str = "qwen-01-us-east-1";

/// What the frontend sends into the model class: the request, and the
/// hint a router reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Routed {
    request: Internal,
    hint: Hint,
}

/// The request a shard serves: here about 100 KB of messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Internal {
    messages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Hint {
    shard: String,
    fingerprint: [u8; 16],
    first_turn: bool,
}

/// The part of [`Routed`] the router reads.
#[derive(Debug, Deserialize, Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: Hint,
}

/// The pool whose name starts with the request's shard; a shard that no
/// pool serves is named in the request's error.
struct ByShard;

impl PoolPolicy for ByShard {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: PoolRequest<'_, Projected<Hinted>>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let shard = request.head().hint.shard.as_str();
        let serves = |pool: &Pool| pool.name().starts_with(shard);
        if !pools.iter().any(serves) {
            return Err(NoPool::because(format!("no pool serves shard `{shard}`")));
        }
        pools
            .iter()
            .position(|pool| serves(pool) && pool.takes_work() && !request.is_excluded(pool))
            .map(PoolChoice::new)
            .ok_or_else(NoPool::new)
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

impl PoolPolicy for Misreading {
    type Head = Projected<Misread>;

    fn choose(
        &self,
        _: PoolRequest<'_, Projected<Misread>>,
        _: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        Err(NoPool::new())
    }
}

/// D: answers with its pool's name and how many messages it was sent.
struct Answer {
    pool: String,
}

impl Sink for Answer {
    type Req = Routed;
    type Item = String;

    async fn generate(&self, _: Cx, req: Routed) -> gdl::Result<Items<String>> {
        let answer = format!("{}:{}", self.pool, req.request.messages.len());
        Ok(items(futures::stream::once(async move { Ok(answer) })))
    }
}

/// `S => D` across the class cut, whose pools of `model` are chosen by
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
        .pool_policy(
            "model",
            gdl_model::PoolPolicy {
                policy: policy.into(),
            },
        );
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// The graph as every instance assembles it; D answers as `pool`.
fn graph(
    deployment: &Deployment,
    pool: &str,
) -> gdl::Result<impl Next<Routed, String> + Register + use<>> {
    deployment.hop(
        0,
        FwdEnd::new(wire_codec!(Routed)),
        DSink::new(Arc::new(Answer { pool: pool.into() })),
        BwdEnd::new(wire_codec!(String)),
    )
}

/// The settings of the hub of the model pool `name`, reached under the
/// name `hub`: one `d` makes it ready.
fn model_hub(name: &str, hub: &str) -> PoolSettings {
    toml::from_str(&format!(
        "name = '{name}'\nclass = 'model'\nregion = '{REGION}'\nhome = '{GLOBAL}'\n\
         hub = '{hub}'\ndirectory = 'memory'\n[ready]\nd = 1\n"
    ))
    .expect("valid test fixture")
}

/// The chooser's settings at `hub`, with `policies` registered.
fn chooser_settings(
    hub: &velo_hub::Hub,
    policies: PoolPolicies,
) -> gdl_runtime::ProcessConfig<common::pools::Nothing> {
    let mut config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &format!("{}{RUNTIME}", global_pool()),
    );
    config.runtime.pool_policies = policies;
    config
}

fn routed(shard: &str) -> Routed {
    Routed {
        request: Internal {
            messages: vec!["x".repeat(1000); 100],
        },
        hint: Hint {
            shard: shard.into(),
            fingerprint: [7; 16],
            first_turn: true,
        },
    }
}

/// Two model pools of one class, and a frontend whose policy reads only
/// the request's hint: two requests that differ only in the hint's shard
/// reach different pools, each the pool that serves the shard, with the
/// whole request. A shard no pool serves fails with `Routing`, which
/// names the shard as the policy gave it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hint_chooses_the_pool_across_the_class_cut() {
    let spec = spec("by_shard");
    let directory = Resolving::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let mut policies = PoolPolicies::new();
    policies.add("by_shard", ByShard).expect("registered");
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
    let mut instances = Vec::new();
    for (pool, hub_name) in [(LLAMA, "llama-hub:1337"), (QWEN, "qwen-hub:1337")] {
        let (hub, _) = pool_hub_of(&spec, model_hub(pool, hub_name), &directory).await;
        let config = settings(
            hub.public_addr().port(),
            hub.control_addr().port(),
            5_000,
            &format!(
                "[pool]\nname = '{pool}'\nclass = 'model'\nregion = '{REGION}'\ndirectory = 'memory'\n{RUNTIME}"
            ),
        );
        let instance = run_graph_in(
            &config,
            "d",
            SEGMENTS,
            &spec,
            config.runtime.clone(),
            &|deployment| graph(deployment, pool),
        )
        .await
        .expect("a model instance serves");
        instances.push((hub, instance));
    }
    let pools = chooser.pools.clone().expect("the frontend chooses");
    until(&pools, "both model pools ready", |pools| {
        [LLAMA, QWEN].iter().all(|pool| {
            followed(pools, "d", pool)
                .is_some_and(|m| m.state == PoolState::Ready && m.eligible.len() == 1)
        })
    })
    .await;
    let ask = |shard: &str| {
        let request = routed(shard);
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
    for (shard, pool) in [("qwen", QWEN), ("llama", LLAMA), ("qwen", QWEN)] {
        let answer = ask(shard).await.expect("answered");
        assert_eq!(answer.len(), 1, "{answer:?}");
        assert_eq!(
            answer[0].as_ref().expect("an answer"),
            &format!("{pool}:100"),
            "{shard}"
        );
    }
    let error = ask("mistral").await.expect_err("no pool serves mistral");
    assert_eq!(error.kind(), gdl::ErrorKind::Routing, "{error}");
    let message = error.to_string();
    for part in ["`model`", "by_shard", "no pool serves shard `mistral`"] {
        assert!(message.contains(part), "{part}: {message}");
    }
    for (hub, instance) in instances {
        instance.stop().await;
        hub.shutdown().await.expect("valid test fixture");
    }
    chooser.stop().await;
    global.shutdown().await.expect("valid test fixture");
}

/// A frontend whose placement names a pool policy that nothing registers
/// refuses to launch, before it registers with its hub, naming the policy
/// and the class; with the policy registered, it launches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unregistered_policy_is_refused_at_launch() {
    let spec = spec("by_shard");
    let directory = Resolving::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let launch = |policies: PoolPolicies| {
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
    let Err(error) = launch(PoolPolicies::new()).await else {
        panic!("a policy nothing registers was taken");
    };
    let message = error.to_string();
    assert_eq!(error.kind(), gdl::ErrorKind::Placement, "{message}");
    assert!(
        message.contains("by_shard") && message.contains("[pools.model]"),
        "{message}"
    );
    let mut policies = PoolPolicies::new();
    policies.add("by_shard", ByShard).expect("registered");
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
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let mut policies = PoolPolicies::new();
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
/// `by_shard` in its `build` when `REGISTERS`, and whose serving body
/// returns at once.
struct Frontend<const REGISTERS: bool>;

impl<const REGISTERS: bool> gdl_runtime::SegmentBinary for Frontend<REGISTERS> {
    const PACKAGE: &'static str = "routed-a";
    const SEGMENT: &'static str = "a";
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "";
    type Settings = common::pools::Nothing;
    type Hosts = ();

    async fn build(
        _: common::pools::Nothing,
        builder: &gdl_runtime::Builder,
    ) -> Result<(), gdl_runtime::Failure> {
        if REGISTERS {
            builder.pool_policy("by_shard", ByShard)?;
        }
        Ok(())
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        Ok(spec("by_shard"))
    }

    fn start(
        _: (),
        node: gdl_runtime::velo::Velo,
        roster: &gdl_runtime::Roster,
        runtime: gdl_runtime::RuntimeConfig,
    ) -> gdl::Result<gdl_runtime::Serving> {
        let spec = spec("by_shard");
        let deployment = Deployment::new(node, &spec.graph, &spec.placement, "a", roster, runtime)?;
        let root = graph(&deployment, "frontend")?;
        let segment = deployment.start(root)?;
        Ok(gdl_runtime::Serving::new(segment, |_, _| async { Ok(()) }))
    }
}

/// A segment binary hands the policies its `build` registers to the
/// launch, which checks the placement's names against them: with
/// `by_shard` registered, the frontend launches and serves; without, it
/// is refused at launch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_binary_registers_a_policy_before_its_launch() {
    let spec = spec("by_shard");
    let directory = Resolving::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
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
            global_pool(),
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
        message.contains("by_shard") && message.contains("[pools.model]"),
        "{message}"
    );
    global.shutdown().await.expect("valid test fixture");
}

/// Set when `Gated`'s readiness wait starts; `GATE` ends it.
static ENTERED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static GATE: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// `ByShard`, whose `ready` waits for `GATE`.
struct Gated;

impl gdl::Build for Gated {
    type Config = toml::Table;
    type Metrics = ();

    async fn build(_: toml::Table, (): ()) -> gdl::Result<Self> {
        Ok(Self)
    }

    async fn ready(&self) -> gdl::Result<()> {
        ENTERED.store(true, std::sync::atomic::Ordering::SeqCst);
        GATE.notified().await;
        Ok(())
    }
}

impl PoolPolicy for Gated {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: PoolRequest<'_, Self::Head>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        ByShard.choose(request, pools)
    }
}

/// The frontend as a generated segment binary builds a bound policy: from
/// its settings table, registered as `by_shard`, with its readiness kept.
struct GatedFrontend;

impl gdl_runtime::SegmentBinary for GatedFrontend {
    const PACKAGE: &'static str = "routed-a";
    const SEGMENT: &'static str = "a";
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "";
    type Settings = common::pools::Nothing;
    type Hosts = ();

    async fn build(
        _: common::pools::Nothing,
        builder: &gdl_runtime::Builder,
    ) -> Result<(), gdl_runtime::Failure> {
        builder.build_pool_policy::<Gated>("by_shard", None).await
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        Ok(spec("by_shard"))
    }

    fn start(
        (): (),
        node: gdl_runtime::velo::Velo,
        roster: &gdl_runtime::Roster,
        runtime: gdl_runtime::RuntimeConfig,
    ) -> gdl::Result<gdl_runtime::Serving> {
        Frontend::<false>::start((), node, roster, runtime)
    }
}

/// A chooser whose bound policy is not ready yet has registered with its
/// hub, so its deployment sees it live, but does not announce `loaded`
/// until the policy is ready, as for a selector or a node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_policy_is_waited_for_after_the_chooser_registers() {
    use velo::discovery::ServiceDiscovery as _;
    let spec = spec("by_shard");
    let directory = Resolving::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
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
            global_pool(),
        ),
    )
    .expect("settings");
    let registered = || global.store().peers().iter().any(|stored| !stored.pinned);
    let client = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(global.public_addr().port())
        .control_port(global.control_addr().port())
        .build()
        .expect("valid test fixture");
    let loaded = gdl_model::segment_service("a", gdl_model::SegmentPhase::Loaded);
    let observed = async {
        tokio::time::timeout(WAIT, async {
            while !(ENTERED.load(std::sync::atomic::Ordering::SeqCst) && registered()) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the chooser registers while its policy is not ready");
        let announced = client
            .services()
            .get_instances(&loaded)
            .await
            .expect("the hub answers");
        assert!(
            announced.is_empty(),
            "loaded before its policy: {announced:?}"
        );
        GATE.notify_one();
    };
    let run = gdl_runtime::run_segment::<GatedFrontend>(vec![file.into_os_string()]);
    let (ran, ()) = tokio::time::timeout(WAIT * 2, async { tokio::join!(run, observed) })
        .await
        .expect("the binary ended in time");
    ran.expect("the chooser serves once its policy is ready");
    global.shutdown().await.expect("valid test fixture");
}

/// The frontend, whose start checks that the registry the runtime records
/// into already holds the chooser's pool families: a counter of the same
/// name is refused there.
struct Counting;

impl gdl_runtime::SegmentBinary for Counting {
    const PACKAGE: &'static str = "routed-a";
    const SEGMENT: &'static str = "a";
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "";
    type Settings = common::pools::Nothing;
    type Hosts = ();

    async fn build(
        settings: common::pools::Nothing,
        builder: &gdl_runtime::Builder,
    ) -> Result<(), gdl_runtime::Failure> {
        Frontend::<true>::build(settings, builder).await
    }

    fn deployment() -> gdl::Result<DeploymentSpec> {
        Ok(spec("by_shard"))
    }

    fn start(
        (): (),
        node: gdl_runtime::velo::Velo,
        roster: &gdl_runtime::Roster,
        runtime: gdl_runtime::RuntimeConfig,
    ) -> gdl::Result<gdl_runtime::Serving> {
        let registry = runtime.metrics.as_ref().expect("a listener records");
        for name in ["pool_offers_total", "pool_failovers_total"] {
            let counter = prometheus::IntCounter::new(name, "taken").expect("a counter");
            assert!(
                registry.register(Box::new(counter)).is_err(),
                "the chooser's follower did not register `{name}`"
            );
        }
        Frontend::<true>::start((), node, roster, runtime)
    }
}

/// A segment binary with its system listener on hands its registry to the
/// launch, so that the chooser's follower, which starts there, registers
/// the pools' families in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_binary_records_its_pool_choices() {
    let spec = spec("by_shard");
    let directory = Resolving::new();
    let (global, _) = pool_hub_of(&spec, global_hub(), &directory).await;
    let dir = tempfile::Builder::new()
        .prefix("gdl-policies-")
        .tempdir()
        .expect("settings directory");
    let file = dir.path().join("settings.toml");
    std::fs::write(
        &file,
        format!(
            "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {}, control_port = {}, heartbeat_ms = 200 }}\nrendezvous_ms = 5000\n[system]\nhost = \"127.0.0.1\"\nport = 0\n{}{RUNTIME}",
            global.public_addr().port(),
            global.control_addr().port(),
            global_pool(),
        ),
    )
    .expect("settings");
    tokio::time::timeout(
        WAIT,
        gdl_runtime::run_segment::<Counting>(vec![file.into_os_string()]),
    )
    .await
    .expect("the binary ended in time")
    .expect("the chooser launches and serves");
    global.shutdown().await.expect("valid test fixture");
}

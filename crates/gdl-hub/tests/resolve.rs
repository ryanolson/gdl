//! `gdl-hub --resolve <class>/<pool> --write <file>`: a job that joins a
//! running pool learns the pool's hub from the directory when it starts.
//! The resolver writes `[hub]` from the pool's record only once the record
//! shows a heartbeat, and writes nothing for a pool it cannot join. The
//! hub and the resolver run as the `gdl-hub` binary, against a `dir`
//! directory in a scratch directory; the joining segment runs as a
//! segment's `main` runs it, `run_segment` with its settings file and the
//! file the resolver wrote. Every listener is on loopback.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use gdl_pools::{
    DirDirectory, DirWatch, DirectoryError, LastWrite, PoolDirectory, PoolEvent, PoolRecord,
    PoolState, Presence, ResumeToken, Withdrawal,
};
use gdl_runtime::{Builder, DeploymentSpec, Failure, Placement, Roster, RuntimeConfig, Serving};
use gdl_watch::{InstancePhase, Instances};
use serde::Deserialize;

/// How long any one observation may take before the test fails.
const WAIT: Duration = Duration::from_secs(10);

const CLASS: &str = "model_x";
const POOL: &str = "model-x-01";

mod joiner {
    gdl_runtime::graph! {
        name = "joiner", schema = 1;
        source Ask: u32 -> stream u32;
        sink Echo: u32 -> stream u32;
        topology: Ask => Echo;
    }
}

fn placement() -> Placement {
    Placement::new()
        .segment("only", ["Ask", "Echo"])
        .class(CLASS, ["only"])
}

fn deployment() -> gdl_runtime::gdl::Result<DeploymentSpec> {
    Ok(DeploymentSpec::new(joiner::manifest(), placement())?)
}

/// Answers with what it was asked.
struct Echo;

impl gdl_runtime::gdl::Build for Echo {
    type Config = toml::Table;
    type Metrics = ();
    async fn build(_config: toml::Table, (): ()) -> gdl_runtime::gdl::Result<Self> {
        Ok(Self)
    }
}

impl gdl_runtime::gdl::Sink for Echo {
    type Req = u32;
    type Item = u32;
    async fn generate(
        &self,
        _cx: gdl_runtime::gdl::Cx,
        req: u32,
    ) -> gdl_runtime::gdl::Result<gdl_runtime::gdl::Items<u32>> {
        Ok(gdl_runtime::gdl::items(futures::stream::iter([Ok(req)])))
    }
}

/// What the segment takes besides its process settings: nothing.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {}

impl gdl_runtime::Hosted for Settings {
    const KEYS: &'static [&'static str] = &[];
    const NODES: &'static [&'static str] = &["Echo"];
    const ADAPTERS: &'static [&'static str] = &[];
    const GRAPH_NODES: &'static [&'static str] = &["Echo"];
}

/// Ends the joiner's serving body, so that it drains and returns.
static LEAVE: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// The segment binary of the `joiner` graph, as a generated crate makes
/// it: it serves until the test tells it to leave.
struct Joiner;

impl gdl_runtime::SegmentBinary for Joiner {
    const PACKAGE: &'static str = "joiner-only";
    const SEGMENT: &'static str = "only";
    const SEGMENTS: &'static [&'static str] = &["only"];
    const GRAPH_CONFIG: &'static str = "";
    type Settings = Settings;
    type Hosts = Echo;

    async fn build(_settings: Settings, builder: &Builder) -> Result<Echo, Failure> {
        builder.build::<Echo>("Echo", None).await
    }

    fn deployment() -> gdl_runtime::gdl::Result<DeploymentSpec> {
        deployment()
    }

    fn property_types() -> Vec<gdl_runtime::gdl::PropertyType> {
        Vec::new()
    }

    const ENTRY_NEEDS: &'static [gdl_runtime::EntryNeed] = &[];

    fn start(
        echo: Echo,
        node: gdl_runtime::velo::Velo,
        roster: &Roster,
        runtime: RuntimeConfig,
    ) -> gdl_runtime::gdl::Result<Serving> {
        let manifest = joiner::manifest();
        let deployment =
            gdl_runtime::Deployment::new(node, &manifest, &placement(), "only", roster, runtime)?;
        let root = joiner::assemble(&deployment, joiner::Bindings::new().echo(echo))?;
        let segment = deployment.start(root)?;
        Ok(Serving::new(segment, |_segment, _cx| async move {
            LEAVE.notified().await;
            Ok(())
        }))
    }
}

/// A port the OS had free a moment ago. A pool's hub must know the address
/// it publishes before it serves, so its ports are chosen first.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port()
}

/// A scratch directory with the deployment's file and the pool directory.
struct Scratch {
    dir: tempfile::TempDir,
}

impl Scratch {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-hub-resolve-")
            .tempdir()
            .expect("a directory");
        std::fs::create_dir(dir.path().join("pools")).expect("the pool directory");
        std::fs::write(
            dir.path().join("deployment.json"),
            serde_json::to_vec(&deployment().expect("a deployment")).expect("serializable"),
        )
        .expect("written");
        Self { dir }
    }

    fn pools(&self) -> PathBuf {
        self.dir.path().join("pools")
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// `gdl-hub --resolve <target> --write <file>` against the pool
    /// directory, with `extra` variables, run to its end.
    fn resolve(&self, target: &str, file: &Path, extra: &[(&str, &str)]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_gdl-hub"))
            .args(["--resolve", target, "--write"])
            .arg(file)
            .env("GDL_POOL__DIRECTORY", "dir")
            .env("GDL_POOL__DIR", self.pools())
            .envs(extra.iter().copied())
            .stdin(Stdio::null())
            .output()
            .expect("the binary runs")
    }
}

/// A pool's hub process, killed when the test ends however it ends.
struct Hub {
    child: Child,
    public: u16,
    control: u16,
}

impl Drop for Hub {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Hub {
    /// The hub of pool [`POOL`], publishing to the scratch's directory
    /// with a heartbeat of 200 ms, once it says that it serves. Its ports
    /// were free when they were chosen, but another process can bind one
    /// before the hub does: then it starts again on ports chosen afresh.
    fn start(scratch: &Scratch) -> Self {
        let mut said = Vec::new();
        for _ in 0..5 {
            let (public, control) = (free_port(), free_port());
            let mut child = Command::new(env!("CARGO_BIN_EXE_gdl-hub"))
                .arg(scratch.path("deployment.json"))
                .env("GDL_POOL__NAME", POOL)
                .env("GDL_POOL__CLASS", CLASS)
                .env("GDL_POOL__REGION", "us-east-1")
                .env("GDL_POOL__DIRECTORY", "dir")
                .env("GDL_POOL__DIR", scratch.pools())
                .env("GDL_POOL__HUB", format!("127.0.0.1:{public}"))
                .env("GDL_POOL__HEARTBEAT_MS", "200")
                .env("GDL_POOL__LEASE_MS", "1000")
                .env("GDL_HUB__PUBLIC_PORT", public.to_string())
                .env("GDL_HUB__CONTROL_PORT", control.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .expect("a hub");
            let lines = lines(child.stderr.take().expect("piped stderr"));
            let hub = Self {
                child,
                public,
                control,
            };
            said.clear();
            while let Ok(line) = lines.recv_timeout(WAIT) {
                if line.starts_with("gdl-hub: serving") {
                    return hub;
                }
                said.push(line);
            }
            if !said
                .iter()
                .any(|line| line.contains("Address already in use"))
            {
                break;
            }
        }
        panic!("the hub does not serve: {said:?}");
    }
}

/// Each line `stream` writes, as it writes it.
fn lines(stream: impl std::io::Read + Send + 'static) -> mpsc::Receiver<String> {
    use std::io::BufRead;
    let (send, lines) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stream).lines() {
            let Ok(line) = line else { break };
            if send.send(line).is_err() {
                break;
            }
        }
    });
    lines
}

/// What the resolver said on stderr, which must be one line.
fn one_line(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let lines: Vec<&str> = stderr.lines().collect();
    assert_eq!(lines.len(), 1, "not one line: {stderr:?}");
    lines[0].to_string()
}

/// A record of pool `name` that its hub never refreshes, as a hub that
/// died left it.
fn record(name: &str, state: PoolState, lease: Duration) -> PoolRecord {
    PoolRecord {
        name: name.into(),
        class: CLASS.into(),
        region: "us-east-1".into(),
        parent: None,
        contract: 1,
        placement: 1,
        state,
        epoch: 1,
        seq: 1,
        hub: "127.0.0.1:1337".into(),
        lease,
        load: None,
        presence: Presence::Serving,
        properties: Vec::new(),
        properties_version: 0,
        revoked: Vec::new(),
        dropped: Vec::new(),
    }
}

/// The end-to-end case: a pool's hub publishes; the resolver writes its
/// `[hub]`, and only that; a segment started with its settings and that
/// file reaches the hub and turns `ready` in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_joiner_reaches_the_hub_its_pool_published() {
    let scratch = Scratch::new();
    let hub = Hub::start(&scratch);
    let file = scratch.path("gdl-hub.toml");

    let output = scratch.resolve(&format!("{CLASS}/{POOL}"), &file, &[]);
    let said = one_line(&output);
    assert!(output.status.success(), "{said}");
    let written: toml::Table =
        toml::from_str(&std::fs::read_to_string(&file).expect("the file")).expect("TOML");
    let expected: toml::Table = toml::from_str(&format!(
        "[hub]\nhost = \"127.0.0.1\"\npublic_port = {}\n",
        hub.public
    ))
    .expect("TOML");
    assert_eq!(written, expected, "only [hub]: host and public_port");
    // The rename leaves nothing beside the file.
    let beside: Vec<_> = std::fs::read_dir(scratch.dir.path())
        .expect("the scratch directory")
        .map(|entry| entry.expect("an entry").file_name())
        .collect();
    assert_eq!(beside.len(), 3, "{beside:?}");

    join_until_ready(
        &scratch,
        file,
        SocketAddr::from(([127, 0, 0, 1], hub.control)),
    )
    .await;
}

/// The same with the hub on IPv6 loopback, which the hub binary does not
/// bind, so the hub runs in this process: its record names `[::1]`, the
/// resolver writes `host = "::1"` without brackets, and a segment that
/// reads the file reaches both of the hub's listeners on `::1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_joiner_reaches_a_hub_on_an_ipv6_address() {
    let scratch = Scratch::new();
    let hub = Ipv6Hub::start(&scratch).await;
    let public = hub.0.public_addr();
    let file = scratch.path("gdl-hub.toml");
    let output = scratch.resolve(&format!("{CLASS}/{POOL}"), &file, &[]);
    let said = one_line(&output);
    assert!(output.status.success(), "{said}");
    assert!(said.contains(&format!("[::1]:{}", public.port())), "{said}");
    let written: toml::Table =
        toml::from_str(&std::fs::read_to_string(&file).expect("the file")).expect("TOML");
    let expected: toml::Table = toml::from_str(&format!(
        "[hub]\nhost = \"::1\"\npublic_port = {}\n",
        public.port()
    ))
    .expect("TOML");
    assert_eq!(written, expected, "the host without brackets");
    join_until_ready(&scratch, file, hub.0.control_addr()).await;
    hub.0.shutdown().await.expect("the hub stops");
}

/// A pool's hub on `[::1]`, in this process, publishing to the scratch's
/// directory with a heartbeat of 200 ms.
struct Ipv6Hub(velo_hub::Hub);

impl Ipv6Hub {
    /// Its ports were free when they were chosen, but another process can
    /// bind one before the hub does: then it starts again on ports chosen
    /// afresh.
    async fn start(scratch: &Scratch) -> Self {
        let loopback = std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST);
        let mut failed = Vec::new();
        for _ in 0..5 {
            let free = || {
                TcpListener::bind((loopback, 0))
                    .and_then(|listener| listener.local_addr())
                    .expect("a free port on ::1: this test needs IPv6 loopback")
                    .port()
            };
            let (public, control) = (free(), free());
            let spec = deployment().expect("a deployment");
            let vars: Vec<(std::ffi::OsString, std::ffi::OsString)> = [
                ("GDL_POOL__NAME", POOL.to_owned()),
                ("GDL_POOL__CLASS", CLASS.to_owned()),
                ("GDL_POOL__REGION", "us-east-1".to_owned()),
                ("GDL_POOL__DIRECTORY", "dir".to_owned()),
                ("GDL_POOL__DIR", scratch.pools().display().to_string()),
                ("GDL_POOL__HUB", format!("[::1]:{public}")),
                ("GDL_POOL__HEARTBEAT_MS", "200".to_owned()),
                ("GDL_POOL__LEASE_MS", "1000".to_owned()),
            ]
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
            let pool = gdl_hub::pool_from_vars(&spec, vars).expect("the pool's settings");
            let mut config = velo_hub::HubConfig::default();
            config.server.bind_addr = loopback;
            config.server.public_port = public;
            config.server.control_port = control;
            let builder = gdl_hub::hub_builder(config, spec, pool)
                .await
                .expect("a pool's hub");
            match builder.serve().await {
                Ok(hub) => return Self(hub),
                Err(error) => failed.push(error.to_string()),
            }
        }
        panic!("the hub does not serve: {failed:?}");
    }
}

/// A segment started as `segment <its toml> <file> <control>` turns
/// `ready` in the hub whose control listener is `control`, then leaves.
/// Its own settings name no hub; the resolver's `file` does. The control
/// port is the hub's default, which these hubs do not use, so `<control>`
/// names it as a launcher's environment would.
async fn join_until_ready(scratch: &Scratch, file: PathBuf, control: SocketAddr) {
    let settings = scratch.path("only.toml");
    std::fs::write(
        &settings,
        format!(
            "listen = \"127.0.0.1:0\"\nrendezvous_ms = 5000\nhub_wait_ms = 5000\n[pool]\nname = \"{POOL}\"\nclass = \"{CLASS}\"\nregion = \"us-east-1\"\n"
        ),
    )
    .expect("settings");
    let overrides = scratch.path("control.toml");
    std::fs::write(
        &overrides,
        format!(
            "[hub]\ncontrol_port = {}\nheartbeat_ms = 200\n",
            control.port()
        ),
    )
    .expect("settings");
    // The segment's launch is not `Send`: it runs in this task, beside
    // the wait for it to turn ready.
    let joiner = gdl_runtime::run_segment::<Joiner>([
        settings.into_os_string(),
        file.into_os_string(),
        overrides.into_os_string(),
    ]);
    tokio::pin!(joiner);
    let ready = async {
        let started = Instant::now();
        loop {
            let listed = instances(control).await;
            if listed.instances.iter().any(|instance| {
                instance.segment == "only" && instance.phase == InstancePhase::Ready
            }) {
                return;
            }
            assert!(started.elapsed() < WAIT, "not ready in time: {listed:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::select! {
        ended = &mut joiner => panic!("the joiner ended before it was ready: {ended:?}"),
        () = ready => {}
    }
    LEAVE.notify_one();
    tokio::time::timeout(WAIT, joiner)
        .await
        .expect("the joiner leaves in time")
        .expect("the joiner served");
}

/// The members of the hub whose control listener is `control`, as
/// `GET /v1/ext/gdl-pool/instances` lists them.
async fn instances(control: SocketAddr) -> Instances {
    reqwest::get(format!(
        "http://{control}/v1/ext/gdl-pool{}",
        gdl_watch::INSTANCES_ROUTE
    ))
    .await
    .expect("the hub answers")
    .json()
    .await
    .expect("an instance list")
}

/// A pool the directory has never held, or one that withdrew, writes
/// nothing, leaves a file already at the path as it was, and exits
/// non-zero with one line.
#[tokio::test]
async fn an_unknown_or_withdrawn_pool_writes_nothing() {
    let scratch = Scratch::new();
    let directory = DirDirectory::new(scratch.pools());
    directory
        .publish(record("gone-01", PoolState::Ready, Duration::from_secs(30)))
        .await
        .expect("published");
    directory
        .withdraw(Withdrawal {
            class: CLASS.into(),
            pool: "gone-01".into(),
            epoch: 1,
            seq: 2,
            properties_version: 0,
        })
        .await
        .expect("withdrawn");
    let file = scratch.path("gdl-hub.toml");
    std::fs::write(&file, "kept").expect("written");
    for pool in ["nowhere-01", "gone-01"] {
        let output = scratch.resolve(&format!("{CLASS}/{pool}"), &file, &[]);
        let said = one_line(&output);
        assert!(!output.status.success(), "{pool}: {said}");
        assert!(said.contains(pool), "{said}");
        assert_eq!(std::fs::read_to_string(&file).expect("the file"), "kept");
    }
}

/// A record whose hub sends no heartbeat within its lease, as one a dead
/// hub left, writes nothing: the resolver waits one lease, on its own
/// clock, from when it first saw the record.
#[tokio::test]
async fn a_record_whose_lease_passed_writes_nothing() {
    let scratch = Scratch::new();
    let lease = Duration::from_millis(500);
    DirDirectory::new(scratch.pools())
        .publish(record(POOL, PoolState::Ready, lease))
        .await
        .expect("published");
    let file = scratch.path("gdl-hub.toml");
    let started = Instant::now();
    let output = scratch.resolve(&format!("{CLASS}/{POOL}"), &file, &[]);
    let said = one_line(&output);
    assert!(!output.status.success(), "{said}");
    assert!(said.contains("lease"), "{said}");
    assert!(started.elapsed() >= lease, "gave up before the lease");
    assert!(!file.exists(), "wrote {file:?}");
}

/// A draining pool, or one scaled to 0, has no hub to join: nothing is
/// written, and the resolver says so at once.
#[tokio::test]
async fn a_draining_or_scaled_down_pool_writes_nothing() {
    let scratch = Scratch::new();
    let directory = DirDirectory::new(scratch.pools());
    directory
        .publish(record(POOL, PoolState::Draining, Duration::from_secs(30)))
        .await
        .expect("published");
    let scaled = PoolRecord {
        presence: Presence::ScaledToZero,
        ..record("idle-01", PoolState::Unavailable, Duration::from_secs(30))
    };
    directory.publish(scaled).await.expect("published");
    let file = scratch.path("gdl-hub.toml");
    for (pool, why) in [(POOL, "draining"), ("idle-01", "scaled to 0")] {
        let started = Instant::now();
        let output = scratch.resolve(&format!("{CLASS}/{pool}"), &file, &[]);
        let said = one_line(&output);
        assert!(!output.status.success(), "{said}");
        assert!(said.contains(why), "{said}");
        assert!(started.elapsed() < Duration::from_secs(5), "waited a lease");
        assert!(!file.exists(), "wrote {file:?}");
    }
}

/// What the resolver cannot run is refused with one line that names what
/// to change: a target that is not `<class>/<pool>`, no `--write`, and a
/// backend that a process outside a hub cannot read.
#[test]
fn the_resolver_refuses_what_it_cannot_run() {
    let scratch = Scratch::new();
    let file = scratch.path("gdl-hub.toml");
    for (args, extra, expected) in [
        (
            vec!["--resolve", "model-x-01", "--write", "f.toml"],
            vec![],
            "<class>/<pool>",
        ),
        // A name the directory could not hold, which the `dir` backend
        // would read as a part of a path.
        (
            vec!["--resolve", "../model-x-01", "--write", "f.toml"],
            vec![],
            "class name `..` must be",
        ),
        (
            vec!["--resolve", "model_x/Model.X", "--write", "f.toml"],
            vec![],
            "pool name `Model.X` must be",
        ),
        (vec!["--resolve", "model_x/model-x-01"], vec![], "--write"),
        (
            vec!["--resolve", "model_x/model-x-01", "--write", "f.toml"],
            vec![("GDL_POOL__DIRECTORY", "relay")],
            "GDL_POOL__DIRECTORY",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_gdl-hub"))
            .args(&args)
            .current_dir(scratch.dir.path())
            .env("GDL_POOL__DIRECTORY", "dir")
            .env("GDL_POOL__DIR", scratch.pools())
            .envs(extra)
            .stdin(Stdio::null())
            .output()
            .expect("the binary runs");
        let said = one_line(&output);
        assert!(!output.status.success(), "{args:?}: {said}");
        assert!(said.contains(expected), "{args:?}: {said}");
    }
    assert!(!file.exists());
    assert!(!scratch.path("f.toml").exists());
}

/// A directory the resolver may only watch: a write through it fails the
/// test. The pool's hub writes through its own client.
struct WatchOnly(DirDirectory);

impl PoolDirectory for WatchOnly {
    type Watch = DirWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        panic!("the resolver published {record:?}")
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        panic!("the resolver withdrew {withdrawal:?}")
    }

    async fn last_write(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<Option<LastWrite>, DirectoryError> {
        panic!("the resolver read the last write of {class}/{pool}, a writer's read")
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch {
        self.0.watch(class, from)
    }
}

/// The resolver only reads: it watches the directory, and a heartbeat
/// that another client writes is what lets it resolve the hub.
#[tokio::test]
async fn the_resolver_only_watches_the_directory() {
    let scratch = Scratch::new();
    let hub = DirDirectory::new(scratch.pools());
    let first = PoolRecord {
        hub: "[fd00::7]:1337".into(),
        ..record(POOL, PoolState::Unavailable, Duration::from_secs(30))
    };
    hub.publish(first.clone()).await.expect("published");
    let reader = WatchOnly(DirDirectory::new(scratch.pools()));
    let resolving = gdl_hub::resolve_hub(&reader, CLASS, POOL);
    tokio::pin!(resolving);
    // No heartbeat yet: the resolver waits, on the first record alone.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut resolving)
            .await
            .is_err(),
        "resolved before a heartbeat"
    );
    hub.publish(PoolRecord { seq: 2, ..first })
        .await
        .expect("the heartbeat");
    let resolved = tokio::time::timeout(WAIT, resolving)
        .await
        .expect("resolved within the wait")
        .expect("resolved");
    assert_eq!((resolved.host(), resolved.public_port()), ("fd00::7", 1337));
}

/// A directory whose one watch yields what the test sends it, when the
/// test sends it.
struct Sent(std::sync::Mutex<Option<futures::channel::mpsc::UnboundedReceiver<PoolEvent>>>);

impl PoolDirectory for Sent {
    type Watch = futures::stream::BoxStream<'static, Result<PoolEvent, DirectoryError>>;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        panic!("the resolver published {record:?}")
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        panic!("the resolver withdrew {withdrawal:?}")
    }

    fn watch(&self, _class: &str, _from: Option<ResumeToken>) -> Self::Watch {
        use futures::StreamExt;
        let events = self.0.lock().unwrap().take().expect("one watch");
        events.map(Ok).boxed()
    }
}

/// A heartbeat the resolver first reads after the lease that ran from its
/// first sight of the record, as when the resolver was not scheduled for
/// that long, is refused like one that never came: the lease is measured
/// on the resolver's clock, and it passed.
#[tokio::test]
async fn a_heartbeat_read_after_the_lease_writes_nothing() {
    let lease = Duration::from_millis(100);
    let first = record(POOL, PoolState::Ready, lease);
    let (send, events) = futures::channel::mpsc::unbounded();
    let directory = Sent(std::sync::Mutex::new(Some(events)));
    let resolving = gdl_hub::resolve_hub(&directory, CLASS, POOL);
    tokio::pin!(resolving);
    send.unbounded_send(PoolEvent::Current(first.clone()))
        .unwrap();
    send.unbounded_send(PoolEvent::Synced {
        token: ResumeToken::new(CLASS, 1),
        snapshot: true,
    })
    .unwrap();
    // The resolver reads the snapshot and waits for a heartbeat.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut resolving)
            .await
            .is_err(),
        "resolved before a heartbeat"
    );
    // Not polled for twice the lease, by when the heartbeat is queued.
    tokio::time::sleep(lease * 2).await;
    send.unbounded_send(PoolEvent::Put {
        record: PoolRecord { seq: 2, ..first },
        token: ResumeToken::new(CLASS, 2),
    })
    .unwrap();
    let result = tokio::time::timeout(WAIT, resolving)
        .await
        .expect("the resolver returns");
    let error = result.expect_err("a heartbeat read after the lease is refused");
    assert!(error.to_string().contains("lease"), "{error}");
}

/// A heartbeat that arrives beside another pool's change resolves the
/// hub: an event of another pool of the class does not stop it.
#[tokio::test]
async fn another_pools_change_does_not_stop_a_heartbeat() {
    let first = record(POOL, PoolState::Ready, Duration::from_secs(30));
    let heartbeat = PoolRecord {
        seq: 2,
        ..first.clone()
    };
    let other = record("other-01", PoolState::Ready, Duration::from_secs(30));
    let (send, events) = futures::channel::mpsc::unbounded();
    let directory = Sent(std::sync::Mutex::new(Some(events)));
    let resolving = gdl_hub::resolve_hub(&directory, CLASS, POOL);
    tokio::pin!(resolving);
    send.unbounded_send(PoolEvent::Current(first)).unwrap();
    send.unbounded_send(PoolEvent::Synced {
        token: ResumeToken::new(CLASS, 1),
        snapshot: true,
    })
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut resolving)
            .await
            .is_err(),
        "resolved before a heartbeat"
    );
    // Queued together while the resolver was not running.
    for (position, record) in [(2, other), (3, heartbeat)] {
        send.unbounded_send(PoolEvent::Put {
            record,
            token: ResumeToken::new(CLASS, position),
        })
        .unwrap();
    }
    let resolved = tokio::time::timeout(WAIT, resolving)
        .await
        .expect("the resolver returns")
        .expect("another pool's change does not stop the heartbeat");
    assert_eq!(
        (resolved.host(), resolved.public_port()),
        ("127.0.0.1", 1337)
    );
}

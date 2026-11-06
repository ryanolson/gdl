//! The recipes of a fleet's Slurm sites, from a spec prepared as for
//! Kubernetes: the same settings files, and the same `GDL_POOL__*`
//! variables, with the addresses srt-slurm fills in when each process
//! starts (`{node_ip}`, `{head_ip}`). Each recipe is one JSON object,
//! which YAML reads as it is.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use gdl_model::{
    FleetBackend, HUB_CONTROL_PORT, HUB_PUBLIC_PORT, PoolSettings, Problem, SlurmSite,
};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};

use crate::deployment::etcd;
use crate::deployment::k8s::{Named, Naming, Prepared, SYSTEM_HOST, image, is_system, parse_port};
use crate::deployment::pools::{self, DYNAMODB_LOCAL};
use crate::deployment::variables::{Names, pool_variables, string};
use crate::deployment::{Bundle, Published, check_segment_name, io, reference};
use crate::{Error, Files, emit, export::image_problem};

/// The binary of every gdl image.
const BINARY: &str = gdl_k8s::BINARY;

/// Where a pool's job mounts the pool's settings: `<run_dir>/<fleet>/<pool>`.
const SETTINGS: &str = "/gdl";

/// Where every job mounts the fleet's directory on the shared file system,
/// `<run_dir>/<fleet>`: the directory job to write its store's address
/// there, and the others, read-only, to read it.
const FLEET_DIR: &str = "/fleet";

/// The fleet's pointer to its store, a DynamoDB Local or etcd, in
/// [`FLEET_DIR`]: a link to the latest directory job's file of its store's
/// address.
const POINTER: &str = "directory.endpoint";

/// The file a joining job's `resolve` writes, in the job's logs directory,
/// which srt-slurm mounts at `/logs` and its file probe reads.
const RESOLVED: &str = "gdl-hub.toml";

/// DynamoDB Local's port.
const LOCAL_PORT: u16 = 8000;

/// The names of a recipe's own services: a segment may not take one.
const HUB: &str = "hub";
const SPARE: &str = "spare";
const RESOLVE: &str = "resolve";
const OWN: [&str; 3] = [HUB, SPARE, RESOLVE];

/// How long srt-slurm waits for each probe, in seconds. A segment's covers
/// a build that loads a model; Compose gives one as long.
const HUB_READY_S: u32 = 300;
const SEGMENT_READY_S: u32 = 1800;
const LOCAL_READY_S: u32 = 300;

/// The recipes of `fleet`'s Slurm sites from `prepared`, each image named
/// by `naming`. [`super::slurm_files`] has refused what keeps a pool off
/// its site before the spec was prepared.
pub(super) fn render(
    prepared: &Prepared,
    fleet: &gdl_model::Fleet,
    naming: &Naming<'_>,
) -> Result<Files, Error> {
    let fleet_name = fleet.name().ok_or_else(|| {
        Error::new("the fleet has no `[fleet] name`, which names every job it runs")
    })?;
    let mut render = Render {
        fleet,
        fleet_name,
        naming,
        bundle: prepared.bundle.clone(),
        files: Files::new(),
        problems: Vec::new(),
        store: Store::of(fleet),
    };
    let pools: Vec<PoolSettings> = fleet
        .pools()
        .filter(|pool| site_of(fleet, &pool.name).is_some())
        .collect();
    let hub_repository = emit::package_name(&render.bundle.graph, "hub");
    for laid in prepared.spec.layouts() {
        let ours: Vec<&PoolSettings> = pools
            .iter()
            .filter(|pool| pool.layout.as_deref().unwrap_or(laid.name) == laid.name)
            .collect();
        if ours.is_empty() {
            continue;
        }
        let layout = prepared
            .layouts
            .get(laid.name)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::new(laid.named("the placement does not fit the graph".into())))?;
        let deployment =
            gdl_model::DeploymentSpec::new(prepared.manifest.clone(), laid.placement.clone())
                .map_err(io)?;
        let deployment = serde_json::to_string_pretty(&deployment).map_err(io)?;
        // Each segment that a pool of the layout runs, read once, in the
        // placement's order, which is the order its services start in.
        let mut segments = Vec::new();
        for (segment, _) in laid.placement.segments() {
            let class = layout.class_of(segment);
            if !ours.iter().any(|pool| class == Some(pool.class.as_str())) {
                continue;
            }
            if let Some(read) = render.segment(&laid, segment)? {
                segments.push((class, read));
            }
        }
        for pool in ours {
            let members: Vec<&Segment> = segments
                .iter()
                .filter(|(class, _)| *class == Some(pool.class.as_str()))
                .map(|(_, segment)| segment)
                .collect();
            render.pool(pool, &deployment, &members, &hub_repository)?;
        }
    }
    if let Some(directory) = fleet.directory().filter(|d| d.runs_store())
        && let Some(name) = directory.cluster.as_deref()
        && let Some(site) = fleet.cluster(name).and_then(gdl_model::Cluster::slurm)
    {
        let tables = match &directory.backend {
            FleetBackend::Dynamodb {
                records_table,
                lists_table,
                ..
            } => Some((records_table.as_str(), lists_table.as_str())),
            FleetBackend::Etcd => None,
        };
        render.directory(name, site, tables, &hub_repository)?;
    }
    if !render.problems.is_empty() {
        return Err(Error::all(render.problems));
    }
    Ok(render.files)
}

/// The Slurm site that runs pool `pool`, by its name.
fn site_of<'f>(fleet: &'f gdl_model::Fleet, pool: &str) -> Option<(&'f str, &'f SlurmSite)> {
    let (name, cluster) = fleet.cluster_of(pool)?;
    Some((name, cluster.slurm()?))
}

/// The DynamoDB Local's address at `host`.
fn local_address(host: &str) -> String {
    format!("http://{host}:{LOCAL_PORT}")
}

/// One render of a fleet's Slurm sites: what is written so far, and what is
/// wrong.
struct Render<'f> {
    fleet: &'f gdl_model::Fleet,
    fleet_name: &'f str,
    naming: &'f Naming<'f>,
    bundle: Bundle,
    files: Files,
    problems: Vec<Problem>,
    /// The store the fleet's directory job runs.
    store: Store,
}

/// One segment of a layout as a job runs it.
struct Segment {
    name: String,
    /// Its image's repository: the package it builds.
    repository: String,
    /// Its settings file.
    config: String,
    /// Its system listener's port and liveness path, which srt-slurm
    /// probes.
    port: u16,
    live: String,
    /// Every port its process binds on its node, beside the one the OS
    /// chooses for its velo listener: its system port, and each port it
    /// publishes, by protocol.
    listeners: Vec<Listener>,
}

/// A port a process binds, and its protocol.
type Listener = (u16, String);

/// The processes of one service of a pool's job: on how many nodes, each
/// binding what.
struct Placed<'s> {
    name: &'s str,
    nodes: u32,
    listeners: &'s [Listener],
}

impl Render<'_> {
    /// Segment `segment` of layout `laid` as a job runs it. `None`, with
    /// the problem noted, when a job cannot run it.
    fn segment(
        &mut self,
        laid: &crate::laid::Laid<'_>,
        segment: &str,
    ) -> Result<Option<Segment>, Error> {
        check_segment_name(segment)?;
        if OWN.contains(&segment) {
            self.problems.push(Problem::at(
                Some(format!("segment.{segment}")),
                None,
                format!(
                    "segment `{segment}` takes the name of a service that a pool's recipe has of its own (`{}`): rename the segment to run it on a Slurm site",
                    OWN.join("`, `")
                ),
            ));
            return Ok(None);
        }
        let package = laid.package(&self.bundle.graph, segment);
        if let Some(problem) = image_problem(segment, &package) {
            return Err(Error::new(problem));
        }
        let secrets = self.bundle.secrets(segment);
        if !secrets.is_empty() {
            self.problems.push(Problem::at(
                Some(format!("deployment.segment.{segment}")),
                Some("secrets".into()),
                format!(
                    "segment `{segment}` mounts secret `{}`, and no Slurm job is given a secret yet: run the segment's pools on a Kubernetes cluster, or leave the secret out",
                    secrets.join("`, `")
                ),
            ));
            return Ok(None);
        }
        let path = laid.dir().join(segment).join("config.toml");
        self.bundle.segment_config(&path, segment, None, None)?;
        let mut table: toml::Table = toml::from_str(&self.bundle.files[&path]).map_err(io)?;
        // srt-slurm probes each instance from the head node, at its node's
        // address: a listener on loopback alone would never answer.
        if let toml::Value::Table(system) = table
            .entry("system")
            .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        {
            system.insert("host".into(), SYSTEM_HOST.into());
        }
        let config = toml::to_string_pretty(&table).map_err(io)?;
        let settings = match gdl_k8s::Settings::of(&table) {
            Ok(settings) => settings,
            Err(message) => {
                self.problems.push(Problem::at(
                    Some(format!("deployment.segment.{segment}.config.system")),
                    Some("port".into()),
                    message,
                ));
                return Ok(None);
            }
        };
        let mut listeners = vec![(settings.port, "TCP".to_string())];
        for published in self.bundle.ports(segment) {
            match parse_port(published) {
                // A host publication of the system port is Compose's route
                // to it; the process binds the port once.
                Ok(port) if is_system(&port, &settings) && Published::of(published).host => {}
                Ok(port) => {
                    let listener = (port.port, format!("{:?}", port.protocol).to_uppercase());
                    if !listeners.contains(&listener) {
                        listeners.push(listener);
                    }
                }
                Err(message) => self.problems.push(Problem::at(
                    Some(format!("deployment.segment.{segment}")),
                    Some("ports".into()),
                    message,
                )),
            }
        }
        Ok(Some(Segment {
            name: segment.to_string(),
            repository: package,
            config,
            port: settings.port,
            live: settings.live,
            listeners,
        }))
    }

    /// The recipes and settings of `pool`, which runs `segments`.
    fn pool(
        &mut self,
        pool: &PoolSettings,
        deployment: &str,
        segments: &[&Segment],
        hub_repository: &str,
    ) -> Result<(), Error> {
        let (site_name, site) = site_of(self.fleet, &pool.name)
            .expect("render passes over every pool off a Slurm site");
        let dir = Path::new(site_name).join(&pool.name);
        let mut settings: Files =
            Files::from([(PathBuf::from("deployment.json"), deployment.into())]);
        for segment in segments {
            settings.insert(
                PathBuf::from(format!("{}.toml", segment.name)),
                segment.config.clone(),
            );
        }
        let hub_image = self.pulled(site_name, hub_repository)?;
        // The hub drains its pool on the signal Slurm sends before the
        // time limit, which `check` makes long enough for its stop wait.
        let mut stopping = pool.clone();
        stopping.drain_on_stop = Some(
            pool.drain_on_stop
                .unwrap_or(PoolSettings::DEFAULT_STOP_WAIT),
        );
        let mut hub_env = pool_variables(
            &pools::table(
                &stopping,
                format!("{{node_ip}}:{HUB_PUBLIC_PORT}"),
                None,
                false,
                None,
            )?,
            Names::Shell,
        );
        let mut segment_env = pool_variables(
            &pools::table(
                pool,
                format!("{{head_ip}}:{HUB_PUBLIC_PORT}"),
                None,
                false,
                None,
            )?,
            Names::Shell,
        );
        // Each process reads etcd's address when it starts (`read_address`),
        // so the render names none.
        for env in [&mut hub_env, &mut segment_env] {
            env.remove("GDL_POOL__ENDPOINTS");
        }
        // A segment joining from another job finds its hub in the file
        // `resolve` writes, and a variable would override that file.
        let mut join_env = segment_env.clone();
        join_env.remove("GDL_POOL__HUB");
        join_env.insert("GDL_LISTEN".into(), string("{node_ip}:0").into());
        segment_env.insert("GDL_HUB__HOST".into(), string("{head_ip}").into());
        segment_env.insert("GDL_LISTEN".into(), string("{node_ip}:0").into());

        let mut services = vec![json!({
            "name": HUB,
            "type": "generic",
            "placement": {"node": "head"},
            "critical": true,
            "terminal": true,
            "container": hub_image,
            "command": [BINARY, format!("{SETTINGS}/deployment.json")],
            "env": hub_env,
            "preamble": format!(
                "{}; {}; {}",
                read_address(&self.fleet_dir(site), self.store),
                hub_address(&format!("{}/{}", self.fleet_dir(site), pool.name)),
                leave_by("GDL_POOL__LEAVE_BY")
            ),
            "readiness": {
                "http": {"port": HUB_PUBLIC_PORT, "path": "/v1/health", "status": 200},
                "timeout_seconds": HUB_READY_S,
            },
        })];
        // What each process binds, by the service it is, in the order the
        // nodes are carved: the hub on the head node, the first.
        let mut placed: Vec<Placed<'_>> = Vec::new();
        let hub_ports = vec![
            (HUB_PUBLIC_PORT, "TCP".to_string()),
            (HUB_CONTROL_PORT, "TCP".to_string()),
        ];
        let mut images = BTreeMap::new();
        for segment in segments {
            let image = self.pulled(site_name, &segment.repository)?;
            images.insert(segment.name.as_str(), image.clone());
            let nodes = pool.quorum(&segment.name);
            if nodes == 0 {
                continue;
            }
            services.push(json!({
                "name": segment.name,
                "type": "generic",
                "nodes": nodes,
                "container": image,
                "command": [BINARY, format!("{SETTINGS}/{}.toml", segment.name)],
                "env": segment_env,
                "preamble": segment_preamble(&self.fleet_dir(site), self.store),
                "readiness": segment_readiness(segment),
            }));
            placed.push(Placed {
                name: &segment.name,
                nodes,
                listeners: &segment.listeners,
            });
        }
        let spare = self
            .fleet
            .slurm(&pool.name)
            .map_or(0, |slurm| slurm.spare_nodes);
        if spare > 0 {
            services.push(json!({
                "name": SPARE,
                "type": "generic",
                "nodes": spare,
                "container": hub_image,
                "command": ["sleep", "infinity"],
            }));
        }
        if placed.is_empty() && spare == 0 {
            self.problems.push(Problem::at(
                Some(format!("pool.{}", pool.name)),
                None,
                format!(
                    "pool `{}` runs on Slurm site `{site_name}` with no node: its `ready` gives each segment of its class 0 instances and it holds no `slurm.spare_nodes`, so its job has no node for its hub",
                    pool.name
                ),
            ));
        }
        self.check_ports(pool, &hub_ports, &placed);

        let mut directives = self.draining(site);
        if site.no_kill {
            directives.insert("no-kill".into(), "".into());
        }
        let mut job = self.recipe(
            format!("gdl-{}-{}", self.fleet_name, pool.name),
            site,
            self.limits(site, pool),
            &hub_image,
            services,
            self.mounts(site, pool),
            directives,
        );
        let pool_dir = format!("{}/{}", self.fleet_dir(site), pool.name);
        job["host_setup"] = job_end(&pool_dir);
        job["host_setup"]["teardown"]
            .as_array_mut()
            .expect("job_end writes a teardown")
            .push(format!("rm -f {}/{HUB_ADDRESS}", sh_quote(&pool_dir)).into());
        let fleet_name = self.fleet_name;
        // Each job's hash covers the settings it reads: a pool's, all of
        // them; a join job's, its segment's alone.
        let comment = |kind: &str, recipe: &Value, read: &Files| {
            format!(
                "gdl:{fleet_name}:{}:{kind}:{}",
                pool.name,
                hash(recipe, read)
            )
        };
        let said = comment("pool", &job, &settings);
        let job = with_comment(job, said);
        self.files.insert(dir.join("pool.yaml"), text(&job)?);

        for segment in segments {
            let join = self.join(site, pool, segment, &hub_image, &images, &join_env);
            let own = PathBuf::from(format!("{}.toml", segment.name));
            let read = Files::from([(own.clone(), settings[&own].clone())]);
            let said = comment("join", &join, &read);
            let join = with_comment(join, said);
            self.files.insert(
                dir.join(format!("join-{}.yaml", segment.name)),
                text(&join)?,
            );
        }
        for (path, text) in settings {
            self.files.insert(dir.join(path), text);
        }
        Ok(())
    }

    /// The job that adds one instance of `segment` to running pool `pool`:
    /// `resolve` reads where the pool's hub is from the directory when the
    /// job starts, not when it is queued, since a queued job may outlive
    /// the hub it was queued for; then the segment starts with that file
    /// over its settings, and the job ends when it leaves.
    fn join(
        &self,
        site: &SlurmSite,
        pool: &PoolSettings,
        segment: &Segment,
        hub_image: &str,
        images: &BTreeMap<&str, String>,
        env: &Map<String, Value>,
    ) -> Value {
        let resolved = format!("/logs/{RESOLVED}");
        // srt-slurm probes the file, then asks whether the process lives:
        // one that wrote the file and exited in between would fail the
        // job. `resolve` waits, once it has written, for the job's end.
        // It is not critical: the job's `TERM` before its time limit ends
        // the wait, and srt-slurm would take a critical service's end on a
        // signal as a failure, and stop the segment's drain. One that
        // fails before it writes still fails the job, by its probe.
        let script = format!(
            "{BINARY} --resolve {}/{} --write {resolved} && exec sleep infinity",
            pool.class, pool.name
        );
        let services = vec![
            json!({
                "name": RESOLVE,
                "type": "generic",
                "placement": {"node": "head"},
                "container": hub_image,
                "command": ["bash", "-c", script],
                "env": env,
                "preamble": read_address(&self.fleet_dir(site), self.store),
                "readiness": {"file": {"path": RESOLVED}, "timeout_seconds": HUB_READY_S},
            }),
            json!({
                "name": segment.name,
                "type": "generic",
                "nodes": 1,
                "terminal": true,
                "container": images[segment.name.as_str()],
                "command": [BINARY, format!("{SETTINGS}/{}.toml", segment.name), resolved],
                "env": env,
                "preamble": segment_preamble(&self.fleet_dir(site), self.store),
                "readiness": segment_readiness(segment),
            }),
        ];
        let mut job = self.recipe(
            format!(
                "gdl-{}-{}-join-{}",
                self.fleet_name, pool.name, segment.name
            ),
            site,
            self.limits(site, pool),
            hub_image,
            services,
            self.mounts(site, pool),
            self.draining(site),
        );
        job["host_setup"] = job_end(&format!("{}/{}", self.fleet_dir(site), pool.name));
        job
    }

    /// The directory job on site `name`, which runs the fleet's store: a
    /// DynamoDB Local on a node of its own for `tables`, or, without them,
    /// srt-slurm's etcd on the head node, with the backend's flags. A step
    /// on the head node writes the store's address, to a file of this
    /// job's id, written beside it and renamed into place: under DynamoDB,
    /// the tables step, once the tables exist; under etcd, once srt-slurm
    /// has found etcd's port open, before it starts any other service. A
    /// requeued job would keep its id, so no job is requeued, and the step
    /// removes the file first. etcd keeps its data in srt-slurm's data
    /// directory on the node's own disk, which each job starts empty: the
    /// directory lasts as long as the job, as a Local in memory does.
    fn directory(
        &mut self,
        name: &str,
        site: &SlurmSite,
        tables: Option<(&str, &str)>,
        hub_repository: &str,
    ) -> Result<(), Error> {
        let hub_image = self.pulled(name, hub_repository)?;
        let (address, step) = match tables {
            Some(_) => (local_address("{node_ip}"), format!("{BINARY} --tables && ")),
            None => (format!("http://{{node_ip}}:{}", etcd::PORT), String::new()),
        };
        let file = |suffix: &str| format!("{FLEET_DIR}/directory.$SLURM_JOB_ID.{suffix}");
        // The job's own file, for the driver that submitted it; then the
        // fleet's pointer, for the pools' processes: a link to that file,
        // renamed into place, so the latest job's pointer wins.
        let script = format!(
            "rm -f {endpoint} && {step}echo {address} > {tmp} && mv {tmp} {endpoint} && ln -sfn directory.$SLURM_JOB_ID.endpoint {ptr} && mv {ptr} {FLEET_DIR}/{POINTER}",
            endpoint = file("endpoint"),
            tmp = file("tmp"),
            ptr = file("ptr"),
        );
        let services = match tables {
            Some((records_table, lists_table)) => {
                let env: Map<String, Value> = [
                    ("GDL_POOL__DIRECTORY", "dynamodb".to_string()),
                    ("GDL_POOL__RECORDS_TABLE", records_table.to_string()),
                    ("GDL_POOL__LISTS_TABLE", lists_table.to_string()),
                    ("GDL_POOL__AWS_REGION", site.region.clone()),
                    ("GDL_POOL__ENDPOINT", address.clone()),
                ]
                .into_iter()
                .map(|(name, value)| (name.to_string(), string(&value).into()))
                .collect();
                vec![
                    json!({
                        "name": "dynamodb",
                        "type": "generic",
                        "nodes": 1,
                        "critical": true,
                        "terminal": true,
                        "container": DYNAMODB_LOCAL,
                        "command": [
                            "java", "-jar", "/home/dynamodblocal/DynamoDBLocal.jar",
                            "-inMemory", "-sharedDb", "-port", LOCAL_PORT.to_string(),
                        ],
                        "readiness": {"tcp": {"port": LOCAL_PORT}, "timeout_seconds": LOCAL_READY_S},
                    }),
                    json!({
                        "name": "tables",
                        "type": "generic",
                        "placement": {"node": "head"},
                        "critical": true,
                        "container": hub_image,
                        "command": ["bash", "-c", script],
                        "env": env,
                    }),
                ]
            }
            // srt-slurm's etcd runs its own binary, `/configs/etcd`, in the
            // job's container, and gives clients the node's IP; the job
            // ends when etcd does.
            None => vec![
                json!({
                    "name": "etcd",
                    "type": "etcd",
                    "placement": {"node": "head"},
                    "terminal": true,
                    "args": etcd::FLAGS,
                }),
                json!({
                    "name": "pointer",
                    "type": "generic",
                    "placement": {"node": "head"},
                    "critical": true,
                    "container": hub_image,
                    "command": ["bash", "-c", script],
                }),
            ],
        };
        let limits = site
            .time_limit
            .as_ref()
            .map(|limit| Map::from_iter([("time_limit".to_string(), Value::from(limit.clone()))]))
            .unwrap_or_default();
        let mut job = self.recipe(
            format!("gdl-{}-directory", self.fleet_name),
            site,
            limits,
            &hub_image,
            services,
            vec![format!("{}:{FLEET_DIR}", self.fleet_dir(site))],
            self.directives(),
        );
        // When the job ends, its Local is gone: it removes its own file,
        // so a pointer to it dangles and no pool starts with it. It never
        // touches the pointer, which a later job may have taken.
        let dir = sh_quote(&self.fleet_dir(site));
        job["host_setup"] = json!({
            "nodes": "all",
            "commands": [],
            "teardown": [format!("rm -f {dir}/directory.$SLURM_JOB_ID.endpoint")],
        });
        let comment = format!(
            "gdl:{}:-:directory:{}",
            self.fleet_name,
            hash(&job, &Files::new())
        );
        let job = with_comment(job, comment);
        self.files
            .insert(Path::new(name).join("directory.yaml"), text(&job)?);
        Ok(())
    }

    /// Refuse a node of `pool`'s job on which two of its processes bind
    /// one port: the hub's `hub` and the first instance of the first
    /// segment on the head node, and the ports of each instance.
    fn check_ports(&mut self, pool: &PoolSettings, hub: &[Listener], placed: &[Placed<'_>]) {
        let mut on: BTreeMap<u32, Vec<(&str, &Listener)>> = BTreeMap::new();
        on.entry(0)
            .or_default()
            .extend(hub.iter().map(|port| ("the hub", port)));
        let mut next = 0;
        for Placed {
            name,
            nodes,
            listeners,
        } in placed
        {
            for node in next..next + nodes {
                on.entry(node)
                    .or_default()
                    .extend(listeners.iter().map(|port| (*name, port)));
            }
            next += nodes;
        }
        for (node, ports) in on {
            let mut seen: BTreeMap<&Listener, &str> = BTreeMap::new();
            for (who, port) in ports {
                if let Some(other) = seen.insert(port, who) {
                    let named = |name: &str| match name {
                        "the hub" => "the hub".to_string(),
                        segment => format!("segment `{segment}`"),
                    };
                    self.problems.push(Problem::at(
                        Some(format!("pool.{}", pool.name)),
                        None,
                        format!(
                            "pool `{}`: on node {node} of its job, {} and {} both bind {} port {}: every gdl job takes its nodes whole and keeps the default ports, and the hub shares the head node with the first segment's first instance. Give the segment another port",
                            pool.name,
                            named(other),
                            named(who),
                            port.1,
                            port.0,
                        ),
                    ));
                }
            }
        }
    }

    /// The image of `repository` that site `site` runs, as Pyxis pulls it,
    /// refused unless Docker reads it as a reference.
    fn pulled(&self, site: &str, repository: &str) -> Result<String, Error> {
        let named = self.naming.named(site, repository)?;
        let image = image(named.registry, named.repository, named.tag, named.digest);
        reference::check_image(&image)
            .map_err(|why| Error::new(format!("[cluster.{site}] registry: {why}")))?;
        Ok(pulled(&named))
    }

    /// What a pool's job and its joining jobs mount: the pool's settings
    /// at [`SETTINGS`], and the fleet's directory, read-only, at
    /// [`FLEET_DIR`].
    fn mounts(&self, site: &SlurmSite, pool: &PoolSettings) -> Vec<String> {
        vec![
            format!("{}/{}:{SETTINGS}", self.fleet_dir(site), pool.name),
            format!("{}:{FLEET_DIR}:ro", self.fleet_dir(site)),
        ]
    }

    /// `<run_dir>/<fleet>` on `site`.
    fn fleet_dir(&self, site: &SlurmSite) -> String {
        format!("{}/{}", site.run_dir.trim_end_matches('/'), self.fleet_name)
    }

    /// `pool`'s partition and time limit, over `site`'s: srt-slurm takes
    /// `srtslurm.yaml`'s for each one unsaid.
    fn limits(&self, site: &SlurmSite, pool: &PoolSettings) -> Map<String, Value> {
        let own = self.fleet.slurm(&pool.name);
        let mut limits = Map::new();
        if let Some(partition) = own.and_then(|slurm| slurm.partition.as_ref()) {
            limits.insert("partition".into(), partition.clone().into());
        }
        if let Some(limit) = own
            .and_then(|slurm| slurm.time_limit.as_ref())
            .or(site.time_limit.as_ref())
        {
            limits.insert("time_limit".into(), limit.clone().into());
        }
        limits
    }

    /// The sbatch directives of every gdl job: its nodes whole, and never
    /// requeued.
    fn directives(&self) -> Map<String, Value> {
        let mut directives = Map::new();
        directives.insert("exclusive".into(), "".into());
        directives.insert("no-requeue".into(), "".into());
        directives
    }

    /// The sbatch directives of a job that runs gdl instances on `site`:
    /// [`Render::directives`], and `TERM` the site's lead before its time
    /// limit, on which each instance retires and the hub drains its pool.
    /// The directory's job has none: the pools withdraw their records
    /// from it as they drain.
    fn draining(&self, site: &SlurmSite) -> Map<String, Value> {
        let mut directives = self.directives();
        directives.insert(
            "signal".into(),
            format!("TERM@{}", site.signal_lead_s).into(),
        );
        directives
    }

    /// A services-only recipe. srt-slurm requires a model: its path is the
    /// fleet's directory, which its preflight and its runtime require to
    /// exist, and nothing reads.
    #[allow(clippy::too_many_arguments, reason = "the parts of one recipe")]
    fn recipe(
        &self,
        name: String,
        site: &SlurmSite,
        limits: Map<String, Value>,
        container: &str,
        services: Vec<Value>,
        mounts: Vec<String>,
        directives: Map<String, Value>,
    ) -> Value {
        json!({
            "schema": 2,
            "name": name,
            "slurm": limits,
            "model": {"path": self.fleet_dir(site), "container": container, "precision": "bf16"},
            "resources": {},
            "frontend": {"type": "none"},
            "services": services,
            "extra_mount": mounts,
            "sbatch_directives": directives,
            "observability": {"tachometer": {"enabled": false}},
        })
    }
}

/// The file a job's end time is written to, in its pool's directory: one
/// per job, since a pool's job and its joining jobs share the directory.
const JOB_END: &str = "job.$SLURM_JOB_ID.end";

/// The step srt-slurm runs on each node's host before any service starts,
/// which writes the job's end time, in Unix seconds, to [`JOB_END`] in
/// `dir`, the pool's directory on the shared file system; and removes it
/// once the job's services have stopped. This is where the end time comes
/// from: `SLURM_JOB_END_TIME` where the cluster's Slurm sets it, else the
/// end `squeue` gives for the job, which runs only on the host. A job with
/// neither, as one with no time limit, writes an empty file, and its
/// processes set no leave time. Each node writes the same time, through a
/// file of its own renamed into place.
fn job_end(dir: &str) -> Value {
    // The directory is quoted, and the job's id, which the shell fills in,
    // is not.
    let file = format!("{}/{JOB_END}", sh_quote(dir));
    let write = format!(
        "f={file}; end=\"${{SLURM_JOB_END_TIME:-}}\"; \
         if [ -z \"$end\" ]; then e=$(squeue -h -j \"$SLURM_JOB_ID\" -o %e 2>/dev/null); \
         case \"$e\" in [0-9]*) end=$(date -d \"$e\" +%s 2>/dev/null) ;; esac; fi; \
         t=\"$f.$(hostname).$$\"; echo \"$end\" > \"$t\" && mv \"$t\" \"$f\""
    );
    json!({
        "nodes": "all",
        "commands": [write],
        "teardown": [format!("rm -f {file}")],
    })
}

/// `text` as one word of a POSIX shell, whatever it holds: in single
/// quotes, each of its own written `'\''`.
fn sh_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

/// The file a pool's hub writes its addresses to, in its pool's
/// directory, for `cargo gdl deploy` and `down` on the login node: one per
/// job, since an old job's file may outlive it when its teardown does not
/// run.
pub(crate) const HUB_ADDRESS: &str = "hub.$SLURM_JOB_ID.address";

/// The part of a hub's preamble that writes [`HUB_ADDRESS`] in the pool's
/// directory, `dir` on the shared file system: its public and its control
/// address, on its node's address, which srt-slurm fills in. A hub that
/// cannot write it does not start: nothing could reach it.
fn hub_address(dir: &str) -> String {
    let file = format!("{SETTINGS}/{HUB_ADDRESS}");
    format!(
        "{{ printf '%s %s\\n' '{{node_ip}}:{HUB_PUBLIC_PORT}' '{{node_ip}}:{HUB_CONTROL_PORT}' > {file}.tmp && mv {file}.tmp {file}; }} || {{ echo \"gdl: the hub cannot write {dir}/{HUB_ADDRESS}, which tells cargo gdl where it is\" >&2; exit 1; }}"
    )
}

/// The preamble of a gdl process in a job: `variable`, its leave time,
/// `[runtime] leave_by` or `[pool] leave_by` in Unix milliseconds, from the
/// end time [`job_end`] wrote, when it wrote one. It drains before then.
fn leave_by(variable: &str) -> String {
    format!(
        "end=$(cat {SETTINGS}/{JOB_END} 2>/dev/null || true); case \"$end\" in ''|*[!0-9]*) ;; *) export {variable}=\"$end\"000 ;; esac"
    )
}

/// The preamble of a segment process, whose fleet's directory is
/// `fleet_dir`: the address of `store` ([`read_address`]); glibc's
/// tunables as every launcher gives them (`gdl_model::segment_tunables`),
/// when the job's environment holds none, and the user's, as given, when
/// it holds one, which a variable of the recipe's would replace; then its
/// leave time.
fn segment_preamble(fleet_dir: &str, store: Store) -> String {
    let tunables = gdl_model::GLIBC_TUNABLES;
    format!(
        "{}; export {tunables}=\"${{{tunables}-{}}}\"; {}",
        read_address(fleet_dir, store),
        gdl_model::segment_tunables(None),
        leave_by("GDL_RUNTIME__LEAVE_BY")
    )
}

/// The store that a fleet's directory job runs on a Slurm site, whose
/// address each gdl process reads from the fleet's pointer when it starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Store {
    /// A DynamoDB Local: `[directory] backend = "dynamodb"`, with
    /// `local = true`.
    Local,
    /// srt-slurm's etcd: `[directory] backend = "etcd"`.
    Etcd,
}

impl Store {
    /// The store of `fleet`'s directory. [`super::refusals`] refused a
    /// Slurm pool's fleet without one the site runs.
    fn of(fleet: &gdl_model::Fleet) -> Self {
        match fleet.directory().map(|directory| &directory.backend) {
            Some(FleetBackend::Etcd) => Self::Etcd,
            _ => Self::Local,
        }
    }

    /// What it is, as a message names it.
    fn what(self) -> &'static str {
        match self {
            Self::Local => "DynamoDB Local",
            Self::Etcd => "etcd",
        }
    }

    /// The export of its address, `$a`, as the backend's `[pool]` key
    /// reads it: one etcd member is a list of one.
    fn export(self) -> &'static str {
        match self {
            Self::Local => "export GDL_POOL__ENDPOINT=\"$a\"",
            Self::Etcd => "export GDL_POOL__ENDPOINTS=\"[\\\"$a\\\"]\"",
        }
    }
}

/// The start of a gdl process's preamble: the address of the fleet's
/// `store`, read from the pointer the directory job writes in `fleet_dir`,
/// mounted at [`FLEET_DIR`]: `GDL_POOL__ENDPOINT` for a DynamoDB Local,
/// `GDL_POOL__ENDPOINTS` for etcd. Read when the process starts, not
/// rendered: a new directory job moves no pool's recipe. With no pointer,
/// or one that is not `http://<host>:<port>`, the process does not start,
/// and says why in one line, even after a cluster's preamble of `set -e`,
/// which a failed read alone would end.
fn read_address(fleet_dir: &str, store: Store) -> String {
    format!(
        "a=$(cat {FLEET_DIR}/{POINTER} 2>/dev/null || true); case \"$a\" in http://?*:[0-9]*) {} ;; *) echo \"gdl: {fleet_dir}/{POINTER} names no {}: submit the fleet's directory job, and wait for it to write the file\" >&2; exit 1 ;; esac",
        store.export(),
        store.what(),
    )
}

/// The readiness of an instance of `segment`: its system listener live.
/// srt-slurm starts the instances of a job one at a time, each once the
/// one before is ready, and an instance answers its health only once every
/// segment of its class has instances: a class of two segments would wait
/// on health forever.
fn segment_readiness(segment: &Segment) -> Value {
    json!({
        "http": {"port": segment.port, "path": segment.live, "status": 200},
        "timeout_seconds": SEGMENT_READY_S,
    })
}

/// `named` as Pyxis pulls it: the host Docker reads in its name, then
/// `#`, then the rest, by its digest, or by its tag when no digest is
/// known (only as `images` judges a fleet before it builds). A name on
/// Docker Hub has no host, which Pyxis then takes.
fn pulled(named: &Named<'_>) -> String {
    let name = format!(
        "{}/{}",
        named.registry.trim_end_matches('/'),
        named.repository
    );
    let image = match reference::host_of(&name) {
        Some((host, rest)) => format!("{host}#{rest}"),
        None => match name.split_once('/') {
            Some(("docker.io" | "index.docker.io", rest)) => rest.to_string(),
            _ => name.clone(),
        },
    };
    match named.digest {
        Some(digest) => format!("{image}@{digest}"),
        None => format!("{image}:{}", named.tag),
    }
}

/// `recipe` with sbatch's `--comment` set to `comment`.
fn with_comment(mut recipe: Value, comment: String) -> Value {
    recipe["sbatch_directives"]["comment"] = comment.into();
    recipe
}

/// What a job runs, as the first 16 hex digits of a sha256 of its recipe
/// and the settings it mounts, so that a deploy can tell a job it would
/// submit again from one that changed.
fn hash(recipe: &Value, settings: &Files) -> String {
    let mut hash = Sha256::new();
    let mut field = |bytes: &[u8]| {
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    };
    field(b"gdl slurm job");
    field(recipe.to_string().as_bytes());
    for (path, text) in settings {
        field(path.to_string_lossy().as_bytes());
        field(text.as_bytes());
    }
    hash.finalize()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// `recipe` as its file holds it.
fn text(recipe: &Value) -> Result<String, Error> {
    let mut text = serde_json::to_string_pretty(recipe).map_err(io)?;
    text.push('\n');
    Ok(text)
}

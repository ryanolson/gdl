use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::pool::{check_pool_name, check_region_name, check_table_name};
use crate::{
    DirectoryBackend, Layout, Parent, PoolSettings, Problem, Problems, check_layout_name, read_toml,
};

mod cluster;
mod errors;
mod placement;
mod slurm;
mod written;

pub use cluster::{Cluster, KubernetesCluster};
pub use errors::{FleetError, FleetErrors, FleetTable};
pub use placement::{
    ClusterResources, HUB, PoolResources, Quantities, Quantity, Resources, Toleration,
};
pub use slurm::{PoolSlurm, SlurmReach, SlurmSite};

use cluster::{Header, check_clusters};
use slurm::check_time_limit;

/// A deployment's pools: `fleet.toml`. Not part of the placement's
/// identity: pools are runtime topology, like replicas. It says what each
/// class's pools need by default, which pools there are, with their
/// layout, class, region, parent, and quorum overrides, and the clusters
/// that run them; a bundle generator writes each pool's own settings from
/// it ([`Fleet::pool`]).
///
/// ```toml
/// [fleet]
/// name = "llm"                       # optional: labels the objects the fleet deploys
///
/// [cluster.use1]                     # optional: a Kubernetes cluster ([`KubernetesCluster`]), `kind = "kubernetes"` unsaid
/// context = "kind-use1"
/// namespace = "gdl-llm"
/// region = "us-east-1"
/// node_selector = { "gdl.dev/node-group" = "llm" }   # optional: beside the platform's OS and architecture
/// tolerations = [{ key = "dedicated", operator = "Equal", value = "llm", effect = "NoSchedule" }]
///
/// [cluster.use1.resources.default]   # optional: then every pod of the cluster gets all four
/// requests = { cpu = "250m", memory = "256Mi" }
/// limits = { cpu = "1", memory = "1Gi" }
/// [cluster.use1.resources.segment.e] # a segment's own, or `hub`, over the default
/// limits = { memory = "4Gi" }
///
/// [cluster.lab]                      # optional: a Slurm site ([`SlurmSite`]); `cargo gdl deploy` runs it from a login node
/// kind = "slurm"
/// region = "slurm-lab"
/// srtctl = "/shared/srt-slurm"       # the srt-slurm checkout on the login node
/// run_dir = "/shared/gdl-runs"       # shared by the login node and the compute nodes
/// registry = "registry.example/gdl"
/// platform = "linux/arm64"
///
/// [class.global]
/// ready = { a = 2, b = 2, c = 2 }   # default quorum of a pool of this class, per segment
/// requires = { model_x = 1 }         # optional: ready children of each class it sends to its health needs; 1 of each when unsaid
///
/// [pool.model-x-01-us-east-1]
/// layout = "regional"                # the spec's layout it runs: required when the spec has several
/// class = "model_x"
/// cluster = "use1"                   # the cluster that runs it, whose region it takes
/// region = "us-east-1"               # required without a cluster; with one, its region if given
/// home = "global-us-east-1"          # the parent pool, required when another class sends to this one: whose hub follows it
/// # parents = "global"               # or, in place of `home`, the parent class: every pool of it follows this one
/// ready = { e = 8 }                  # optional: overrides the class's default
/// resources.segment.e = { limits = { memory = "8Gi" } }   # optional: over its cluster's
/// # slurm = { partition = "gpu" }   # on a Slurm site, in place of `resources`: its job's partition, time limit, and spare nodes ([`PoolSlurm`])
///
/// [pool.model-x-01-us-east-1.dgd]   # optional: a DynamoGraphDeployment the pool owns ([`PoolDgd`])
/// file = "dynamo/dgd.yaml"           # relative to the fleet file
/// components = { e = "Frontend" }    # segment = the DGD component that runs it
///
/// [directory]                        # optional: without it, each child's hub reports to its parent's (`relay`)
/// backend = "dynamodb"               # every pool's hub publishes to two DynamoDB tables
/// records_table = "gdl-pools"
/// lists_table = "gdl-pool-lists"
/// local = true                       # optional: the bundle runs one DynamoDB Local for them
/// cluster = "use1"                   # with local, when the fleet names clusters: the one that runs it
/// # or, in place of the tables: backend = "etcd", an etcd the deployment runs, with `cluster` as for a Local
///
/// [metrics]                         # optional
/// pod_monitor = true                 # each cluster's objects include a PodMonitor for the Prometheus operator
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fleet {
    #[serde(rename = "fleet", default, skip_serializing_if = "Header::is_empty")]
    header: Header,
    #[serde(
        rename = "cluster",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    clusters: BTreeMap<String, Cluster>,
    #[serde(rename = "class", default, skip_serializing_if = "BTreeMap::is_empty")]
    classes: BTreeMap<String, ClassDefaults>,
    #[serde(rename = "pool", default, skip_serializing_if = "BTreeMap::is_empty")]
    pools: BTreeMap<String, PoolSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    directory: Option<FleetDirectory>,
    #[serde(default, skip_serializing_if = "FleetMetrics::is_empty")]
    metrics: FleetMetrics,
}

/// The fleet's `[metrics]` table: how a Prometheus finds what the fleet
/// serves. Read by `cargo gdl k8s`; Compose does not read it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMetrics {
    /// Whether each cluster's objects include a `PodMonitor` of the
    /// fleet's pods, for a cluster that runs the Prometheus operator.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pod_monitor: bool,
}

impl FleetMetrics {
    fn is_empty(&self) -> bool {
        !self.pod_monitor
    }
}

/// The fleet's `[directory]` table: where every pool's hub publishes its
/// record. Without it, each child pool's hub reports to its parent's
/// (`relay`). Read from, and written as, one table whose `backend` names
/// the store and selects the keys it takes ([`FleetBackend`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DirectoryTable", into = "DirectoryTable")]
pub struct FleetDirectory {
    /// The store, with its own keys.
    pub backend: FleetBackend,
    /// The cluster that runs the store the deployment runs itself
    /// ([`FleetDirectory::runs_store`]), at an address every cluster
    /// reaches: one store for the fleet, since a store in each cluster
    /// would give each region a directory of its own. Required with such a
    /// store when the fleet names clusters, and refused without one.
    /// Compose, which runs on one host, does not read it.
    pub cluster: Option<String>,
}

/// The stores a fleet's `[directory]` can name, with their keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetBackend {
    /// Two DynamoDB tables ([`DirectoryBackend::DynamoDb`]):
    /// `backend = "dynamodb"`.
    Dynamodb {
        /// The records table's name.
        records_table: String,
        /// The lists table's name.
        lists_table: String,
        /// Whether the deployment runs one DynamoDB Local for the tables,
        /// which every hub uses with fixed test credentials, instead of
        /// AWS.
        local: bool,
    },
    /// An etcd that the deployment runs for the fleet
    /// ([`DirectoryBackend::Etcd`]), under the default prefix:
    /// `backend = "etcd"`. Each render gives the pools its address.
    Etcd,
}

impl FleetDirectory {
    /// Whether the deployment runs the directory's store itself: a
    /// DynamoDB Local with `local = true`, and etcd always.
    pub fn runs_store(&self) -> bool {
        match self.backend {
            FleetBackend::Dynamodb { local, .. } => local,
            FleetBackend::Etcd => true,
        }
    }
}

/// The `[directory]` table as it is written: every key of every backend,
/// which [`FleetDirectory`] is read from and written as, so that a key a
/// backend does not read is refused by name.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryTable {
    backend: BackendName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    records_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lists_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster: Option<String>,
}

/// The names `backend` takes.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BackendName {
    Dynamodb,
    Etcd,
}

impl TryFrom<DirectoryTable> for FleetDirectory {
    type Error = String;

    fn try_from(table: DirectoryTable) -> Result<Self, String> {
        let backend = match table.backend {
            BackendName::Dynamodb => FleetBackend::Dynamodb {
                records_table: table.records_table.ok_or("missing field `records_table`")?,
                lists_table: table.lists_table.ok_or("missing field `lists_table`")?,
                local: table.local.unwrap_or(false),
            },
            BackendName::Etcd => {
                // One spelling: the fleet always runs its etcd, so `local`
                // would say nothing, and `false` would say what no render
                // does.
                for (key, set) in [
                    ("records_table", table.records_table.is_some()),
                    ("lists_table", table.lists_table.is_some()),
                    ("local", table.local.is_some()),
                ] {
                    if set {
                        return Err(format!(
                            "`{key}` is read only by backend = \"dynamodb\"; under backend = \"etcd\" the deployment always runs the fleet's etcd, which needs no table"
                        ));
                    }
                }
                FleetBackend::Etcd
            }
        };
        Ok(Self {
            backend,
            cluster: table.cluster,
        })
    }
}

impl From<FleetDirectory> for DirectoryTable {
    fn from(directory: FleetDirectory) -> Self {
        let (backend, records_table, lists_table, local) = match directory.backend {
            FleetBackend::Dynamodb {
                records_table,
                lists_table,
                local,
            } => (
                BackendName::Dynamodb,
                Some(records_table),
                Some(lists_table),
                local.then_some(true),
            ),
            FleetBackend::Etcd => (BackendName::Etcd, None, None, None),
        };
        Self {
            backend,
            records_table,
            lists_table,
            local,
            cluster: directory.cluster,
        }
    }
}

/// What every pool of a class needs unless it says otherwise.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassDefaults {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    ready: BTreeMap<String, u32>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    requires: BTreeMap<String, u32>,
}

/// One pool, as the fleet file lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolSpec {
    /// The layout the pool runs: required when the spec has more than one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    layout: Option<String>,
    class: String,
    /// The cluster that runs the pool, whose region is the pool's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster: Option<String>,
    /// The pool's region: required without a cluster, and its cluster's
    /// when given with one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    /// The parent pool: the pool of a class that sends to this one that
    /// follows this pool and offers it work. This or `parents` is required
    /// for a pool of a class that another class sends to, and both are
    /// refused for one of a class that nothing sends to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    home: Option<String>,
    /// In place of `home`, the parent class: every pool of it in the
    /// pool's layout follows this pool and offers it work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parents: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    ready: BTreeMap<String, u32>,
    /// This pool's pods' requests and limits, over its cluster's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resources: Option<PoolResources>,
    /// The DynamoGraphDeployment the pool owns, which runs some of its
    /// segments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dgd: Option<PoolDgd>,
    /// How the pool's job runs, on a Slurm site.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    slurm: Option<PoolSlurm>,
}

/// A pool's `dgd` table: the DynamoGraphDeployment (DGD) it owns, which
/// dynamo's operator runs, and the segments that run in its components.
/// A segment it does not name stays one of gdl's own Deployments. Only
/// `cargo gdl k8s` and `deploy` read it, for a pool of a cluster the gdl
/// operator runs; the file itself is read and judged there, beside the
/// fleet file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolDgd {
    /// The DGD's file, YAML or JSON, relative to the fleet file.
    pub file: String,
    /// Each segment that runs in the DGD, and the component that runs it.
    pub components: BTreeMap<String, String>,
}

impl PoolSpec {
    /// Whether this pool can be `child`'s parent in their layout, whose
    /// classes send as `sends` says: its class sends to the child's. The
    /// layouts themselves are compared first ([`Fleet::home_problem`]):
    /// pools of different layouts never follow or offer to each other.
    fn sends_to(&self, child: &PoolSpec, sends: &Names<'_>) -> bool {
        sends
            .get(self.class.as_str())
            .is_some_and(|into| into.contains(child.class.as_str()))
    }
}

/// The classes of one layout, as a fleet is judged against them.
struct Classes<'a> {
    /// The layout's name, when the spec has several: a problem names it.
    named: Option<&'a str>,
    /// Each class's segments.
    segments: Names<'a>,
    /// The classes each class sends to.
    sends: Names<'a>,
}

impl<'a> Classes<'a> {
    fn of(layout: &'a Layout, named: Option<&'a str>) -> Self {
        let mut sends = Names::new();
        for boundary in layout.boundaries() {
            sends
                .entry(boundary.from.as_str())
                .or_default()
                .insert(boundary.into.as_str());
        }
        Self {
            named,
            segments: layout.class_segments(),
            sends,
        }
    }
}

/// Each layout of the spec by its name, with its classes when its
/// placement fits the graph.
type Laid<'a> = BTreeMap<&'a str, Option<Classes<'a>>>;

impl Fleet {
    /// Parse a fleet file. An unknown key anywhere is refused; what the
    /// values mean is checked against the spec's layouts by
    /// [`Fleet::check`].
    pub fn from_toml(text: &str) -> Result<Self, Problems> {
        read_toml(text)
    }

    /// Check the fleet against the layouts of the spec it deploys, each by
    /// its name with its [`Layout`] when its placement fits the graph,
    /// reporting every problem.
    ///
    /// Each pool runs the layout its `layout` names, which must be one of
    /// the spec's; a pool of a spec of one layout may leave it unsaid. Its
    /// class, its quorum, and its parent are judged against that layout: a
    /// home is a pool of the same layout, whose class sends to the pool's
    /// there, and `parents` is a class that sends to the pool's there; a
    /// pool names one of the two. A pool that names `parents` is refused
    /// while the fleet's pools publish through `relay`, where a pool reports
    /// its record to one parent's hub. Every pool whose class sends to
    /// another class is a parent, and has at least one child of each class
    /// its class sends to: a pool of that class and of its layout whose
    /// home it is, or whose parents are its class; each class it lacks is a
    /// problem at the parent's table. A class's defaults apply
    /// to its pools in every layout with the class, and are judged against
    /// each. A pool whose layout or class is unknown is reported once: its
    /// quorum, its home, and its children are not judged against them.
    /// A pool's cluster is one of the fleet's, and its region, when it
    /// gives one with a cluster, is the cluster's; a pool without a cluster
    /// gives its region. No two clusters share a kube context and a
    /// namespace. What needs a layout's classes is not judged for a layout
    /// given without its [`Layout`], and without any layouts (the spec did
    /// not read) only what needs none is: the fleet's name, its clusters,
    /// the pools' names, regions, clusters, and layout names, that each
    /// home is a pool of the fleet in the layout its pool names, and that
    /// no count is 0. [`Fleet::children_unchecked`] says
    /// when that left a pool's children unjudged.
    pub fn check(
        &self,
        layouts: Option<&BTreeMap<String, Option<Layout>>>,
    ) -> Result<(), FleetErrors> {
        let several = layouts.is_some_and(|layouts| layouts.len() > 1);
        let laid: Option<Laid<'_>> = layouts.map(|layouts| {
            layouts
                .iter()
                .map(|(name, layout)| {
                    let named = several.then_some(name.as_str());
                    (
                        name.as_str(),
                        layout.as_ref().map(|l| Classes::of(l, named)),
                    )
                })
                .collect()
        });
        let mut errors = Vec::new();
        if let Some(directory) = &self.directory {
            if let FleetBackend::Dynamodb {
                records_table,
                lists_table,
                ..
            } = &directory.backend
            {
                for (key, name) in [
                    ("records_table", records_table),
                    ("lists_table", lists_table),
                ] {
                    if let Err(reason) = check_table_name(name) {
                        errors.push(FleetError::InvalidDirectory {
                            key: key.into(),
                            reason,
                        });
                    }
                }
            }
            let store = match directory.backend {
                FleetBackend::Dynamodb { .. } => "a directory in DynamoDB Local",
                FleetBackend::Etcd => "an etcd directory",
            };
            let reason = match (&directory.cluster, directory.runs_store()) {
                (Some(cluster), false) => Some(format!(
                    "{cluster:?} names the cluster that runs DynamoDB Local, so it needs local = true"
                )),
                (Some(cluster), true) if !self.clusters.contains_key(cluster) => {
                    Some(format!("the fleet has no cluster `{cluster}`"))
                }
                (None, true) if !self.clusters.is_empty() => Some(format!(
                    "missing; the fleet names clusters, so {store} names the one cluster that runs it, for every cluster to reach"
                )),
                _ => None,
            };
            if let Some(reason) = reason {
                errors.push(FleetError::InvalidDirectory {
                    key: "cluster".into(),
                    reason,
                });
            }
        }
        for (class, defaults) in &self.classes {
            self.check_class(class, defaults, laid.as_ref(), &mut errors);
        }
        self.header.check(&mut errors);
        check_clusters(&self.clusters, &mut errors);
        // A cluster's segment keys name a segment of any layout: which of
        // its pools runs which is the pools' to say.
        if let Some(laid) = &laid {
            let members: BTreeSet<&str> = laid
                .values()
                .flatten()
                .flat_map(|classes| classes.segments.values().flatten().copied())
                .collect();
            for (name, cluster) in &self.clusters {
                if let Some(resources) = cluster.kubernetes().and_then(|c| c.resources.as_ref()) {
                    placement::check_segments(
                        FleetTable::Cluster(name.clone()),
                        resources.segment.keys(),
                        &members,
                        &mut errors,
                    );
                }
            }
        }
        // Each child by its home, its layout, and its class, and each
        // shared pool by its parents' class: a parent's children are the
        // pools that name it, or its class, in its own layout.
        let (mut homed, mut shared) = (BTreeSet::new(), BTreeSet::new());
        for (name, pool) in &self.pools {
            let Some(runs) = self.layout_of(name, pool, laid.as_ref()).ok().flatten() else {
                continue;
            };
            if let Some(home) = &pool.home {
                homed.insert((home.as_str(), runs, pool.class.as_str()));
            }
            if let Some(parents) = &pool.parents {
                shared.insert((parents.as_str(), runs, pool.class.as_str()));
            }
        }
        for (name, pool) in &self.pools {
            let table = || FleetTable::Pool(name.clone());
            if let Err(reason) = check_pool_name(name) {
                errors.push(FleetError::InvalidPoolName {
                    pool: name.clone(),
                    reason,
                });
            }
            self.region_problems(name, pool, &mut errors);
            let runs = match self.layout_of(name, pool, laid.as_ref()) {
                Ok(runs) => runs,
                Err(error) => {
                    errors.push(error);
                    None
                }
            };
            match runs.and_then(|runs| Some((runs, laid.as_ref()?.get(runs)?.as_ref()?))) {
                None => zero_ready(table, &pool.ready, &mut errors),
                Some((runs, classes)) => match classes.segments.get(pool.class.as_str()) {
                    Some(members) => {
                        check_ready(
                            table,
                            &pool.class,
                            members,
                            &pool.ready,
                            classes.named,
                            &mut errors,
                        );
                        for &into in classes.sends.get(pool.class.as_str()).into_iter().flatten() {
                            if !homed.contains(&(name.as_str(), runs, into))
                                && !shared.contains(&(pool.class.as_str(), runs, into))
                            {
                                errors.push(FleetError::MissingChild {
                                    pool: name.clone(),
                                    layout: runs.to_string(),
                                    class: into.to_string(),
                                });
                            }
                        }
                    }
                    None => errors.push(FleetError::UnknownClass {
                        table: table(),
                        class: pool.class.clone(),
                        layout: classes.named.map(str::to_string),
                    }),
                },
            }
            let site = pool
                .cluster
                .as_ref()
                .and_then(|cluster| self.clusters.get(cluster))
                .and_then(Cluster::slurm);
            self.slurm_problems(name, pool, site, &mut errors);
            if let Some(dgd) = &pool.dgd {
                self.dgd_problems(name, pool, dgd, runs, laid.as_ref(), &mut errors);
            }
            if let Some(resources) = pool.resources.as_ref().filter(|_| site.is_none()) {
                let cluster = pool
                    .cluster
                    .as_ref()
                    .and_then(|cluster| self.clusters.get(cluster))
                    .and_then(Cluster::kubernetes)
                    .and_then(|cluster| cluster.resources.as_ref());
                placement::check_pool(name, resources, cluster, &mut errors);
                if let Some(members) = runs
                    .and_then(|runs| laid.as_ref()?.get(runs)?.as_ref())
                    .and_then(|classes| classes.segments.get(pool.class.as_str()))
                {
                    placement::check_segments(
                        table(),
                        resources.segment.keys(),
                        members,
                        &mut errors,
                    );
                }
            }
            errors.extend(self.home_problem(name, pool, laid.as_ref()));
            // Under the fleet's DynamoDB directory every parent's hub reads
            // every record, so a shared pool is found by each.
            if let (Some(parents), None) = (&pool.parents, &self.directory) {
                errors.push(FleetError::SharedUnderRelay {
                    pool: name.clone(),
                    parents: parents.clone(),
                });
            }
        }
        for (name, site) in &self.clusters {
            if let Cluster::Slurm(site) = site {
                errors.extend(self.lead_problem(name, site));
            }
        }
        // A class's table is judged in each layout with the class, and
        // what is wrong in all of them alike is one problem.
        let mut unique: Vec<FleetError> = Vec::with_capacity(errors.len());
        for error in errors {
            if !unique.contains(&error) {
                unique.push(error);
            }
        }
        if unique.is_empty() {
            Ok(())
        } else {
            Err(FleetErrors(unique))
        }
    }

    /// Slurm site `name`'s `signal_lead_s`, when it is shorter than the
    /// longest stop wait of a pool on the site plus
    /// [`SlurmSite::SIGNAL_MARGIN_S`]: Slurm signals each job that long
    /// before its time limit, and the pool's hub, which drains the pool on
    /// that signal, waits up to its stop wait before it stops. A pool's
    /// stop wait is its settings' `stop_wait_ms`, else
    /// [`PoolSettings::DEFAULT_STOP_WAIT`].
    fn lead_problem(&self, name: &str, site: &SlurmSite) -> Option<FleetError> {
        let (wait, pool) = self
            .pools
            .iter()
            .filter(|(_, pool)| pool.cluster.as_deref() == Some(name))
            .filter_map(|(pool, _)| {
                let settings = self.pool(pool)?;
                let wait = settings
                    .drain_on_stop
                    .unwrap_or(PoolSettings::DEFAULT_STOP_WAIT);
                Some((wait, pool))
            })
            // On a tie, the first pool in name order.
            .max_by_key(|(wait, pool)| (*wait, std::cmp::Reverse(*pool)))?;
        // Whole seconds, rounded up: a wait of 660.5 s needs a lead of 781.
        let wait = u64::try_from(wait.as_millis().div_ceil(1000)).unwrap_or(u64::MAX);
        let floor = wait.saturating_add(SlurmSite::SIGNAL_MARGIN_S);
        (site.signal_lead_s < floor).then(|| FleetError::Slurm {
            table: FleetTable::Cluster(name.to_string()),
            key: Some("signal_lead_s".into()),
            reason: format!(
                "is {} s, below pool `{pool}`'s stop wait of {wait} s plus {} s, {floor} s: Slurm signals a job that long before its time limit, and the pool's hub drains the pool and waits up to its stop wait before it stops. Raise it to at least {floor}",
                site.signal_lead_s,
                SlurmSite::SIGNAL_MARGIN_S,
            ),
        })
    }

    /// What pool `name` asks of Slurm that it cannot have: a `slurm` table
    /// on a pool of no Slurm site, which nothing would read, or one whose
    /// partition or time limit Slurm would not take; and, on Slurm site
    /// `site`, a `dgd` table, which only dynamo's operator on Kubernetes
    /// runs, or Kubernetes requests and limits, which nothing would apply.
    fn slurm_problems(
        &self,
        name: &str,
        pool: &PoolSpec,
        site: Option<&SlurmSite>,
        errors: &mut Vec<FleetError>,
    ) {
        let mut refuse = |key: &str, reason: String| {
            errors.push(FleetError::Slurm {
                table: FleetTable::Pool(name.to_string()),
                key: Some(key.to_string()),
                reason,
            });
        };
        if let Some(slurm) = &pool.slurm {
            match (site, &pool.cluster) {
                (None, None) => refuse(
                    "slurm",
                    "the pool names no cluster, so no Slurm site reads it: name the Slurm site that runs the pool, or leave the table out".into(),
                ),
                (None, Some(cluster)) => {
                    if let Some(placed) = self.clusters.get(cluster) {
                        refuse(
                            "slurm",
                            format!(
                                "cluster `{cluster}` is a {} cluster, which does not read it: leave the table out, or run the pool on a Slurm site",
                                placed.kind()
                            ),
                        );
                    }
                }
                (Some(site), _) => {
                    if let Some(partition) = &slurm.partition
                        && (partition.is_empty() || partition.contains(char::is_whitespace))
                    {
                        refuse(
                            "slurm.partition",
                            format!("`{partition}` is not a partition's name"),
                        );
                    }
                    if let Some(limit) = &slurm.time_limit {
                        check_time_limit(limit, site.signal_lead_s, &mut |key, reason| {
                            refuse(&format!("slurm.{key}"), reason)
                        });
                    }
                }
            }
        }
        let (Some(_), Some(cluster)) = (site, &pool.cluster) else {
            return;
        };
        if pool.dgd.is_some() {
            refuse(
                "dgd",
                format!(
                    "cluster `{cluster}` is a Slurm site: a DynamoGraphDeployment runs only under dynamo's operator on Kubernetes. Run the pool on a Kubernetes cluster, or leave the table out"
                ),
            );
        }
        if pool.resources.is_some() {
            refuse(
                "resources",
                format!(
                    "cluster `{cluster}` is a Slurm site, where each segment instance takes its nodes whole, and no pod's requests and limits apply: leave the table out"
                ),
            );
        }
    }

    /// What is wrong with `pool`'s `dgd` table, which it runs in layout
    /// `runs` when that is known: the gdl operator runs its cluster, since
    /// it owns the DGD, its stop and its scaling, and `deploy` alone could
    /// neither prune nor delete it; and each key of `components` is a
    /// segment of the pool's class. What the file says is judged where it
    /// is read.
    fn dgd_problems(
        &self,
        name: &str,
        pool: &PoolSpec,
        dgd: &PoolDgd,
        runs: Option<&str>,
        laid: Option<&Laid<'_>>,
        errors: &mut Vec<FleetError>,
    ) {
        let placed = pool
            .cluster
            .as_ref()
            .and_then(|cluster| self.clusters.get(cluster));
        let operated = placed
            .and_then(Cluster::kubernetes)
            .is_some_and(|cluster| cluster.operator);
        // A DGD on a Slurm site is that pool's Slurm problem.
        if !operated && placed.and_then(Cluster::slurm).is_none() {
            errors.push(FleetError::DgdWithoutOperator {
                pool: name.to_string(),
                cluster: pool.cluster.clone(),
            });
        }
        let Some(classes) = runs.and_then(|runs| laid?.get(runs)?.as_ref()) else {
            return;
        };
        let Some(members) = classes.segments.get(pool.class.as_str()) else {
            return;
        };
        for segment in dgd.components.keys() {
            if !members.contains(segment.as_str()) {
                errors.push(FleetError::DgdNotInClass {
                    pool: name.to_string(),
                    segment: segment.clone(),
                    class: pool.class.clone(),
                    layout: classes.named.map(str::to_string),
                });
            }
        }
    }

    /// What is wrong with `pool`'s region and cluster: its region is a DNS
    /// label, its cluster one of the fleet's, and it has a region, its own
    /// or its cluster's, the same when it has both. A cluster's own region
    /// is judged at the cluster.
    fn region_problems(&self, name: &str, pool: &PoolSpec, errors: &mut Vec<FleetError>) {
        let mut shaped = true;
        if let Some(region) = &pool.region
            && let Err(reason) = check_region_name(region)
        {
            shaped = false;
            errors.push(FleetError::InvalidRegion {
                table: FleetTable::Pool(name.into()),
                reason,
            });
        }
        let Some(cluster) = &pool.cluster else {
            if pool.region.is_none() {
                errors.push(FleetError::MissingRegion { pool: name.into() });
            }
            return;
        };
        let Some(placed) = self.clusters.get(cluster) else {
            errors.push(FleetError::UnknownCluster {
                pool: name.into(),
                cluster: cluster.clone(),
            });
            return;
        };
        // A region that is no region's name is already its problem.
        if let Some(region) = pool.region.as_ref().filter(|_| shaped)
            && region != placed.region()
        {
            errors.push(FleetError::RegionNotCluster {
                pool: name.into(),
                region: region.clone(),
                cluster: cluster.clone(),
                cluster_region: placed.region().to_string(),
            });
        }
    }

    /// What is wrong with the defaults of `class`: judged against each
    /// layout with the class, and unknown when no layout has it.
    fn check_class(
        &self,
        class: &str,
        defaults: &ClassDefaults,
        laid: Option<&Laid<'_>>,
        errors: &mut Vec<FleetError>,
    ) {
        let table = || FleetTable::Class(class.to_string());
        let zero = |errors: &mut Vec<FleetError>| {
            zero_ready(table, &defaults.ready, errors);
            for (required, _) in defaults.requires.iter().filter(|(_, n)| **n == 0) {
                errors.push(FleetError::ZeroRequires {
                    class: class.to_string(),
                    required: required.clone(),
                });
            }
        };
        let Some(laid) = laid else {
            // A count of 0 is wrong whatever the classes are.
            return zero(errors);
        };
        let with: Vec<&Classes<'_>> = laid
            .values()
            .flatten()
            .filter(|classes| classes.segments.contains_key(class))
            .collect();
        if with.is_empty() {
            // A layout whose classes are not known may have it.
            if laid.values().any(Option::is_none) {
                return zero(errors);
            }
            errors.push(match laid.len() > 1 {
                true => FleetError::ClassInNoLayout {
                    class: class.to_string(),
                },
                false => FleetError::UnknownClass {
                    table: table(),
                    class: class.to_string(),
                    layout: None,
                },
            });
            return;
        }
        for classes in with {
            let members = &classes.segments[class];
            check_ready(
                table,
                class,
                members,
                &defaults.ready,
                classes.named,
                errors,
            );
            let into = classes.sends.get(class);
            for (required, &count) in &defaults.requires {
                if !into.is_some_and(|into| into.contains(required.as_str())) {
                    errors.push(FleetError::RequiresNotSent {
                        class: class.to_string(),
                        required: required.clone(),
                        layout: classes.named.map(str::to_string),
                    });
                } else if count == 0 {
                    errors.push(FleetError::ZeroRequires {
                        class: class.to_string(),
                        required: required.clone(),
                    });
                }
            }
        }
    }

    /// The layout `pool` runs: the one its `layout` names, or, when it
    /// names none, the spec's only one. `None` when it cannot be told:
    /// without the spec's layouts, a pool that names none runs whichever
    /// the spec has. A name that is not a layout's is the pool's problem.
    fn layout_of<'a>(
        &'a self,
        name: &str,
        pool: &'a PoolSpec,
        laid: Option<&Laid<'a>>,
    ) -> Result<Option<&'a str>, FleetError> {
        if let Some(layout) = &pool.layout
            && let Err(reason) = check_layout_name(layout)
        {
            return Err(FleetError::InvalidLayout {
                pool: name.into(),
                reason,
            });
        }
        let Some(laid) = laid else {
            return Ok(pool.layout.as_deref());
        };
        let names = || laid.keys().map(|name| name.to_string()).collect();
        match &pool.layout {
            Some(layout) if laid.contains_key(layout.as_str()) => Ok(Some(layout)),
            Some(layout) => Err(FleetError::UnknownLayout {
                pool: name.into(),
                layout: layout.clone(),
                layouts: names(),
            }),
            None if laid.len() == 1 => Ok(laid.keys().next().copied()),
            None => Err(FleetError::MissingLayout {
                pool: name.into(),
                layouts: names(),
            }),
        }
    }

    /// What is wrong with `pool`'s parent, its `home` or its `parents`, if
    /// anything. That it names at most one needs nothing; whether a home
    /// names a pool of the fleet needs no layout, and whether that pool runs
    /// the same layout needs only the layouts' names; whether a pool needs a
    /// parent, and whether it names one, need the classes, and a pool of a
    /// layout or a class the spec does not have is reported as that pool's
    /// problem.
    fn home_problem(
        &self,
        name: &str,
        pool: &PoolSpec,
        laid: Option<&Laid<'_>>,
    ) -> Option<FleetError> {
        if let (Some(home), Some(parents)) = (&pool.home, &pool.parents) {
            return Some(FleetError::HomeAndParents {
                pool: name.into(),
                home: home.clone(),
                parents: parents.clone(),
            });
        }
        if let Some(home) = &pool.home
            && !self.pools.contains_key(home)
        {
            return Some(FleetError::UnknownHome {
                pool: name.into(),
                home: home.clone(),
            });
        }
        let runs = self.layout_of(name, pool, laid).ok().flatten();
        if let Some(home) = &pool.home {
            let parent = &self.pools[home];
            let parent_runs = self.layout_of(home, parent, laid).ok().flatten();
            if let (Some(runs), Some(parent_runs)) = (runs, parent_runs)
                && runs != parent_runs
            {
                return Some(FleetError::HomeInOtherLayout {
                    pool: name.into(),
                    home: home.clone(),
                    home_layout: parent_runs.to_string(),
                    layout: runs.to_string(),
                });
            }
        }
        let Classes {
            named,
            segments,
            sends,
        } = laid?.get(runs?)?.as_ref()?;
        if !segments.contains_key(pool.class.as_str()) {
            return None;
        }
        let parents: Vec<String> = sends
            .iter()
            .filter(|(_, into)| into.contains(pool.class.as_str()))
            .map(|(from, _)| from.to_string())
            .collect();
        if let Some(shared) = &pool.parents {
            return match parents.is_empty() {
                true => Some(FleetError::ParentOfTopClass {
                    pool: name.into(),
                    class: pool.class.clone(),
                    key: "parents",
                }),
                false => (!parents.contains(shared)).then(|| FleetError::ParentsNotSender {
                    pool: name.into(),
                    parents: shared.clone(),
                    class: pool.class.clone(),
                    layout: named.map(str::to_string),
                }),
            };
        }
        match &pool.home {
            None if parents.is_empty() => None,
            None => Some(FleetError::MissingParent {
                pool: name.into(),
                class: pool.class.clone(),
                parents,
            }),
            Some(_) if parents.is_empty() => Some(FleetError::ParentOfTopClass {
                pool: name.into(),
                class: pool.class.clone(),
                key: "home",
            }),
            Some(home) => {
                let parent = &self.pools[home];
                (segments.contains_key(parent.class.as_str()) && !parent.sends_to(pool, sends))
                    .then(|| FleetError::HomeNotParent {
                        pool: name.into(),
                        home: home.clone(),
                        home_class: parent.class.clone(),
                        class: pool.class.clone(),
                    })
            }
        }
    }

    /// A note that the rule [`Fleet::check`] judges with a layout's
    /// classes, that every parent pool has a child of each class its class
    /// sends to, was not judged for some pool: the layouts are not known,
    /// or a layout a pool runs has problems that leave its classes unknown. `None` when every
    /// pool's layout is known. It is in no table: one note for the file.
    pub fn children_unchecked(
        &self,
        layouts: Option<&BTreeMap<String, Option<Layout>>>,
    ) -> Option<Problem> {
        let because = "which class sends to which is a layout's";
        let Some(layouts) = layouts else {
            return (!self.pools.is_empty()).then(|| {
                Problem::at(
                    None,
                    None,
                    format!(
                        "the pools' children are not checked: {because}, and the spec's layouts did not read"
                    ),
                )
            });
        };
        let laid: Laid<'_> = layouts
            .iter()
            .map(|(name, layout)| (name.as_str(), layout.as_ref().map(|l| Classes::of(l, None))))
            .collect();
        let unfit: BTreeSet<&str> = self
            .pools
            .iter()
            .filter_map(|(name, pool)| self.layout_of(name, pool, Some(&laid)).ok()?)
            .filter(|runs| laid.get(runs).is_some_and(Option::is_none))
            .collect();
        let quoted: Vec<String> = unfit.iter().map(|name| format!("`{name}`")).collect();
        let message = match quoted.as_slice() {
            [] => return None,
            [one] => format!(
                "the pools' children are not checked in layout {one}: {because}, and the layout has problems"
            ),
            many => format!(
                "the pools' children are not checked in layouts {}: {because}, and the layouts have problems",
                many.join(", ")
            ),
        };
        Some(Problem::at(None, None, message))
    }

    /// The settings of the pool `name`, with its quorum resolved: for each
    /// segment, the pool's own `ready` if it sets that segment, else its
    /// class's default; a segment with neither is left to
    /// [`PoolSettings::quorum`]'s fallback. Its region is its cluster's,
    /// when it names one, else its own. Its `requires` is its class's, and
    /// unsaid when that is, which requires one ready child of each class its
    /// class sends to ([`PoolSettings::requirements`]). Its directory is
    /// the `[directory]` table's: DynamoDB in the pool's region with no
    /// endpoint, or etcd at no endpoints and the default prefix, either of
    /// which a render gives the address of the store the deployment runs;
    /// or the default, `relay`, without the table. The hub address, summary
    /// period, and hub timing are not in the fleet file and take their
    /// defaults. Meant for a fleet that
    /// [`Fleet::check`] accepted, whose `ready` keys are all segments of
    /// the pool's class.
    pub fn pool(&self, name: &str) -> Option<PoolSettings> {
        let pool = self.pools.get(name)?;
        let defaults = self.classes.get(&pool.class);
        let mut ready = defaults
            .map(|class| class.ready.clone())
            .unwrap_or_default();
        ready.extend(pool.ready.iter().map(|(s, n)| (s.clone(), *n)));
        let region = self
            .cluster_of(name)
            .map(|(_, cluster)| cluster.region().to_string())
            .or_else(|| pool.region.clone())
            .unwrap_or_default();
        Some(PoolSettings {
            properties: Vec::new(),
            name: name.to_string(),
            layout: pool.layout.clone(),
            class: pool.class.clone(),
            region: region.clone(),
            // A fleet that `check` accepted names at most one.
            parent: Parent::from_keys(pool.home.clone(), pool.parents.clone())
                .ok()
                .flatten(),
            directory: match self.directory.as_ref().map(|d| &d.backend) {
                None => DirectoryBackend::default(),
                Some(FleetBackend::Dynamodb {
                    records_table,
                    lists_table,
                    ..
                }) => DirectoryBackend::DynamoDb {
                    records_table: records_table.clone(),
                    lists_table: lists_table.clone(),
                    aws_region: region,
                    endpoint: None,
                },
                // Its address is the network's: each render gives it.
                Some(FleetBackend::Etcd) => DirectoryBackend::Etcd {
                    endpoints: Vec::new(),
                    prefix: DirectoryBackend::ETCD_PREFIX.into(),
                },
            },
            ready,
            requires: defaults
                .map(|class| class.requires.clone())
                .unwrap_or_default(),
            hub: None,
            summary: PoolSettings::DEFAULT_SUMMARY,
            timing: Default::default(),
            // Compose and Kubernetes stop a hub at once; a site that drains
            // pools on stop sets it in its own render.
            drain_on_stop: None,
            leave_by: None,
        })
    }

    /// The fleet's `[directory]` table, if it has one.
    pub fn directory(&self) -> Option<&FleetDirectory> {
        self.directory.as_ref()
    }

    /// The requests and limits of a pod of cluster `cluster`: of `pod`, a
    /// segment or [`HUB`], of `pool`, merged field by field over the
    /// cluster's for the segment and its default; a pod of no pool or
    /// segment, as the DynamoDB directory's, takes the default. `None` when
    /// the cluster gives no resources.
    pub fn pod_resources(
        &self,
        cluster: &str,
        pool: Option<&str>,
        pod: Option<&str>,
    ) -> Option<Resources> {
        let resources = self
            .clusters
            .get(cluster)?
            .kubernetes()?
            .resources
            .as_ref()?;
        let mut merged = resources.default.clone()?;
        if let Some(pod) = pod {
            if let Some(own) = resources.segment.get(pod) {
                merged = own.over(&merged);
            }
            if let Some(own) = pool
                .and_then(|pool| self.pools.get(pool))
                .and_then(|pool| pool.resources.as_ref())
                .and_then(|resources| resources.segment.get(pod))
            {
                merged = own.over(&merged);
            }
        }
        Some(merged)
    }

    /// The `slurm` table of pool `pool`, if the fleet has the pool and it
    /// has one.
    pub fn slurm(&self, pool: &str) -> Option<&PoolSlurm> {
        self.pools.get(pool)?.slurm.as_ref()
    }

    /// The `dgd` table of pool `pool`, if the fleet has the pool and it
    /// has one.
    pub fn dgd(&self, pool: &str) -> Option<&PoolDgd> {
        self.pools.get(pool)?.dgd.as_ref()
    }

    /// Every pool with a `dgd` table, by its name, in name order.
    pub fn dgds(&self) -> impl Iterator<Item = (&str, &PoolDgd)> + '_ {
        self.pools
            .iter()
            .filter_map(|(name, pool)| Some((name.as_str(), pool.dgd.as_ref()?)))
    }

    /// The fleet's `[metrics]` table.
    pub fn metrics(&self) -> &FleetMetrics {
        &self.metrics
    }

    /// Every pool's settings, as [`Fleet::pool`] gives them, in name order:
    /// what a bundle generator deploys.
    pub fn pools(&self) -> impl Iterator<Item = PoolSettings> + '_ {
        self.pools.keys().filter_map(|name| self.pool(name))
    }

    /// The fleet's name, from `[fleet] name`, if it has one.
    pub fn name(&self) -> Option<&str> {
        self.header.name.as_deref()
    }

    /// The cluster `name`, if the fleet has it.
    pub fn cluster(&self, name: &str) -> Option<&Cluster> {
        self.clusters.get(name)
    }

    /// Every cluster, by its name, in name order.
    pub fn clusters(&self) -> impl Iterator<Item = (&str, &Cluster)> + '_ {
        self.clusters
            .iter()
            .map(|(name, cluster)| (name.as_str(), cluster))
    }

    /// The cluster that runs the pool `pool`, by its name: `None` when the
    /// fleet has no such pool, the pool names no cluster, or the fleet has
    /// no cluster of that name.
    pub fn cluster_of(&self, pool: &str) -> Option<(&str, &Cluster)> {
        let name = self.pools.get(pool)?.cluster.as_deref()?;
        Some((name, self.clusters.get(name)?))
    }
}

/// Names by a class: its segments, or the classes it sends to.
type Names<'a> = BTreeMap<&'a str, BTreeSet<&'a str>>;

/// Every `ready` count is at least 1: what can be judged of `ready`
/// without the classes.
fn zero_ready(
    table: impl Fn() -> FleetTable,
    ready: &BTreeMap<String, u32>,
    errors: &mut Vec<FleetError>,
) {
    for (segment, _) in ready.iter().filter(|(_, count)| **count == 0) {
        errors.push(FleetError::ZeroReady {
            table: table(),
            segment: segment.clone(),
        });
    }
}

/// Every `ready` key is a segment of `class`, with a count of at least 1.
/// `layout` names the class's layout, when the spec has several.
fn check_ready(
    table: impl Fn() -> FleetTable,
    class: &str,
    members: &BTreeSet<&str>,
    ready: &BTreeMap<String, u32>,
    layout: Option<&str>,
    errors: &mut Vec<FleetError>,
) {
    for (segment, &count) in ready {
        if !members.contains(segment.as_str()) {
            errors.push(FleetError::ReadyNotInClass {
                table: table(),
                segment: segment.clone(),
                class: class.to_string(),
                layout: layout.map(str::to_string),
            });
        } else if count == 0 {
            errors.push(FleetError::ZeroReady {
                table: table(),
                segment: segment.clone(),
            });
        }
    }
}

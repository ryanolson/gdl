use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::pool::{check_pool_name, check_region_name, check_table_name};
use crate::{
    DirectoryBackend, Layout, Parent, PoolSettings, Problem, Problems, check_layout_name, read_toml,
};

mod cluster;
mod errors;

pub use cluster::Cluster;
pub use errors::{FleetError, FleetErrors, FleetTable};

use cluster::{Header, check_clusters};

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
/// [cluster.use1]                     # optional: a Kubernetes cluster ([`Cluster`])
/// context = "kind-use1"
/// namespace = "gdl-llm"
/// region = "us-east-1"
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
///
/// [directory]                        # optional: without it, each child's hub reports to its parent's (`relay`)
/// backend = "dynamodb"               # every pool's hub publishes to two DynamoDB tables
/// records_table = "gdl-pools"
/// lists_table = "gdl-pool-lists"
/// local = true                       # optional: the bundle runs one DynamoDB Local for them
/// cluster = "use1"                   # with local, when the fleet names clusters: the one that runs it
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
}

/// The fleet's `[directory]` table: where every pool's hub publishes its
/// record. Without it, each child pool's hub reports to its parent's
/// (`relay`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetDirectory {
    /// The backend: `dynamodb`, the one a fleet can name.
    pub backend: FleetBackend,
    /// The records table's name.
    pub records_table: String,
    /// The lists table's name.
    pub lists_table: String,
    /// Whether the deployment runs one DynamoDB Local for the tables, which
    /// every hub uses with fixed test credentials, instead of AWS.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub local: bool,
    /// The cluster that runs the DynamoDB Local, at an address every
    /// cluster reaches: one Local for the fleet, since a Local in each
    /// cluster would give each region a directory of its own. Required
    /// with `local` when the fleet names clusters, and refused without
    /// `local`. Compose, which runs on one host, does not read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
}

/// The backends a fleet's `[directory]` can name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetBackend {
    /// Two DynamoDB tables ([`DirectoryBackend::DynamoDb`]).
    Dynamodb,
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
            for (key, name) in [
                ("records_table", &directory.records_table),
                ("lists_table", &directory.lists_table),
            ] {
                if let Err(reason) = check_table_name(name) {
                    errors.push(FleetError::InvalidDirectory {
                        key: key.into(),
                        reason,
                    });
                }
            }
            let reason = match (&directory.cluster, directory.local) {
                (Some(cluster), false) => Some(format!(
                    "{cluster:?} names the cluster that runs DynamoDB Local, so it needs local = true"
                )),
                (Some(cluster), true) if !self.clusters.contains_key(cluster) => {
                    Some(format!("the fleet has no cluster `{cluster}`"))
                }
                (None, true) if !self.clusters.is_empty() => Some(
                    "missing; the fleet names clusters, so a directory in DynamoDB Local names the one cluster that runs it, for every cluster to reach".into(),
                ),
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
            && *region != placed.region
        {
            errors.push(FleetError::RegionNotCluster {
                pool: name.into(),
                region: region.clone(),
                cluster: cluster.clone(),
                cluster_region: placed.region.clone(),
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
    /// the `[directory]` table's, in the pool's region, with no endpoint,
    /// or the default, `relay`, without one. The hub address, summary
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
            .map(|(_, cluster)| cluster.region.clone())
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
            directory: match &self.directory {
                None => DirectoryBackend::default(),
                Some(directory) => DirectoryBackend::DynamoDb {
                    records_table: directory.records_table.clone(),
                    lists_table: directory.lists_table.clone(),
                    aws_region: region,
                    endpoint: None,
                },
            },
            ready,
            requires: defaults
                .map(|class| class.requires.clone())
                .unwrap_or_default(),
            hub: None,
            summary: PoolSettings::DEFAULT_SUMMARY,
            timing: Default::default(),
        })
    }

    /// The fleet's `[directory]` table, if it has one.
    pub fn directory(&self) -> Option<&FleetDirectory> {
        self.directory.as_ref()
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

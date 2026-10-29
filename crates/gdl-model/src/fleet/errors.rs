use std::fmt;

use crate::{Error, Problem, Problems};

/// Why a fleet file does not fit the placement it deploys.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FleetError {
    /// A class table, or a pool's `class`, names a class the placement
    /// does not have: the pool's layout's, for a pool of a spec of several
    /// layouts.
    UnknownClass {
        /// The table it is in.
        table: FleetTable,
        /// The unknown name.
        class: String,
        /// The layout without it, when the spec has several.
        layout: Option<String>,
    },
    /// A class table names a class that no layout of the spec has.
    ClassInNoLayout {
        /// The unknown name.
        class: String,
    },
    /// A `ready` key is not a segment of the class.
    ReadyNotInClass {
        /// The table it is in.
        table: FleetTable,
        /// The key.
        segment: String,
        /// The class.
        class: String,
        /// The class's layout, when the spec has several.
        layout: Option<String>,
    },
    /// A `ready` count is 0.
    ZeroReady {
        /// The table it is in.
        table: FleetTable,
        /// The segment.
        segment: String,
    },
    /// A `requires` key is not a class that the class sends to.
    RequiresNotSent {
        /// The class whose table it is in.
        class: String,
        /// The key.
        required: String,
        /// The class's layout, when the spec has several.
        layout: Option<String>,
    },
    /// A `requires` count is 0.
    ZeroRequires {
        /// The class whose table it is in.
        class: String,
        /// The required class.
        required: String,
    },
    /// A pool's name is not a DNS label.
    InvalidPoolName {
        /// The pool.
        pool: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A key of the `[directory]` table does not fit.
    InvalidDirectory {
        /// The key.
        key: String,
        /// Why its value is refused.
        reason: String,
    },
    /// A pool's or a cluster's region is not a DNS label.
    InvalidRegion {
        /// The pool's or the cluster's table.
        table: FleetTable,
        /// Why the region is refused.
        reason: String,
    },
    /// A pool names neither a region nor a cluster.
    MissingRegion {
        /// The pool.
        pool: String,
    },
    /// A pool names a cluster the fleet does not have.
    UnknownCluster {
        /// The pool.
        pool: String,
        /// The unknown name.
        cluster: String,
    },
    /// A pool's region is not its cluster's.
    RegionNotCluster {
        /// The pool.
        pool: String,
        /// The pool's region.
        region: String,
        /// Its cluster.
        cluster: String,
        /// The cluster's region.
        cluster_region: String,
    },
    /// The fleet's name is not a DNS label.
    InvalidFleetName {
        /// Why the name is refused.
        reason: String,
    },
    /// A cluster's name is not a DNS label.
    InvalidClusterName {
        /// The cluster.
        cluster: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A cluster's context is empty.
    EmptyContext {
        /// The cluster.
        cluster: String,
    },
    /// A cluster's namespace is not a Kubernetes namespace's name.
    InvalidNamespace {
        /// The cluster.
        cluster: String,
        /// Why the namespace is refused.
        reason: String,
    },
    /// A cluster has the kube context and the namespace of a cluster
    /// before it in name order.
    SharedNamespace {
        /// The later cluster.
        cluster: String,
        /// The first cluster with them.
        first: String,
        /// The context they share.
        context: String,
        /// The namespace they share.
        namespace: String,
    },
    /// A cluster's `secrets` names a Kubernetes Secret by a name no Secret
    /// can have.
    InvalidSecretName {
        /// The cluster.
        cluster: String,
        /// The spec's secret.
        secret: String,
        /// The name given for its Kubernetes Secret.
        name: String,
    },
    /// A cluster's `service_account_annotations` key is not an annotation
    /// key.
    InvalidAnnotation {
        /// The cluster.
        cluster: String,
        /// The key.
        key: String,
    },
    /// A cluster's `env` key is not a variable's name.
    InvalidVariable {
        /// The cluster.
        cluster: String,
        /// The key.
        variable: String,
    },
    /// A pool's layout is not a layout's name.
    InvalidLayout {
        /// The pool.
        pool: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A pool names no layout, and the spec has several.
    MissingLayout {
        /// The pool.
        pool: String,
        /// The spec's layouts.
        layouts: Vec<String>,
    },
    /// A pool names a layout the spec does not have.
    UnknownLayout {
        /// The pool.
        pool: String,
        /// The unknown name.
        layout: String,
        /// The spec's layouts.
        layouts: Vec<String>,
    },
    /// A pool's home runs another layout than the pool.
    HomeInOtherLayout {
        /// The pool.
        pool: String,
        /// Its home.
        home: String,
        /// The home's layout.
        home_layout: String,
        /// The pool's layout.
        layout: String,
    },
    /// A pool's home names no pool of the fleet.
    UnknownHome {
        /// The pool.
        pool: String,
        /// Its home.
        home: String,
    },
    /// A pool of a class that another class sends to names no parent:
    /// neither `home` nor `parents`.
    MissingParent {
        /// The pool.
        pool: String,
        /// Its class.
        class: String,
        /// The classes that send to it.
        parents: Vec<String>,
    },
    /// A pool of a class that nothing sends to names a parent.
    ParentOfTopClass {
        /// The pool.
        pool: String,
        /// Its class.
        class: String,
        /// The key that names it: `home` or `parents`.
        key: &'static str,
    },
    /// A pool names both a `home` and `parents`.
    HomeAndParents {
        /// The pool.
        pool: String,
        /// Its home.
        home: String,
        /// Its parents' class.
        parents: String,
    },
    /// A pool's `parents` is not a class that sends to the pool's class.
    ParentsNotSender {
        /// The pool.
        pool: String,
        /// Its parents' class.
        parents: String,
        /// The pool's class.
        class: String,
        /// The pool's layout, when the spec has several.
        layout: Option<String>,
    },
    /// A pool names `parents`, which the fleet's directory cannot serve:
    /// under `relay` a pool reports its record to one parent's hub.
    SharedUnderRelay {
        /// The pool.
        pool: String,
        /// Its parents' class.
        parents: String,
    },
    /// A pool's home is of a class that does not send to the pool's class.
    HomeNotParent {
        /// The pool.
        pool: String,
        /// Its home.
        home: String,
        /// The home's class.
        home_class: String,
        /// The pool's class.
        class: String,
    },
    /// A parent pool has no child, a pool whose home it is or whose
    /// parents are its class, of a class its class sends to.
    MissingChild {
        /// The parent pool.
        pool: String,
        /// The layout it runs, which its child runs too.
        layout: String,
        /// The class it sends to and has no child of.
        class: String,
    },
}

/// A table of the fleet file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetTable {
    /// `[class.<name>]`.
    Class(String),
    /// `[pool.<name>]`.
    Pool(String),
    /// `[cluster.<name>]`.
    Cluster(String),
    /// `[fleet]`.
    Fleet,
}

impl FleetTable {
    /// The table's path, without its brackets.
    fn path(&self) -> String {
        match self {
            Self::Class(name) => format!("class.{name}"),
            Self::Pool(name) => format!("pool.{name}"),
            Self::Cluster(name) => format!("cluster.{name}"),
            Self::Fleet => "fleet".into(),
        }
    }
}

impl fmt::Display for FleetTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.path())
    }
}

impl FleetError {
    /// The problem, with the table and the key it is in; its reader adds
    /// the file.
    pub fn problem(&self) -> Problem {
        let class = |name: &str| Some(format!("class.{name}"));
        let pool = |name: &str| Some(format!("pool.{name}"));
        let cluster = |name: &str| Some(format!("cluster.{name}"));
        let key = |key: &str| Some(key.to_string());
        let quoted = |names: &[String]| {
            let names: Vec<String> = names.iter().map(|n| format!("`{n}`")).collect();
            names.join(", ")
        };
        let within = |layout: &Option<String>| match layout {
            Some(layout) => format!(" in layout `{layout}`"),
            None => String::new(),
        };
        let (table, key, message) = match self {
            Self::UnknownClass {
                table,
                class,
                layout,
            } => {
                // A class table's own name is the class; a pool names it
                // with its `class` key.
                let key = match table {
                    FleetTable::Pool(_) => key("class"),
                    _ => None,
                };
                let message = match layout {
                    Some(layout) => format!("layout `{layout}` has no class `{class}`"),
                    None => format!("the placement has no class `{class}`"),
                };
                (Some(table.path()), key, message)
            }
            Self::ClassInNoLayout { class: name } => (
                class(name),
                None,
                format!("no layout of the spec has a class `{name}`"),
            ),
            Self::ReadyNotInClass {
                table,
                segment,
                class,
                layout,
            } => (
                Some(table.path()),
                Some(format!("ready.{segment}")),
                format!("not a segment of class `{class}`{}", within(layout)),
            ),
            Self::ZeroReady { table, segment } => (
                Some(table.path()),
                Some(format!("ready.{segment}")),
                "is 0; a quorum is at least 1".into(),
            ),
            Self::RequiresNotSent {
                class: name,
                required,
                layout,
            } => (
                class(name),
                Some(format!("requires.{required}")),
                format!("not a class that `{name}` sends to{}", within(layout)),
            ),
            Self::ZeroRequires {
                class: name,
                required,
            } => (
                class(name),
                Some(format!("requires.{required}")),
                "is 0; a requirement is at least 1".into(),
            ),
            Self::InvalidPoolName { pool: name, reason } => (pool(name), None, reason.clone()),
            Self::InvalidDirectory { key: name, reason } => {
                (Some("directory".into()), key(name), reason.clone())
            }
            Self::InvalidRegion { table, reason } => {
                (Some(table.path()), key("region"), reason.clone())
            }
            Self::MissingRegion { pool: name } => (
                pool(name),
                key("region"),
                "missing; a pool names its region, or a cluster whose region it takes".into(),
            ),
            Self::UnknownCluster {
                pool: name,
                cluster,
            } => (
                pool(name),
                key("cluster"),
                format!("the fleet has no cluster `{cluster}`"),
            ),
            Self::RegionNotCluster {
                pool: name,
                region,
                cluster,
                cluster_region,
            } => (
                pool(name),
                key("region"),
                format!(
                    "`{region}` is not the region of cluster `{cluster}`, which is `{cluster_region}`; a pool's region is its cluster's"
                ),
            ),
            Self::InvalidFleetName { reason } => {
                (Some(FleetTable::Fleet.path()), key("name"), reason.clone())
            }
            Self::InvalidClusterName {
                cluster: name,
                reason,
            } => (cluster(name), None, reason.clone()),
            Self::EmptyContext { cluster: name } => (
                cluster(name),
                key("context"),
                "is empty; a cluster names the kube context that reaches it".into(),
            ),
            Self::InvalidNamespace {
                cluster: name,
                reason,
            } => (cluster(name), key("namespace"), reason.clone()),
            Self::SharedNamespace {
                cluster: name,
                first,
                context,
                namespace,
            } => (
                cluster(name),
                None,
                format!(
                    "cluster `{first}` has the same context `{context}` and namespace `{namespace}`; each cluster deploys to a namespace of its own"
                ),
            ),
            Self::InvalidSecretName {
                cluster: name,
                secret,
                name: holder,
            } => (
                cluster(name),
                Some(format!("secrets.{secret}")),
                format!(
                    "`{holder}` is not a Kubernetes Secret's name: lowercase letters, digits, `-`, and `.`, starting and ending with a letter or digit, at most 253 characters"
                ),
            ),
            Self::InvalidAnnotation { cluster: name, key } => (
                cluster(name),
                Some(format!("service_account_annotations.{key}")),
                "not an annotation key: an optional DNS subdomain and `/`, then at most 63 letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit".into(),
            ),
            Self::InvalidVariable {
                cluster: name,
                variable,
            } => (
                cluster(name),
                Some(format!("env.{variable}")),
                "not a variable's name: letters, digits, and `_`, not starting with a digit".into(),
            ),
            Self::InvalidLayout { pool: name, reason } => {
                (pool(name), key("layout"), reason.clone())
            }
            Self::MissingLayout {
                pool: name,
                layouts,
            } => (
                pool(name),
                key("layout"),
                format!(
                    "missing; the spec has layouts {}, and a pool names the one it runs",
                    quoted(layouts)
                ),
            ),
            Self::UnknownLayout {
                pool: name,
                layout,
                layouts,
            } => (
                pool(name),
                key("layout"),
                format!(
                    "the spec has no layout `{layout}`; its layouts are {}",
                    quoted(layouts)
                ),
            ),
            Self::HomeInOtherLayout {
                pool: name,
                home,
                home_layout,
                layout,
            } => (
                pool(name),
                key("home"),
                format!(
                    "pool `{home}` runs layout `{home_layout}`, not `{layout}`; a home is a pool of the pool's own layout"
                ),
            ),
            Self::UnknownHome { pool: name, home } => (
                pool(name),
                key("home"),
                format!("the fleet has no pool `{home}`"),
            ),
            Self::MissingParent {
                pool: name,
                class,
                parents,
            } => (
                pool(name),
                key("home"),
                format!(
                    "missing; a pool of class `{class}` names its parent, a pool of a class that sends to it, or `parents`, one such class, whose every pool follows it: {}",
                    quoted(parents)
                ),
            ),
            Self::ParentOfTopClass {
                pool: name,
                class,
                key: written,
            } => (
                pool(name),
                key(written),
                format!("no class sends to `{class}`, so its pools have no parent"),
            ),
            Self::HomeAndParents {
                pool: name,
                home,
                parents,
            } => (
                pool(name),
                key("parents"),
                format!(
                    "`{parents}` is given with `home = \"{home}\"`; a pool names one pool as its parent, `home`, or every pool of a class, `parents`, not both"
                ),
            ),
            Self::ParentsNotSender {
                pool: name,
                parents,
                class,
                layout,
            } => (
                pool(name),
                key("parents"),
                format!(
                    "class `{parents}` does not send to class `{class}`{}; `parents` is a class that sends to the pool's",
                    within(layout)
                ),
            ),
            Self::SharedUnderRelay {
                pool: name,
                parents,
            } => (
                pool(name),
                key("parents"),
                format!(
                    "every pool of class `{parents}` follows a shared pool, so it needs a directory each of their hubs reads; a fleet's pools publish through `relay`, where a pool reports its record to one parent's hub: name the pool's parent with `home`"
                ),
            ),
            Self::HomeNotParent {
                pool: name,
                home,
                home_class,
                class,
            } => (
                pool(name),
                key("home"),
                format!(
                    "pool `{home}` is of class `{home_class}`, which does not send to `{class}`; a home is a pool of a class that sends to the pool's"
                ),
            ),
            Self::MissingChild {
                pool: name,
                layout,
                class,
            } => (
                pool(name),
                None,
                format!(
                    "no child of class `{class}` in layout `{layout}`; a pool of a class that sends to `{class}` has a child of it, a pool whose `home` is this one or whose `parents` is its class"
                ),
            ),
        };
        Problem::at(table, key, message)
    }
}

impl fmt::Display for FleetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.problem().fmt(f)
    }
}

/// Every reason a fleet file was rejected; the check reports all of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetErrors(pub Vec<FleetError>);

impl fmt::Display for FleetErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for FleetErrors {}

impl From<FleetErrors> for Problems {
    fn from(errors: FleetErrors) -> Self {
        Problems(errors.0.iter().map(FleetError::problem).collect())
    }
}

impl From<FleetErrors> for Error {
    fn from(errors: FleetErrors) -> Self {
        Error::new(errors.to_string())
    }
}

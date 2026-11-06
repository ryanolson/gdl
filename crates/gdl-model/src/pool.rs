use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{Layout, LeaveBy, Problem, Problems, PropertyKey, check_layout_name};

/// A pool's own settings: the `[pool]` table of a segment's settings
/// (`GDL_POOL__NAME`, `GDL_POOL__READY__<SEGMENT>`, and so on), which tells
/// a process which pool it belongs to. The fleet file fills them in for
/// each pool ([`crate::Fleet::pool`]).
///
/// ```toml
/// name = "model-x-01-us-east-1"
/// layout = "regional"          # the spec's layout the pool runs, when it has several
/// class = "model_x"
/// region = "us-east-1"
/// home = "global-us-east-1"    # the parent pool, for a class that another sends to: whose hub follows this one
/// # parents = "global"         # or, in place of `home`, the parent class: every pool of it follows this one (not with "relay")
/// directory = "relay"          # optional: "relay" (the default), "memory", "dir", "dynamodb", or "etcd"
/// parent_hub = "global-hub:1337" # with "relay", for a class that another sends to: the parent's hub
/// # dir = "/var/lib/gdl/pools" # with "dir", and only with it: the shared directory
/// # records_table = "gdl-pools"     # with "dynamodb", and only with it: the records table,
/// # lists_table = "gdl-pool-lists"  # the lists table,
/// # aws_region = "us-east-1"        # the region whose copy of the tables the hub uses,
/// # endpoint = "http://dynamodb:8000" # and, optionally, a DynamoDB Local to use instead of AWS
/// # endpoints = ["http://etcd:2379"] # with "etcd", and only with it: the etcd cluster's members,
/// # prefix = "/gdl/pools"           # and, optionally, the prefix of the directory's keys
/// hub = "model-x-01-hub:1337"  # optional: where other pools' hubs reach this pool's hub
/// summary_ms = 10000           # optional: the load summary's period, at least 1000
/// hold_ms = 2000               # optional, the hub's alone: how long the quorum holds before `ready`
/// heartbeat_ms = 10000         # optional, the hub's alone: how often the record is published
/// lease_ms = 30000             # optional, the hub's alone: how long readers keep it; longer than the heartbeat, plus the poll period under `dir` and `dynamodb`
/// watch_buffer = 1024          # optional, the hub's alone: events a watcher may fall behind by
/// lost_chooser_ms = 30000      # optional, the hub's alone: how long it waits for a chooser with no watch of its relay
/// keyed_capacity = 1000000     # optional, the hub's alone: keyed records its table holds
/// drain_on_stop = false        # optional, the hub's alone: drain the pool on the first SIGTERM or SIGINT
/// # stop_wait_ms = 660000      # with drain_on_stop, and only with it: how long the hub waits for the drain
/// # leave_by = 1800000000000   # with drain_on_stop, and only with it: unix ms by which the hub must be gone
/// # leave_margin_ms = 30000    # with leave_by, and only with it: how long before leave_by the stop must end
/// [watch_caps]                 # optional, the hub's alone: watches each route serves at once
/// relay = 2048
/// [ready]                      # optional: the quorum of each segment of the class
/// e = 4
/// [requires]                   # optional: ready pools of each class it sends to that its health needs; 1 of each when unsaid
/// model_y = 1
/// ```
///
/// A pool whose instances report no properties of a kind declares, for
/// its hub alone, the properties of that kind it serves, each a key of
/// the kind's own type:
///
/// ```toml
/// [properties]
/// shard = [{ name = "a" }, { name = "b", partition = "p" }]
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Table", into = "Table")]
pub struct PoolSettings {
    /// The pool's name.
    pub name: String,
    /// The layout the pool runs: one of its spec's `[layout.<name>]`
    /// tables, or `default` for a spec without them. The fleet file
    /// requires it when the spec has more than one layout. Nothing in an
    /// instance or a hub reads it: a pool's placement hash is its layout's,
    /// and a hub follows only pools whose placement hash is its own, so
    /// pools of different layouts never follow or offer to each other.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<String>,
    /// The class the pool deploys.
    pub class: String,
    /// The region the pool runs in.
    pub region: String,
    /// The pool's parents: the pools of a class that sends to this one
    /// whose hubs follow this pool, and whose choosers alone offer it work
    /// (`home` or `parents`). The pool's record names them. The fleet file
    /// requires it for a pool of a class that another class sends to, and
    /// refuses it for one of a class that nothing sends to; a pool given
    /// settings without a fleet file that names none is followed by no
    /// pool.
    pub parent: Option<Parent>,
    /// Where the pool's record is published and the other pools are found:
    /// the `directory` key, with the `parent_hub` and `state_dir` keys for
    /// [`DirectoryBackend::Relay`], the `dir` key for
    /// [`DirectoryBackend::Dir`], and the `records_table`, `lists_table`,
    /// `aws_region`, and `endpoint` keys for [`DirectoryBackend::DynamoDb`],
    /// and the `endpoints` and `prefix` keys for [`DirectoryBackend::Etcd`];
    /// [`DirectoryBackend::DEFAULT`] when it is not set.
    pub directory: DirectoryBackend,
    /// Usable instances each segment of the class needs for the pool to be
    /// ready; see [`PoolSettings::quorum`].
    pub ready: BTreeMap<String, u32>,
    /// How many ready pools of each class the pool's class sends to its
    /// hub must follow for the pool health endpoint to answer that the
    /// pool is healthy. Empty when unsaid, which requires one of each
    /// class the pool's class sends to ([`PoolSettings::requirements`]).
    /// Only the pool's hub reads it.
    pub requires: BTreeMap<String, u32>,
    /// The address, `host:port`, at which other pools' hubs reach this
    /// pool's hub: the hub address of the pool's record. Only the pool's
    /// hub reads it, and it needs it to publish the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub: Option<String>,
    /// The load summary's period (`summary_ms`): how often each entry
    /// instance of the pool reports its admission to the pool's hub, and
    /// how often the hub summarizes the reports. The hub and its entry
    /// instances read the same setting, so that a report arrives once a
    /// summary.
    pub summary: Duration,
    /// How the pool's hub paces its judgment, its record, and its watches,
    /// and how many watches it serves at once. Only the pool's hub reads
    /// it.
    pub timing: HubTiming,
    /// The properties the pool serves when its entry instances report
    /// none of their kind (`[properties]`, a list of keys for each kind):
    /// each usable entry instance that reports no list of a kind serves
    /// the kind's. Sorted by kind, then in the order written. Only the
    /// pool's hub reads it.
    pub properties: Vec<PropertyKey>,
    /// Whether the pool's hub drains the pool when it is first signalled
    /// to stop (SIGTERM or SIGINT), and how long it waits for that before
    /// it stops (`drain_on_stop = true`, with `stop_wait_ms`, by default
    /// [`PoolSettings::DEFAULT_STOP_WAIT`]). The hub waits for the drained
    /// pool's record to be withdrawn, or, for a pool whose class no hop
    /// enters, which cannot drain, for its instances to leave. A second
    /// signal stops it at once. `None`, the default, stops the hub at its
    /// first signal. Only the pool's hub reads it.
    pub drain_on_stop: Option<Duration>,
    /// When the pool's hub must be gone, if it knows, as a batch job knows
    /// its walltime (`leave_by`, unix ms, with `leave_margin_ms`, by
    /// default [`LeaveBy::DEFAULT_MARGIN`]). The hub starts by itself the
    /// stop it runs on its first signal, the stop's wait and the margin
    /// before then, so it is read only with `drain_on_stop`. Only the
    /// pool's hub reads it.
    pub leave_by: Option<LeaveBy>,
}

/// How a pool's hub paces its judgment, its record, and its watches, and
/// how many of its watches it serves at once, and how many keyed records
/// it holds: the `[pool]` keys `hold_ms`, `heartbeat_ms`, `lease_ms`,
/// `watch_buffer`, `lost_chooser_ms`, `[watch_caps]`, and
/// `keyed_capacity`. Each key that is not set takes its default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubTiming {
    /// How long every segment's quorum must hold before the pool is
    /// `ready` (`hold_ms`).
    pub hold: Duration,
    /// How often the pool's record is published while its state does not
    /// change (`heartbeat_ms`).
    pub heartbeat: Duration,
    /// How long a reader keeps the record without seeing it published
    /// again (`lease_ms`): longer than the heartbeat, or the record would
    /// lapse between two publishes.
    pub lease: Duration,
    /// How many events a watcher of the pool, or of what its hub follows,
    /// may fall behind by before its watch ends (`watch_buffer`): at least
    /// this many, rounded up to a power of two.
    pub watch_buffer: usize,
    /// How long a chooser among the hub's members may have no watch of the
    /// hub's relay open before the hub stops waiting for it to say that it
    /// holds no offer to a followed pool (`lost_chooser_ms`): from when its
    /// last watch closed, or from when it said it runs a follower, if it
    /// has opened none since. A chooser that watches again is waited for
    /// again.
    pub lost_chooser: Duration,
    /// How many watches each of the hub's watch routes serves at once
    /// (`[watch_caps]`).
    pub watch_caps: WatchCaps,
    /// How many keyed records the hub's keyed record table holds before
    /// it evicts the least recently used (`keyed_capacity`): at least 1.
    pub keyed_capacity: usize,
}

impl HubTiming {
    /// How long the quorum holds before the pool is `ready` when `hold_ms`
    /// is not set: 2 s.
    pub const DEFAULT_HOLD: Duration = Duration::from_secs(2);

    /// How often the record is published when `heartbeat_ms` is not set:
    /// 10 s.
    pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(10);

    /// How long a reader keeps the record when `lease_ms` is not set: three
    /// default heartbeats, so that one late publish does not cost it.
    pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

    /// The watch buffer when `watch_buffer` is not set: 1024 events.
    pub const DEFAULT_WATCH_BUFFER: usize = 1024;

    /// How long a chooser may have no watch of the relay open when
    /// `lost_chooser_ms` is not set: 30 s, velo-hub's default lease of a
    /// registration. A chooser that loses its hub altogether is let go of
    /// when its lease ends, and one that loses only its watch at about the
    /// same time. A chooser that is up watches again within about a second
    /// (its cap on waits between watches), so the bound is far from it.
    pub const DEFAULT_LOST_CHOOSER: Duration = Duration::from_secs(30);

    /// The keyed record table's capacity when `keyed_capacity` is not
    /// set: 1,000,000 records, about 100 MB of small records when full.
    /// The table holds only what is written, so an unused one costs
    /// nothing.
    pub const DEFAULT_KEYED_CAPACITY: usize = 1_000_000;

    /// The largest watch buffer, 65536 events: 64 times the default. The
    /// hub keeps a buffer for the pool and one for each pool it follows,
    /// and each takes its slots when it is made, so a buffer far past
    /// what a slow watcher needs costs memory for nothing.
    pub const MAX_WATCH_BUFFER: usize = 65536;

    /// The shortest cadence a pool's hub takes from its parent's answer
    /// under the `relay` backend, 100 ms, so that a parent's mistake
    /// cannot make a child report in a loop.
    pub const MIN_RELAY_CADENCE: Duration = Duration::from_millis(100);

    /// The shortest lease under the `relay` backend, 300 ms: three reports
    /// at [`HubTiming::MIN_RELAY_CADENCE`], as a child reports within a
    /// third of its lease so that two lost reports do not drop it.
    pub const MIN_RELAY_LEASE: Duration = Self::MIN_RELAY_CADENCE.saturating_mul(3);
}

impl Default for HubTiming {
    fn default() -> Self {
        Self {
            hold: Self::DEFAULT_HOLD,
            heartbeat: Self::DEFAULT_HEARTBEAT,
            lease: Self::DEFAULT_LEASE,
            watch_buffer: Self::DEFAULT_WATCH_BUFFER,
            lost_chooser: Self::DEFAULT_LOST_CHOOSER,
            watch_caps: WatchCaps::default(),
            keyed_capacity: Self::DEFAULT_KEYED_CAPACITY,
        }
    }
}

/// How many watches each watch route of a pool's hub serves at once. A
/// watch asked for beyond its route's cap is refused with 503 and a
/// `Retry-After`, before any snapshot is built or any subscription taken
/// for it; a watch that closes frees its place at once. The watches a hub
/// gives in process are not capped.
///
/// Each default is sized for a large healthy pool, of some hundreds of
/// instances: at least twice what a pool of 500 holds open, so that a
/// pool whose instances all restart at once, each counted by its old
/// connection until the hub sees it close and by its new one, is still
/// served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchCaps {
    /// The pool's watch (`GET watch`), opened by each of the pool's own
    /// instances and by its parent's hub, one per follow of it.
    pub pool: usize,
    /// The relay (`GET follows`), opened by each chooser instance of the
    /// pool.
    pub relay: usize,
    /// The watch of one follow (`GET follows/{class}/{pool}`), opened by a
    /// chooser for each followed pool it reads again after an event of a
    /// kind it does not know, and held until its next watch of the relay:
    /// one per chooser and per followed pool when an event of a new kind
    /// names every follow.
    pub follow: usize,
}

impl WatchCaps {
    /// The default cap of the pool's watch: one per instance of a pool of
    /// 500, and one per follow of its parent's hub, which holds a few at
    /// most, four times over.
    pub const DEFAULT_POOL: usize = 2048;
    /// The default cap of the relay: one per chooser instance of a pool of
    /// 500, four times over.
    pub const DEFAULT_RELAY: usize = 2048;
    /// The default cap of the watches of one follow: one per chooser of a
    /// pool of 500 and per followed pool, for a parent that follows 16
    /// pools, every one read again, twice over. Each costs one pool's
    /// snapshot, and its pool's channel is shared, so it is far cheaper
    /// than a watch of the relay.
    pub const DEFAULT_FOLLOW: usize = 16384;
}

impl Default for WatchCaps {
    fn default() -> Self {
        Self {
            pool: Self::DEFAULT_POOL,
            relay: Self::DEFAULT_RELAY,
            follow: Self::DEFAULT_FOLLOW,
        }
    }
}

/// Where pool records are published and watched. Each backend's own
/// settings arrive with the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryBackend {
    /// Each pool's hub reports its record to its parent pool's hub, which
    /// holds the records of its children: [`DirectoryBackend::DEFAULT`].
    /// A pool of a class that nothing sends to reports nowhere.
    Relay {
        /// The parent pool's hub, `host:port`: where the pool's hub reports
        /// its record. A pool's hub needs it when another class sends to
        /// the pool's class, and it is refused for any other class.
        parent_hub: Option<String>,
        /// The directory where the pool's hub keeps the records its
        /// children report, so that it still holds them after it restarts,
        /// as given: a relative path is the process's working directory's.
        /// A pool's hub needs it when the pool's class sends to another
        /// class, and it is refused for any other class.
        state_dir: Option<PathBuf>,
    },
    /// One directory in the process, shared by every hub in it: for tests
    /// that run several pools in one process.
    Memory,
    /// One file per pool under `path`, a directory that every hub on one
    /// host shares, through a volume under Compose.
    Dir {
        /// The shared directory, as given: a relative path is the
        /// process's working directory's.
        path: PathBuf,
    },
    /// Two DynamoDB tables, which global tables replicate to every region,
    /// so that every hub reads every pool's record, whatever its region.
    /// The hub needs the `dynamodb` feature of `gdl-hub`.
    DynamoDb {
        /// The records table: one item per pool.
        records_table: String,
        /// The lists table: one item per property list.
        lists_table: String,
        /// The AWS region whose copy of the tables the hub reads and
        /// writes. A key apart from the pool's `region`, which is a name
        /// of the fleet's and need not be an AWS region.
        aws_region: String,
        /// A DynamoDB Local, `http://host:port`, to use instead of AWS.
        /// The hub then signs with fixed test credentials, which DynamoDB
        /// Local never checks; without it, the hub takes its credentials
        /// from its environment, such as a Kubernetes service account's
        /// role.
        endpoint: Option<String>,
    },
    /// An etcd cluster that every hub reaches: one key per pool for its
    /// last write and one for its property list, under `prefix`, and a
    /// watch that pushes each change. The hub needs the `etcd` feature of
    /// `gdl-hub`.
    Etcd {
        /// The members' client URLs, each `http://host:port`. The hub
        /// links no TLS, so each is plain `http://`.
        endpoints: Vec<String>,
        /// The prefix of every key the directory writes, without a
        /// trailing `/`: [`DirectoryBackend::ETCD_PREFIX`] when unsaid.
        prefix: String,
    },
}

impl DirectoryBackend {
    /// The backend when `directory` is not set: the relay, with no parent's
    /// hub.
    pub const DEFAULT: Self = Self::Relay {
        parent_hub: None,
        state_dir: None,
    };

    /// How often a `dir` reader reads a watched class's directory: 50 ms.
    pub const DIR_POLL: Duration = Duration::from_millis(50);

    /// How often a `dynamodb` reader queries a watched class, unless its
    /// program sets another period: 1 s. Each query is billed.
    pub const DYNAMODB_POLL: Duration = Duration::from_secs(1);

    /// The prefix of an `etcd` directory's keys when `prefix` is not set.
    pub const ETCD_PREFIX: &str = "/gdl/pools";
}

/// The DynamoDB directory's tables, as `gdl-pools-dynamodb` makes and
/// reads them: each table's partition and sort key, both strings, and the
/// lists table's TTL attribute. Kept here, which both the backend and a
/// generator's `dynamodb/tables.json` read, so that the two never differ.
pub mod dynamodb_tables {
    /// The records table's partition key, the pool's class, and sort key,
    /// the pool's name.
    pub const RECORDS_KEYS: (&str, &str) = ("class", "pool");
    /// The lists table's partition key, `<class>/<pool>`, and sort key.
    pub const LISTS_KEYS: (&str, &str) = ("owner", "list");
    /// The lists table's TTL attribute.
    pub const TTL_ATTRIBUTE: &str = "expires_at";
}

impl DirectoryBackend {
    /// The `dynamodb` backend with its keys, checked as reading `[pool]`
    /// checks them: each table's name is a DynamoDB table's, the region an
    /// AWS region's code, and the endpoint an `http://` or `https://` URL.
    /// A problem names its key in `[pool]`.
    pub fn dynamodb(
        records_table: String,
        lists_table: String,
        aws_region: String,
        endpoint: Option<String>,
    ) -> Result<Self, Problems> {
        let at = |key: &str, message: String| {
            Problems::from(Problem::at(Some("pool".into()), Some(key.into()), message))
        };
        check_table_name(&records_table).map_err(|why| at("records_table", why))?;
        check_table_name(&lists_table).map_err(|why| at("lists_table", why))?;
        check_aws_region(&aws_region).map_err(|why| at("aws_region", why))?;
        if let Some(endpoint) = &endpoint {
            check_endpoint(endpoint).map_err(|why| at("endpoint", why))?;
        }
        Ok(Self::DynamoDb {
            records_table,
            lists_table,
            aws_region,
            endpoint,
        })
    }
}

impl DirectoryBackend {
    /// The `etcd` backend with its keys, checked as reading `[pool]`
    /// checks them: at least one endpoint, each an `http://` URL, and a
    /// prefix that starts with `/` and does not end with one. A problem
    /// names its key in `[pool]`.
    pub fn etcd(endpoints: Vec<String>, prefix: String) -> Result<Self, Problems> {
        let at = |key: &str, message: String| {
            Problems::from(Problem::at(Some("pool".into()), Some(key.into()), message))
        };
        if endpoints.is_empty() {
            return Err(at(
                "endpoints",
                "is empty; it lists the etcd members' client URLs, `http://host:port`".into(),
            ));
        }
        for endpoint in &endpoints {
            check_etcd_endpoint(endpoint).map_err(|why| at("endpoints", why))?;
        }
        check_etcd_prefix(&prefix).map_err(|why| at("prefix", why))?;
        Ok(Self::Etcd { endpoints, prefix })
    }
}

impl Default for DirectoryBackend {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Which pools follow a pool of a class that another class sends to, and
/// alone offer it work: one pool, or every pool of a class. Both forms are
/// one rule, a parent whose hub follows the pool, and differ only in how
/// many parents there are. Written as `home` or `parents` in `[pool]`,
/// the fleet file, and the pool's record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Parent {
    /// One pool, by its name (`home`): only that pool's hub follows this
    /// one. Any directory serves it.
    #[serde(rename = "home")]
    Pool(String),
    /// Every pool of a class, by its name (`parents`): each of their hubs
    /// follows this one, so it is shared. It needs a directory that every
    /// parent's hub reads, never `relay`, where a pool reports its record
    /// to one parent's hub.
    #[serde(rename = "parents")]
    Class(String),
}

impl Parent {
    /// Whether the pool `pool` of class `class` is one of these parents.
    pub fn includes(&self, pool: &str, class: &str) -> bool {
        match self {
            Self::Pool(name) => name == pool,
            Self::Class(name) => name == class,
        }
    }

    /// The key that writes this form: `home` or `parents`.
    pub fn key(&self) -> &'static str {
        match self {
            Self::Pool(_) => "home",
            Self::Class(_) => "parents",
        }
    }

    /// The parent from the keys `home` and `parents` as written: at most
    /// one of them, or the key of the second and why it is refused.
    pub fn from_keys(
        home: Option<String>,
        parents: Option<String>,
    ) -> Result<Option<Self>, (&'static str, String)> {
        match (home, parents) {
            (Some(home), Some(parents)) => Err((
                "parents",
                format!(
                    "`{parents}` is given with `home = \"{home}\"`; a pool names one pool as its parent, `home`, or every pool of a class, `parents`, not both"
                ),
            )),
            (Some(home), None) => Ok(Some(Self::Pool(home))),
            (None, Some(parents)) => {
                crate::check_class_name(&parents).map_err(|why| ("parents", why))?;
                Ok(Some(Self::Class(parents)))
            }
            (None, None) => Ok(None),
        }
    }

    /// The keys `home` and `parents` that write `parent`.
    pub fn into_keys(parent: Option<Self>) -> (Option<String>, Option<String>) {
        match parent {
            Some(Self::Pool(home)) => (Some(home), None),
            Some(Self::Class(parents)) => (None, Some(parents)),
            None => (None, None),
        }
    }
}

impl PoolSettings {
    /// The load summary's period when `summary_ms` is not set: 10 s.
    pub const DEFAULT_SUMMARY: Duration = Duration::from_secs(10);

    /// The shortest load summary period, 1 s: a report per entry instance and a
    /// summary per pool each period, which a shorter one would make a load
    /// of their own.
    pub const MIN_SUMMARY: Duration = Duration::from_secs(1);

    /// The quorum of a segment that neither the pool's `ready` nor, in
    /// the fleet file, its class's names: one usable instance.
    pub const DEFAULT_QUORUM: u32 = 1;

    /// The ready pools of each class its class sends to that a pool's
    /// health requires when `requires` is unsaid.
    pub const DEFAULT_REQUIRES: u32 = 1;

    /// How long a hub that drains on stop waits when `stop_wait_ms` is not
    /// set: 660 s, an instance's default drain bound (the request
    /// deadline, 600 s) and a minute for the waits before its drain, so
    /// that the hub outlasts instances stopped by the same signal.
    pub const DEFAULT_STOP_WAIT: Duration = Duration::from_secs(660);

    /// The usable instances `segment` needs for the pool to be ready: its
    /// `ready` entry, else [`PoolSettings::DEFAULT_QUORUM`]. The fleet file
    /// supplies the class's default when it fills in a pool's settings, so
    /// the fallback applies only to a segment that neither the pool nor
    /// its class gives a quorum.
    pub fn quorum(&self, segment: &str) -> u32 {
        self.ready
            .get(segment)
            .copied()
            .unwrap_or(Self::DEFAULT_QUORUM)
    }

    /// The ready pools of each class its class sends to in `layout` that
    /// the pool's health requires, among the children its hub follows: its
    /// `requires`, or, when that is unsaid, each class its class sends to
    /// with [`PoolSettings::DEFAULT_REQUIRES`]. An explicit `requires`
    /// replaces the default whole, so a class it does not name is not
    /// required. A pool of a class that sends to none requires nothing.
    pub fn requirements(&self, layout: &Layout) -> BTreeMap<String, u32> {
        if !self.requires.is_empty() {
            return self.requires.clone();
        }
        sent(layout, &self.class)
            .into_iter()
            .map(|class| (class.to_string(), Self::DEFAULT_REQUIRES))
            .collect()
    }

    /// Whether these settings fit an instance of `segment` in `layout`: the
    /// placement has classes, the pool's class is one of them, the segment
    /// is in it, every `ready` key is a segment of it, and every `requires`
    /// key is a class it sends to, with a count of at least 1. Every
    /// problem is reported, each with its key in `[pool]`; its reader adds
    /// the file or the variable.
    pub fn check(&self, layout: &Layout, segment: &str) -> Result<(), Problems> {
        refuse(self.problems(layout, Some(segment)))
    }

    /// Whether these settings fit the pool's hub in `layout`, publishing
    /// to the directory they name: the rule of [`PoolSettings::check`]
    /// without an instance's own segment, since a hub has none. Every
    /// problem is reported.
    ///
    /// A hub also needs `parent_hub` with the `relay` backend when another
    /// class sends to its pool's class, since it reports there
    /// ([`missing_parent_hub`]), and `state_dir` when its pool's class
    /// sends to another class, since it keeps its children's records there
    /// ([`missing_state_dir`]). A shared pool is refused with the `relay`
    /// backend ([`shared_under_relay`]).
    pub fn check_hub(&self, layout: &Layout) -> Result<(), Problems> {
        let mut problems = self.problems(layout, None);
        if let DirectoryBackend::Relay {
            parent_hub,
            state_dir,
        } = &self.directory
        {
            if let Some(Parent::Class(parents)) = &self.parent {
                problems.push(shared_under_relay(parents));
            }
            if let (None, Some(parent)) = (parent_hub, sent_to(layout, &self.class)) {
                problems.push(missing_parent_hub(parent, &self.class));
            }
            if let (None, Some(child)) = (state_dir, sent(layout, &self.class).first()) {
                problems.push(missing_state_dir(&self.class, child));
            }
        }
        refuse(problems)
    }

    /// [`PoolSettings::check_hub`] for a hub whose program gives it its
    /// directory: what the settings' own directory needs is not checked,
    /// since the hub does not publish there.
    pub fn check_hub_given_directory(&self, layout: &Layout) -> Result<(), Problems> {
        refuse(self.problems(layout, None))
    }

    /// Read a `[pool]` table from `deserializer`, a TOML table or a
    /// figment value. A table that does not read is one problem, with the
    /// key it is about (`ready.e`, say) in table `pool`; its reader adds
    /// the file or the variable, which [`pool_variable`] names.
    pub fn read<'de, D>(deserializer: D) -> Result<Self, Problems>
    where
        D: serde::Deserializer<'de>,
        D::Error: std::fmt::Display,
    {
        let table: Table = serde_path_to_error::deserialize(deserializer).map_err(|error| {
            let path = error.path().clone();
            let mut problem = Problem::from_path(&path, error.into_inner().to_string());
            problem.table = Some(match problem.table.take() {
                Some(table) => format!("pool.{table}"),
                None => "pool".into(),
            });
            Problems::from(problem)
        })?;
        Self::try_from(table)
    }

    fn problems(&self, layout: &Layout, segment: Option<&str>) -> Vec<Problem> {
        let class = self.class.as_str();
        let at =
            |key: String, message: String| Problem::at(Some("pool".into()), Some(key), message);
        let classes = layout.class_segments();
        if classes.is_empty() {
            vec![at(
                "class".into(),
                format!("the placement has no classes, and so no class `{class}`"),
            )]
        } else if !classes.contains_key(class) {
            vec![at(
                "class".into(),
                format!("the placement has no class `{class}`"),
            )]
        } else {
            let own = segment.and_then(|segment| match layout.class_of(segment) {
                Some(own) if own == class => None,
                Some(own) => Some(at(
                    "class".into(),
                    format!(
                        "this instance's segment `{segment}` is in class `{own}`, not `{class}`"
                    ),
                )),
                None => Some(at(
                    "class".into(),
                    format!("this instance's segment `{segment}` is not in class `{class}`"),
                )),
            });
            let strays = self
                .ready
                .keys()
                .filter(|key| layout.class_of(key) != Some(class))
                .map(|key| {
                    at(
                        format!("ready.{key}"),
                        format!("not a segment of class `{class}`"),
                    )
                });
            let sent = sent(layout, class);
            let requires = self.requires.iter().filter_map(|(required, &count)| {
                let key = format!("requires.{required}");
                if !sent.contains(required.as_str()) {
                    Some(at(key, format!("not a class that `{class}` sends to")))
                } else if count == 0 {
                    Some(at(key, "is 0; a requirement is at least 1".into()))
                } else {
                    None
                }
            });
            let (reports, keeps) = match &self.directory {
                DirectoryBackend::Relay {
                    parent_hub,
                    state_dir,
                } => (
                    (parent_hub.is_some() && sent_to(layout, class).is_none()).then(|| {
                        at(
                            "parent_hub".into(),
                            format!(
                                "no class sends to class `{class}`, so its pool has no parent to report to"
                            ),
                        )
                    }),
                    (state_dir.is_some() && sent.is_empty()).then(|| {
                        at(
                            "state_dir".into(),
                            format!(
                                "class `{class}` sends to no class, so its pool has no children whose records its hub keeps"
                            ),
                        )
                    }),
                ),
                _ => (None, None),
            };
            let parents = match &self.parent {
                Some(Parent::Class(parents))
                    if !layout
                        .boundaries()
                        .iter()
                        .any(|boundary| boundary.from == *parents && boundary.into == class) =>
                {
                    Some(at(
                        "parents".into(),
                        format!(
                            "class `{parents}` does not send to class `{class}`; `parents` is a class that sends to the pool's"
                        ),
                    ))
                }
                _ => None,
            };
            let unsent = (!self.properties.is_empty() && sent_to(layout, class).is_none())
                .then(|| {
                    at(
                        "properties".into(),
                        format!(
                            "no class sends to class `{class}`, so its pool serves no properties to another"
                        ),
                    )
                });
            // The supply check counts a kind as served from settings only
            // where `[pools.<class>] provides` names it; a pool that served
            // another would serve what no check knows of.
            let provides = layout
                .pool_policy(class)
                .map(|policy| policy.provides.as_slice())
                .unwrap_or_default();
            let kinds: BTreeSet<&str> = match unsent {
                Some(_) => BTreeSet::new(),
                None => self.properties.iter().map(PropertyKey::kind).collect(),
            };
            let undeclared = kinds
                .into_iter()
                .filter(|kind| !provides.iter().any(|p| p == kind))
                .map(|kind| {
                    at(
                        format!("properties.{kind}"),
                        format!(
                            "`[pools.{class}] provides` does not name property kind `{kind}`, so the pool cannot serve it from its settings"
                        ),
                    )
                });
            let properties = unsent.into_iter().chain(undeclared);
            own.into_iter()
                .chain(strays)
                .chain(requires)
                .chain(reports)
                .chain(keeps)
                .chain(parents)
                .chain(properties)
                .collect()
        }
    }
}

/// The problem of a pool's hub under the `relay` backend that has no
/// parent's hub to report its record to, though class `parent` sends to
/// its class, `class`.
pub fn missing_parent_hub(parent: &str, class: &str) -> Problem {
    Problem::at(
        Some("pool".into()),
        Some("parent_hub".into()),
        format!(
            "missing: class `{parent}` sends to class `{class}`, so with directory = \"relay\" the pool's hub reports its record to its parent's hub, at this address"
        ),
    )
}

/// The problem of a shared pool, whose parents are every pool of class
/// `parents`, under the `relay` backend: a pool reports its record there
/// to one parent's hub, so the others would never find it.
pub fn shared_under_relay(parents: &str) -> Problem {
    Problem::at(
        Some("pool".into()),
        Some("parents".into()),
        format!(
            "every pool of class `{parents}` follows a shared pool, so it needs a directory each of their hubs reads; with directory = \"relay\" a pool reports its record to one parent's hub: name the pool's parent with `home`, or use another directory"
        ),
    )
}

/// The problem of a pool's hub under the `relay` backend that has nowhere
/// to keep the records its children report, though its class, `class`,
/// sends to class `child`.
pub fn missing_state_dir(class: &str, child: &str) -> Problem {
    Problem::at(
        Some("pool".into()),
        Some("state_dir".into()),
        format!(
            "missing: class `{class}` sends to class `{child}`, so with directory = \"relay\" the pool's hub holds its children's records, and keeps them in this directory across its restart"
        ),
    )
}

/// A class that sends to `class` in `layout`, if any.
fn sent_to<'a>(layout: &'a Layout, class: &str) -> Option<&'a str> {
    layout
        .boundaries()
        .iter()
        .find(|boundary| boundary.into == class)
        .map(|boundary| boundary.from.as_str())
}

/// The classes `class` sends to in `layout`.
fn sent<'a>(layout: &'a Layout, class: &str) -> BTreeSet<&'a str> {
    layout
        .boundaries()
        .iter()
        .filter(|boundary| boundary.from == class)
        .map(|boundary| boundary.into.as_str())
        .collect()
}

fn refuse(problems: Vec<Problem>) -> Result<(), Problems> {
    if problems.is_empty() {
        Ok(())
    } else {
        Err(Problems(problems))
    }
}

/// The variable that sets `key` of a pool's settings: `GDL_POOL__READY__E`
/// for `ready.e`. A variable is matched without regard to case, so this is
/// the spelling, not the only one.
pub fn pool_variable(key: &str) -> String {
    format!("GDL_POOL__{}", key.replace('.', "__").to_ascii_uppercase())
}

/// The refusal of a variable that sets a table both a segment and the
/// `gdl-hub` binary read, `GDL_HUB` or `GDL_POOL`, in a form only the
/// segment reads; `None` for any other name. The name is matched in any
/// case and with space around it, as both match names. Two forms are
/// refused: the table's name alone, which would hold the whole table as
/// TOML, and the table's name followed by `.`, which a segment would split
/// into keys (`GDL_HUB.PUBLIC_PORT`). The hub reads only `GDL_HUB__<KEY>`
/// and `GDL_POOL__<KEY>`, one key to a variable, so both readers refuse
/// these with this, and one environment never gives them two values.
pub fn whole_table_variable(name: &str) -> Option<String> {
    let name = name.trim();
    ["GDL_HUB", "GDL_POOL"].into_iter().find_map(|table| {
        let rest = name
            .get(..table.len())
            .filter(|given| given.eq_ignore_ascii_case(table))
            .map(|_| &name[table.len()..])?;
        (rest.is_empty() || rest.starts_with('.')).then(|| {
            format!(
                "{name}: the hub reads `[{}]` only from variables named {table}__<KEY>, one key to each; set each key that way",
                table["GDL_".len()..].to_ascii_lowercase()
            )
        })
    })
}

/// Whether `address` is `host:port` as a hub's address must be: a host
/// name of letters, digits, `-`, and `.`, an IPv4 address, or a bracketed
/// IPv6 address, and a port from 1 to 65535. Nothing else, so that a URL
/// or a stray space is refused where it is written rather than where
/// another hub dials it. Reading [`PoolSettings`] applies it; a hub
/// applies it again to settings made in code.
pub fn check_hub_address(address: &str) -> Result<(), String> {
    let shaped = address.rsplit_once(':').is_some_and(|(host, port)| {
        let port = port.parse::<u16>().is_ok_and(|port| port > 0)
            && port.bytes().all(|b| b.is_ascii_digit());
        let host = match host.strip_prefix('[') {
            Some(bracketed) => bracketed
                .strip_suffix(']')
                .is_some_and(|ip| ip.parse::<std::net::Ipv6Addr>().is_ok()),
            None => {
                !host.is_empty()
                    && host
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
            }
        };
        port && host
    });
    if shaped {
        Ok(())
    } else {
        Err(format!(
            "{address:?} must be `host:port`: a host name, an IPv4 address, or a bracketed IPv6 address, and a port from 1 to 65535"
        ))
    }
}

/// Whether `name` can name a DynamoDB table: 3 to 255 letters, digits,
/// `_`, `-`, and `.`, DynamoDB's own rule, so that a bad name is refused
/// where it is written rather than at the hub's first call.
pub(crate) fn check_table_name(name: &str) -> Result<(), String> {
    let shaped = (3..=255).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if shaped {
        Ok(())
    } else {
        Err(format!(
            "{name:?} must be a DynamoDB table name: 3 to 255 letters, digits, `_`, `-`, and `.`"
        ))
    }
}

/// Whether `region` can be an AWS region's code, such as `us-east-1`:
/// lowercase letters, digits, and `-`.
fn check_aws_region(region: &str) -> Result<(), String> {
    let shaped = region.starts_with(|c: char| c.is_ascii_lowercase())
        && region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if shaped {
        Ok(())
    } else {
        Err(format!(
            "{region:?} must be an AWS region's code, such as `us-east-1`"
        ))
    }
}

/// Whether `endpoint` can be a DynamoDB Local's address: an `http://` or
/// `https://` URL with a host.
fn check_endpoint(endpoint: &str) -> Result<(), String> {
    let host = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"));
    if host.is_some_and(|host| !host.is_empty() && !host.contains(char::is_whitespace)) {
        Ok(())
    } else {
        Err(format!(
            "{endpoint:?} must be a URL, `http://host:port`, of a DynamoDB Local"
        ))
    }
}

/// Whether `endpoint` can be an etcd member's client URL: `http://` and a
/// host. Not `https://`: the hub links no TLS, so a URL it cannot reach is
/// refused where it is written.
fn check_etcd_endpoint(endpoint: &str) -> Result<(), String> {
    let host = endpoint.strip_prefix("http://");
    if host.is_some_and(|host| !host.is_empty() && !host.contains(char::is_whitespace)) {
        Ok(())
    } else {
        Err(format!(
            "{endpoint:?} must be an `http://` URL, `http://host:port`, of an etcd member: the hub links no TLS"
        ))
    }
}

/// Whether `prefix` can prefix an etcd directory's keys: `/` and at least
/// one more character, none of them whitespace or a control character,
/// and no trailing `/`, since the directory adds `/<class>/<pool>`.
fn check_etcd_prefix(prefix: &str) -> Result<(), String> {
    let shaped = prefix.len() > 1
        && prefix.starts_with('/')
        && !prefix.ends_with('/')
        && prefix.chars().all(|c| c.is_ascii_graphic());
    if shaped {
        Ok(())
    } else {
        Err(format!(
            "{prefix:?} must start with `/`, not end with one, and hold no space, such as `/gdl/pools`"
        ))
    }
}

/// Whether `summary` can be a load summary's period: at least
/// [`PoolSettings::MIN_SUMMARY`]. Reading [`PoolSettings`] applies it; a
/// hub applies it again to settings made in code.
pub fn check_summary(summary: Duration) -> Result<(), String> {
    if summary < PoolSettings::MIN_SUMMARY {
        return Err(format!(
            "is {}; the period is at least {} ms",
            summary.as_millis(),
            PoolSettings::MIN_SUMMARY.as_millis()
        ));
    }
    Ok(())
}

/// Whether `lease` can be the lease of a pool whose hub reports its record
/// to its parent's hub (the `relay` backend): at least
/// [`HubTiming::MIN_RELAY_LEASE`], or the record could lapse at the parent
/// between two reports. Reading [`PoolSettings`] applies it to the `relay`
/// backend; a hub applies it again to timing made in code.
pub fn check_relay_lease(lease: Duration) -> Result<(), String> {
    if lease < HubTiming::MIN_RELAY_LEASE {
        return Err(format!(
            "is {}; under the `relay` backend it is at least {} ms, three reports at the shortest cadence a parent can ask for, {} ms, or the record lapses at the parent between reports",
            lease.as_millis(),
            HubTiming::MIN_RELAY_LEASE.as_millis(),
            HubTiming::MIN_RELAY_CADENCE.as_millis()
        ));
    }
    Ok(())
}

/// Whether `timing` suits the readers of `directory`: under a backend
/// whose readers poll, `dir` ([`DirectoryBackend::DIR_POLL`]) and
/// `dynamodb` ([`DirectoryBackend::DYNAMODB_POLL`]), a lease longer than
/// the heartbeat and the poll period together. A reader sees a publish
/// only at its next poll, so two publishes can reach it that far apart,
/// and a shorter lease lapses between polls in every reader. `relay` and
/// `memory` push each change, and take any lease. Reading
/// [`PoolSettings`] applies it; a hub applies it again to settings made
/// in code.
pub fn check_polled_lease(directory: &DirectoryBackend, timing: &HubTiming) -> Result<(), String> {
    let Some((backend, poll)) = polled(directory) else {
        return Ok(());
    };
    let least = timing.heartbeat.saturating_add(poll);
    if timing.lease <= least {
        return Err(format!(
            "is {}; it must be longer than {} ms, the heartbeat, {} ms, and the `{backend}` backend's poll period, {} ms, together, or readers let the record lapse between two polls",
            millis(timing.lease),
            millis(least),
            millis(timing.heartbeat),
            millis(poll)
        ));
    }
    Ok(())
}

/// The name and poll period of a backend whose readers poll.
fn polled(directory: &DirectoryBackend) -> Option<(&'static str, Duration)> {
    match directory {
        DirectoryBackend::Dir { .. } => Some(("dir", DirectoryBackend::DIR_POLL)),
        DirectoryBackend::DynamoDb { .. } => Some(("dynamodb", DirectoryBackend::DYNAMODB_POLL)),
        DirectoryBackend::Relay { .. }
        | DirectoryBackend::Memory
        | DirectoryBackend::Etcd { .. } => None,
    }
}

/// Whether `name` can name a pool: a DNS label.
pub fn check_pool_name(name: &str) -> Result<(), String> {
    check_label("pool", name)
}

/// Whether `name` can name a region: a DNS label.
pub fn check_region_name(name: &str) -> Result<(), String> {
    check_label("region", name)
}

/// The longest pool or region name: a DNS label's.
const LABEL_LIMIT: usize = 63;

/// Whether `name` can name a pool or a region: a DNS label. A pool's name
/// becomes a host name, a URL path segment, part of a hub's service names,
/// and part of a certificate's identity, and a region's is used beside it.
/// A fleet's clusters and the fleet's own name label Kubernetes objects,
/// and take the same rule. `kind` names what the name
/// is for in the refusal.
pub(crate) fn check_label(kind: &str, name: &str) -> Result<(), String> {
    let shaped = name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !shaped || name.len() > LABEL_LIMIT {
        return Err(format!(
            "{kind} name `{name}` must be lowercase letters, digits, and `-`, start with a letter, end with a letter or digit, and be at most {LABEL_LIMIT} characters"
        ));
    }
    Ok(())
}

/// The `[pool]` table as written. `directory`, `parent_hub`, `state_dir`,
/// `dir`, and the `dynamodb` and `etcd` backends' keys are keys of one choice, so
/// that the settings read flat (`GDL_POOL__DIR`),
/// and they are joined here, where a key given to a backend that reads
/// none can be refused rather than left unread.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    layout: Option<String>,
    class: String,
    region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    home: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parents: Option<String>,
    #[serde(default)]
    directory: Backend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_hub: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    state_dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    records_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lists_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    aws_region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    endpoints: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    ready: BTreeMap<String, u32>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    requires: BTreeMap<String, u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hub: Option<String>,
    #[serde(default = "default_summary_ms")]
    summary_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hold_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    heartbeat_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    watch_buffer: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lost_chooser_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Caps::is_empty")]
    watch_caps: Caps,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    keyed_capacity: Option<usize>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    drain_on_stop: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stop_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leave_by: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leave_margin_ms: Option<u64>,
}

/// The `[watch_caps]` table as written: each cap that is set.
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Caps {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pool: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    relay: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    follow: Option<usize>,
}

impl Caps {
    fn is_empty(&self) -> bool {
        self.pool.is_none() && self.relay.is_none() && self.follow.is_none()
    }
}

fn default_summary_ms() -> u64 {
    millis(PoolSettings::DEFAULT_SUMMARY)
}

/// `value`, unless it is `default`: a default is not written out, so that
/// a table written before the key existed is written alike.
fn set<T: PartialEq>(value: T, default: T) -> Option<T> {
    (value != default).then_some(value)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The `directory` key's values.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Backend {
    #[default]
    Relay,
    Memory,
    Dir,
    Dynamodb,
    Etcd,
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Backend::Relay => "relay",
            Backend::Memory => "memory",
            Backend::Dir => "dir",
            Backend::Dynamodb => "dynamodb",
            Backend::Etcd => "etcd",
        }
    }
}

impl TryFrom<Table> for PoolSettings {
    type Error = Problems;

    fn try_from(table: Table) -> Result<Self, Problems> {
        let at = |key: &str, message: String| {
            Problems::from(Problem::at(Some("pool".into()), Some(key.into()), message))
        };
        if let Some(parent_hub) = &table.parent_hub {
            if !matches!(table.directory, Backend::Relay) {
                return Err(at(
                    "parent_hub",
                    format!(
                        "{parent_hub:?} is read only by the `relay` backend, not by directory = \"{}\"",
                        table.directory.name()
                    ),
                ));
            }
            check_hub_address(parent_hub).map_err(|why| at("parent_hub", why))?;
        }
        if let Some(state_dir) = &table.state_dir {
            if !matches!(table.directory, Backend::Relay) {
                return Err(at(
                    "state_dir",
                    format!(
                        "{state_dir:?} is read only by the `relay` backend, not by directory = \"{}\"",
                        table.directory.name()
                    ),
                ));
            }
            if state_dir.as_os_str().is_empty() {
                return Err(at(
                    "state_dir",
                    "is empty; it is the path of the directory the hub keeps its children's records in".into(),
                ));
            }
        }
        if !matches!(table.directory, Backend::Dynamodb) {
            for (key, value) in [
                ("records_table", &table.records_table),
                ("lists_table", &table.lists_table),
                ("aws_region", &table.aws_region),
                ("endpoint", &table.endpoint),
            ] {
                if let Some(value) = value {
                    return Err(at(
                        key,
                        format!(
                            "{value:?} is read only by the `dynamodb` backend, not by directory = \"{}\"",
                            table.directory.name()
                        ),
                    ));
                }
            }
        }
        if !matches!(table.directory, Backend::Etcd) {
            if let Some(endpoints) = &table.endpoints {
                return Err(at(
                    "endpoints",
                    format!(
                        "{endpoints:?} is read only by the `etcd` backend, not by directory = \"{}\"",
                        table.directory.name()
                    ),
                ));
            }
            if let Some(prefix) = &table.prefix {
                return Err(at(
                    "prefix",
                    format!(
                        "{prefix:?} is read only by the `etcd` backend, not by directory = \"{}\"",
                        table.directory.name()
                    ),
                ));
            }
        }
        let timing = hub_timing(&table)?;
        let drain_on_stop = match (table.drain_on_stop, table.stop_wait_ms) {
            (false, None) => None,
            (false, Some(ms)) => {
                return Err(at(
                    "stop_wait_ms",
                    format!(
                        "{ms} is read only by a hub that drains on stop, and `drain_on_stop` is not true"
                    ),
                ));
            }
            (true, Some(0)) => {
                return Err(at(
                    "stop_wait_ms",
                    "is 0, which would stop the hub before its pool drains; it is at least 1"
                        .into(),
                ));
            }
            (true, wait) => Some(wait.map_or(Self::DEFAULT_STOP_WAIT, Duration::from_millis)),
        };
        let leave_by = match (table.leave_by, table.leave_margin_ms) {
            (None, None) => None,
            (None, Some(ms)) => {
                return Err(at(
                    "leave_margin_ms",
                    format!("{ms} is read only with `leave_by`, which is not set"),
                ));
            }
            (Some(ms), _) if drain_on_stop.is_none() => {
                return Err(at(
                    "leave_by",
                    format!(
                        "{ms} is read only by a hub that drains on stop, and `drain_on_stop` is not true"
                    ),
                ));
            }
            (Some(unix_ms), margin) => Some(LeaveBy {
                unix_ms,
                margin: margin.map_or(LeaveBy::DEFAULT_MARGIN, Duration::from_millis),
            }),
        };
        let parent =
            Parent::from_keys(table.home, table.parents).map_err(|(key, why)| at(key, why))?;
        let directory = match (table.directory, table.dir) {
            (Backend::Relay, None) => DirectoryBackend::Relay {
                parent_hub: table.parent_hub,
                state_dir: table.state_dir,
            },
            (Backend::Memory, None) => DirectoryBackend::Memory,
            (Backend::Dir, Some(path)) if !path.as_os_str().is_empty() => {
                DirectoryBackend::Dir { path }
            }
            (Backend::Dir, _) => {
                return Err(at(
                    "directory",
                    "is \"dir\", which needs `dir`, the path of the shared directory".into(),
                ));
            }
            (Backend::Dynamodb, None) => {
                let needs = |key: &str, what: &str| {
                    at(
                        "directory",
                        format!("is \"dynamodb\", which needs `{key}`, {what}"),
                    )
                };
                DirectoryBackend::dynamodb(
                    table
                        .records_table
                        .ok_or_else(|| needs("records_table", "the records table's name"))?,
                    table
                        .lists_table
                        .ok_or_else(|| needs("lists_table", "the lists table's name"))?,
                    table.aws_region.ok_or_else(|| {
                        needs(
                            "aws_region",
                            "the AWS region whose copy of the tables the hub uses",
                        )
                    })?,
                    table.endpoint,
                )?
            }
            (Backend::Etcd, None) => DirectoryBackend::etcd(
                table.endpoints.ok_or_else(|| {
                    at(
                        "directory",
                        "is \"etcd\", which needs `endpoints`, the etcd members' client URLs"
                            .into(),
                    )
                })?,
                table
                    .prefix
                    .unwrap_or_else(|| DirectoryBackend::ETCD_PREFIX.to_owned()),
            )?,
            (backend, Some(path)) => {
                return Err(at(
                    "dir",
                    format!(
                        "{path:?} is read only by the `dir` backend, not by directory = \"{}\"",
                        backend.name()
                    ),
                ));
            }
        };
        if matches!(directory, DirectoryBackend::Relay { .. }) {
            check_relay_lease(timing.lease).map_err(|why| at("lease_ms", why))?;
        }
        if let Err(why) = check_polled_lease(&directory, &timing) {
            return Err(match (table.lease_ms, polled(&directory)) {
                // The lease left at its default: the heartbeat that was
                // set is what to change.
                (None, Some((backend, poll))) => at(
                    "heartbeat_ms",
                    format!(
                        "is {}; it must be shorter than the lease, {} ms by default, by more than the `{backend}` backend's poll period, {} ms: set `lease_ms` longer too",
                        millis(timing.heartbeat),
                        millis(timing.lease),
                        millis(poll)
                    ),
                ),
                _ => at("lease_ms", why),
            });
        }
        if let Some(layout) = &table.layout {
            check_layout_name(layout).map_err(|why| at("layout", why))?;
        }
        if let Some(hub) = &table.hub {
            check_hub_address(hub).map_err(|why| at("hub", why))?;
        }
        let summary = Duration::from_millis(table.summary_ms);
        check_summary(summary).map_err(|why| at("summary_ms", why))?;
        let mut properties = Vec::new();
        let mut declared = BTreeSet::new();
        for (kind, keys) in table.properties {
            for key in keys {
                let property =
                    PropertyKey::new(kind.clone(), key).map_err(|why| at("properties", why))?;
                if !declared.insert(property.clone()) {
                    return Err(at(
                        "properties",
                        format!(
                            "property `{}` of kind `{kind}` is declared twice",
                            property.key()
                        ),
                    ));
                }
                properties.push(property);
            }
        }
        Ok(Self {
            name: table.name,
            layout: table.layout,
            class: table.class,
            region: table.region,
            parent,
            directory,
            ready: table.ready,
            requires: table.requires,
            hub: table.hub,
            summary,
            timing,
            properties,
            drain_on_stop,
            leave_by,
        })
    }
}

/// The hub's timing that `table` sets, each key it leaves out at its
/// default, or the key that is refused and why ([`check_hub_timing`]).
/// The lease is checked against the heartbeat as the hub will run them,
/// defaults included, and a refusal names the one of the two that was
/// set, the lease when both were.
fn hub_timing(table: &Table) -> Result<HubTiming, Problems> {
    let defaults = HubTiming::default();
    let millis_or =
        |value: Option<u64>, default: Duration| value.map_or(default, Duration::from_millis);
    let caps = &table.watch_caps;
    let timing = HubTiming {
        hold: millis_or(table.hold_ms, defaults.hold),
        heartbeat: millis_or(table.heartbeat_ms, defaults.heartbeat),
        lease: millis_or(table.lease_ms, defaults.lease),
        watch_buffer: table.watch_buffer.unwrap_or(defaults.watch_buffer),
        lost_chooser: millis_or(table.lost_chooser_ms, defaults.lost_chooser),
        watch_caps: WatchCaps {
            pool: caps.pool.unwrap_or(defaults.watch_caps.pool),
            relay: caps.relay.unwrap_or(defaults.watch_caps.relay),
            follow: caps.follow.unwrap_or(defaults.watch_caps.follow),
        },
        keyed_capacity: table.keyed_capacity.unwrap_or(defaults.keyed_capacity),
    };
    check_hub_timing(&timing).map_err(|problems| {
        // The lease left at its default: the heartbeat that was set is
        // what to change.
        let lease = |problem: &Problem| problem.key.as_deref() == Some("lease_ms");
        if table.lease_ms.is_none() && problems.0.iter().any(lease) {
            Problems::from(Problem::at(
                Some("pool".into()),
                Some("heartbeat_ms".into()),
                format!(
                    "is {}; it must be shorter than the lease, {} ms by default: set `lease_ms` longer too",
                    millis(timing.heartbeat),
                    millis(timing.lease)
                ),
            ))
        } else {
            problems
        }
    })?;
    Ok(timing)
}

/// Whether a pool's hub can run at `timing`: a heartbeat and a lease of at
/// least 1 ms, a lease longer than the heartbeat, so that readers do not
/// let the record lapse between two publishes, a watch buffer from 1 to
/// [`HubTiming::MAX_WATCH_BUFFER`], a bound on a lost chooser of at least
/// 1 ms, no watch cap of 0, and a keyed record capacity of at least 1. The problem
/// names its key in `[pool]`. Reading [`PoolSettings`] applies it; a hub
/// applies it again to settings made in code.
pub fn check_hub_timing(timing: &HubTiming) -> Result<(), Problems> {
    let at = |key: &str, message: String| {
        Err(Problems::from(Problem::at(
            Some("pool".into()),
            Some(key.into()),
            message,
        )))
    };
    if timing.heartbeat < Duration::from_millis(1) {
        return at("heartbeat_ms", "is 0; it is at least 1".into());
    }
    if timing.lease < Duration::from_millis(1) {
        return at("lease_ms", "is 0; it is at least 1".into());
    }
    if let Err(why) = crate::check_lease(timing.heartbeat, timing.lease) {
        return at("lease_ms", why);
    }
    if timing.watch_buffer == 0 {
        return at("watch_buffer", "is 0; it is at least 1".into());
    }
    if timing.watch_buffer > HubTiming::MAX_WATCH_BUFFER {
        return at(
            "watch_buffer",
            format!(
                "is {}; it is at most {}",
                timing.watch_buffer,
                HubTiming::MAX_WATCH_BUFFER
            ),
        );
    }
    if timing.lost_chooser < Duration::from_millis(1) {
        return at("lost_chooser_ms", "is 0; it is at least 1".into());
    }
    let caps = &timing.watch_caps;
    for (key, cap) in [
        ("watch_caps.pool", caps.pool),
        ("watch_caps.relay", caps.relay),
        ("watch_caps.follow", caps.follow),
    ] {
        if cap == 0 {
            return at(
                key,
                "is 0, which refuses every watch; it is at least 1".into(),
            );
        }
    }
    if timing.keyed_capacity == 0 {
        return at(
            "keyed_capacity",
            "is 0, which holds no record; it is at least 1".into(),
        );
    }
    Ok(())
}

impl From<PoolSettings> for Table {
    fn from(settings: PoolSettings) -> Self {
        let (mut records_table, mut lists_table, mut aws_region, mut endpoint) =
            (None, None, None, None);
        let (mut endpoints, mut prefix) = (None, None);
        let (directory, parent_hub, state_dir, dir) = match settings.directory {
            DirectoryBackend::Relay {
                parent_hub,
                state_dir,
            } => (Backend::Relay, parent_hub, state_dir, None),
            DirectoryBackend::Memory => (Backend::Memory, None, None, None),
            DirectoryBackend::Dir { path } => (Backend::Dir, None, None, Some(path)),
            DirectoryBackend::DynamoDb {
                records_table: records,
                lists_table: lists,
                aws_region: region,
                endpoint: local,
            } => {
                (records_table, lists_table, aws_region, endpoint) =
                    (Some(records), Some(lists), Some(region), local);
                (Backend::Dynamodb, None, None, None)
            }
            DirectoryBackend::Etcd {
                endpoints: members,
                prefix: under,
            } => {
                endpoints = Some(members);
                prefix = set(under, DirectoryBackend::ETCD_PREFIX.to_owned());
                (Backend::Etcd, None, None, None)
            }
        };
        let (home, parents) = Parent::into_keys(settings.parent);
        Self {
            name: settings.name,
            layout: settings.layout,
            class: settings.class,
            region: settings.region,
            home,
            parents,
            directory,
            parent_hub,
            state_dir,
            dir,
            records_table,
            lists_table,
            aws_region,
            endpoint,
            endpoints,
            prefix,
            ready: settings.ready,
            requires: settings.requires,
            hub: settings.hub,
            summary_ms: millis(settings.summary),
            hold_ms: set(settings.timing.hold, HubTiming::DEFAULT_HOLD).map(millis),
            heartbeat_ms: set(settings.timing.heartbeat, HubTiming::DEFAULT_HEARTBEAT).map(millis),
            lease_ms: set(settings.timing.lease, HubTiming::DEFAULT_LEASE).map(millis),
            watch_buffer: set(
                settings.timing.watch_buffer,
                HubTiming::DEFAULT_WATCH_BUFFER,
            ),
            lost_chooser_ms: set(
                settings.timing.lost_chooser,
                HubTiming::DEFAULT_LOST_CHOOSER,
            )
            .map(millis),
            watch_caps: Caps {
                pool: set(settings.timing.watch_caps.pool, WatchCaps::DEFAULT_POOL),
                relay: set(settings.timing.watch_caps.relay, WatchCaps::DEFAULT_RELAY),
                follow: set(settings.timing.watch_caps.follow, WatchCaps::DEFAULT_FOLLOW),
            },
            keyed_capacity: set(
                settings.timing.keyed_capacity,
                HubTiming::DEFAULT_KEYED_CAPACITY,
            ),
            properties: settings.properties.into_iter().fold(
                BTreeMap::new(),
                |mut kinds: BTreeMap<String, Vec<serde_json::Value>>, property| {
                    kinds
                        .entry(property.kind().to_string())
                        .or_default()
                        .push(property.key().clone());
                    kinds
                },
            ),
            drain_on_stop: settings.drain_on_stop.is_some(),
            stop_wait_ms: settings
                .drain_on_stop
                .and_then(|wait| set(wait, PoolSettings::DEFAULT_STOP_WAIT))
                .map(millis),
            leave_by: settings.leave_by.map(|leave| leave.unix_ms),
            leave_margin_ms: settings
                .leave_by
                .and_then(|leave| set(leave.margin, LeaveBy::DEFAULT_MARGIN))
                .map(millis),
        }
    }
}

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{Layout, Problem, Problems, check_layout_name};

/// A cell's own settings: the `[cell]` table of a segment's settings
/// (`GDL_CELL__NAME`, `GDL_CELL__READY__<SEGMENT>`, and so on), which tells
/// a process which cell it belongs to. The fleet file fills them in for
/// each cell ([`crate::Fleet::cell`]).
///
/// ```toml
/// name = "model-x-01-us-east-1"
/// layout = "regional"          # the spec's layout the cell runs, when it has several
/// class = "model_x"
/// region = "us-east-1"
/// home = "global-us-east-1"    # the parent cell, for a class that another sends to: whose hub follows this one
/// directory = "relay"          # optional: "relay" (the default), "memory", or "dir"
/// parent_hub = "global-hub:1337" # with "relay", for a class that another sends to: the parent's hub
/// # dir = "/var/lib/gdl/cells" # with "dir", and only with it: the shared directory
/// hub = "model-x-01-hub:1337"  # optional: where other cells' hubs reach this cell's hub
/// summary_ms = 10000           # optional: the load summary's period, at least 1000
/// hold_ms = 2000               # optional, the hub's alone: how long the quorum holds before `ready`
/// heartbeat_ms = 10000         # optional, the hub's alone: how often the record is published
/// lease_ms = 30000             # optional, the hub's alone: how long readers keep it; longer than the heartbeat
/// watch_buffer = 1024          # optional, the hub's alone: events a watcher may fall behind by
/// [watch_caps]                 # optional, the hub's alone: watches each route serves at once
/// relay = 2048
/// [ready]                      # optional: the quorum of each segment of the class
/// e = 4
/// [requires]                   # optional: ready cells of each class it sends to that its health needs; 1 of each when unsaid
/// model_y = 1
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Table", into = "Table")]
pub struct CellSettings {
    /// The cell's name.
    pub name: String,
    /// The layout the cell runs: one of its spec's `[layout.<name>]`
    /// tables, or `default` for a spec without them. The fleet file
    /// requires it when the spec has more than one layout. Nothing in an
    /// instance or a hub reads it: a cell's placement hash is its layout's,
    /// and a hub follows only cells whose placement hash is its own, so
    /// cells of different layouts never follow or offer to each other.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<String>,
    /// The class the cell deploys.
    pub class: String,
    /// The region the cell runs in.
    pub region: String,
    /// The parent cell: the cell of a class that sends to this one whose
    /// hub follows this cell, and whose choosers alone offer it work. The
    /// cell's record names it. The fleet file requires it for a cell of a
    /// class that another class sends to, and refuses it for one of a class
    /// that nothing sends to; a cell given settings without a fleet file
    /// that names none, or names another cell, is not followed by that
    /// parent's hub.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<String>,
    /// Where the cell's record is published and the other cells are found:
    /// the `directory` key, with the `parent_hub` key for
    /// [`DirectoryBackend::Relay`] and the `dir` key for
    /// [`DirectoryBackend::Dir`]; [`DirectoryBackend::DEFAULT`] when it is
    /// not set.
    pub directory: DirectoryBackend,
    /// Usable instances each segment of the class needs for the cell to be
    /// ready; see [`CellSettings::quorum`].
    pub ready: BTreeMap<String, u32>,
    /// How many ready cells of each class the cell's class sends to its
    /// hub must follow for the cell health endpoint to answer that the
    /// cell is healthy. Empty when unsaid, which requires one of each
    /// class the cell's class sends to ([`CellSettings::requirements`]).
    /// Only the cell's hub reads it.
    pub requires: BTreeMap<String, u32>,
    /// The address, `host:port`, at which other cells' hubs reach this
    /// cell's hub: the hub address of the cell's record. Only the cell's
    /// hub reads it, and it needs it to publish the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub: Option<String>,
    /// The load summary's period (`summary_ms`): how often each entry
    /// instance of the cell reports its admission to the cell's hub, and
    /// how often the hub summarizes the reports. The hub and its entry
    /// instances read the same setting, so that a report arrives once a
    /// summary.
    pub summary: Duration,
    /// How the cell's hub paces its judgment, its record, and its watches,
    /// and how many watches it serves at once. Only the cell's hub reads
    /// it.
    pub timing: HubTiming,
}

/// How a cell's hub paces its judgment, its record, and its watches, and
/// how many of its watches it serves at once: the `[cell]` keys
/// `hold_ms`, `heartbeat_ms`, `lease_ms`, `watch_buffer`, and
/// `[watch_caps]`. Each key that is not set takes its default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubTiming {
    /// How long every segment's quorum must hold before the cell is
    /// `ready` (`hold_ms`).
    pub hold: Duration,
    /// How often the cell's record is published while its state does not
    /// change (`heartbeat_ms`).
    pub heartbeat: Duration,
    /// How long a reader keeps the record without seeing it published
    /// again (`lease_ms`): longer than the heartbeat, or the record would
    /// lapse between two publishes.
    pub lease: Duration,
    /// How many events a watcher of the cell, or of what its hub follows,
    /// may fall behind by before its watch ends (`watch_buffer`): at least
    /// this many, rounded up to a power of two.
    pub watch_buffer: usize,
    /// How many watches each of the hub's watch routes serves at once
    /// (`[watch_caps]`).
    pub watch_caps: WatchCaps,
}

impl HubTiming {
    /// How long the quorum holds before the cell is `ready` when `hold_ms`
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

    /// The largest watch buffer, 65536 events: 64 times the default. The
    /// hub keeps a buffer for the cell and one for each cell it follows,
    /// and each takes its slots when it is made, so a buffer far past
    /// what a slow watcher needs costs memory for nothing.
    pub const MAX_WATCH_BUFFER: usize = 65536;

    /// The shortest cadence a cell's hub takes from its parent's answer
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
            watch_caps: WatchCaps::default(),
        }
    }
}

/// How many watches each watch route of a cell's hub serves at once. A
/// watch asked for beyond its route's cap is refused with 503 and a
/// `Retry-After`, before any snapshot is built or any subscription taken
/// for it; a watch that closes frees its place at once. The watches a hub
/// gives in process are not capped.
///
/// Each default is sized for a large healthy cell, of some hundreds of
/// instances: at least twice what a cell of 500 holds open, so that a
/// cell whose instances all restart at once, each counted by its old
/// connection until the hub sees it close and by its new one, is still
/// served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchCaps {
    /// The cell's watch (`GET watch`), opened by each of the cell's own
    /// instances and by its parent's hub, one per follow of it.
    pub cell: usize,
    /// The relay (`GET follows`), opened by each chooser instance of the
    /// cell.
    pub relay: usize,
    /// The watch of one follow (`GET follows/{class}/{cell}`), opened by a
    /// chooser for each followed cell it reads again after an event of a
    /// kind it does not know, and held until its next watch of the relay:
    /// one per chooser and per followed cell when an event of a new kind
    /// names every follow.
    pub follow: usize,
}

impl WatchCaps {
    /// The default cap of the cell's watch: one per instance of a cell of
    /// 500, and one per follow of its parent's hub, which holds a few at
    /// most, four times over.
    pub const DEFAULT_CELL: usize = 2048;
    /// The default cap of the relay: one per chooser instance of a cell of
    /// 500, four times over.
    pub const DEFAULT_RELAY: usize = 2048;
    /// The default cap of the watches of one follow: one per chooser of a
    /// cell of 500 and per followed cell, for a parent that follows 16
    /// cells, every one read again, twice over. Each costs one cell's
    /// snapshot, and its cell's channel is shared, so it is far cheaper
    /// than a watch of the relay.
    pub const DEFAULT_FOLLOW: usize = 16384;
}

impl Default for WatchCaps {
    fn default() -> Self {
        Self {
            cell: Self::DEFAULT_CELL,
            relay: Self::DEFAULT_RELAY,
            follow: Self::DEFAULT_FOLLOW,
        }
    }
}

/// Where cell records are published and watched. Each backend's own
/// settings arrive with the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryBackend {
    /// Each cell's hub reports its record to its parent cell's hub, which
    /// holds the records of its children: [`DirectoryBackend::DEFAULT`].
    /// A cell of a class that nothing sends to reports nowhere.
    Relay {
        /// The parent cell's hub, `host:port`: where the cell's hub reports
        /// its record. A cell's hub needs it when another class sends to
        /// the cell's class, and it is refused for any other class.
        parent_hub: Option<String>,
    },
    /// One directory in the process, shared by every hub in it: for tests
    /// that run several cells in one process.
    Memory,
    /// One file per cell under `path`, a directory that every hub on one
    /// host shares, through a volume under Compose.
    Dir {
        /// The shared directory, as given: a relative path is the
        /// process's working directory's.
        path: PathBuf,
    },
}

impl DirectoryBackend {
    /// The backend when `directory` is not set: the relay, with no parent's
    /// hub.
    pub const DEFAULT: Self = Self::Relay { parent_hub: None };
}

impl Default for DirectoryBackend {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl CellSettings {
    /// The load summary's period when `summary_ms` is not set: 10 s.
    pub const DEFAULT_SUMMARY: Duration = Duration::from_secs(10);

    /// The shortest load summary period, 1 s: a report per entry instance and a
    /// summary per cell each period, which a shorter one would make a load
    /// of their own.
    pub const MIN_SUMMARY: Duration = Duration::from_secs(1);

    /// The quorum of a segment that neither the cell's `ready` nor, in
    /// the fleet file, its class's names: one usable instance.
    pub const DEFAULT_QUORUM: u32 = 1;

    /// The ready cells of each class its class sends to that a cell's
    /// health requires when `requires` is unsaid.
    pub const DEFAULT_REQUIRES: u32 = 1;

    /// The usable instances `segment` needs for the cell to be ready: its
    /// `ready` entry, else [`CellSettings::DEFAULT_QUORUM`]. The fleet file
    /// supplies the class's default when it fills in a cell's settings, so
    /// the fallback applies only to a segment that neither the cell nor
    /// its class gives a quorum.
    pub fn quorum(&self, segment: &str) -> u32 {
        self.ready
            .get(segment)
            .copied()
            .unwrap_or(Self::DEFAULT_QUORUM)
    }

    /// The ready cells of each class its class sends to in `layout` that
    /// the cell's health requires, among the children its hub follows: its
    /// `requires`, or, when that is unsaid, each class its class sends to
    /// with [`CellSettings::DEFAULT_REQUIRES`]. An explicit `requires`
    /// replaces the default whole, so a class it does not name is not
    /// required. A cell of a class that sends to none requires nothing.
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
    /// placement has classes, the cell's class is one of them, the segment
    /// is in it, every `ready` key is a segment of it, and every `requires`
    /// key is a class it sends to, with a count of at least 1. Every
    /// problem is reported, each with its key in `[cell]`; its reader adds
    /// the file or the variable.
    pub fn check(&self, layout: &Layout, segment: &str) -> Result<(), Problems> {
        refuse(self.problems(layout, Some(segment)))
    }

    /// Whether these settings fit the cell's hub in `layout`: the rule of
    /// [`CellSettings::check`] without an instance's own segment, since a
    /// hub has none. Every problem is reported.
    ///
    /// A hub also needs `parent_hub` with the `relay` backend when another
    /// class sends to its cell's class, since it reports there.
    pub fn check_hub(&self, layout: &Layout) -> Result<(), Problems> {
        let mut problems = self.problems(layout, None);
        let entered = sent_to(layout, &self.class);
        if let (DirectoryBackend::Relay { parent_hub: None }, Some(parent)) =
            (&self.directory, entered)
        {
            problems.push(Problem::at(
                Some("cell".into()),
                Some("parent_hub".into()),
                format!(
                    "missing: class `{parent}` sends to class `{}`, so with directory = \"relay\" the cell's hub reports its record to its parent's hub, at this address",
                    self.class
                ),
            ));
        }
        refuse(problems)
    }

    /// Read a `[cell]` table from `deserializer`, a TOML table or a
    /// figment value. A table that does not read is one problem, with the
    /// key it is about (`ready.e`, say) in table `cell`; its reader adds
    /// the file or the variable, which [`cell_variable`] names.
    pub fn read<'de, D>(deserializer: D) -> Result<Self, Problems>
    where
        D: serde::Deserializer<'de>,
        D::Error: std::fmt::Display,
    {
        let table: Table = serde_path_to_error::deserialize(deserializer).map_err(|error| {
            let path = error.path().clone();
            let mut problem = Problem::from_path(&path, error.into_inner().to_string());
            problem.table = Some(match problem.table.take() {
                Some(table) => format!("cell.{table}"),
                None => "cell".into(),
            });
            Problems::from(problem)
        })?;
        Ok(Self::try_from(table)?)
    }

    fn problems(&self, layout: &Layout, segment: Option<&str>) -> Vec<Problem> {
        let class = self.class.as_str();
        let at =
            |key: String, message: String| Problem::at(Some("cell".into()), Some(key), message);
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
            let reports = match (&self.directory, sent_to(layout, class)) {
                (
                    DirectoryBackend::Relay {
                        parent_hub: Some(_),
                    },
                    None,
                ) => Some(at(
                    "parent_hub".into(),
                    format!(
                        "no class sends to class `{class}`, so its cell has no parent to report to"
                    ),
                )),
                _ => None,
            };
            own.into_iter()
                .chain(strays)
                .chain(requires)
                .chain(reports)
                .collect()
        }
    }
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

/// The variable that sets `key` of a cell's settings: `GDL_CELL__READY__E`
/// for `ready.e`. A variable is matched without regard to case, so this is
/// the spelling, not the only one.
pub fn cell_variable(key: &str) -> String {
    format!("GDL_CELL__{}", key.replace('.', "__").to_ascii_uppercase())
}

/// Whether `address` is `host:port` as a hub's address must be: a host
/// name of letters, digits, `-`, and `.`, an IPv4 address, or a bracketed
/// IPv6 address, and a port from 1 to 65535. Nothing else, so that a URL
/// or a stray space is refused where it is written rather than where
/// another hub dials it. Reading [`CellSettings`] applies it; a hub
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

/// Whether `summary` can be a load summary's period: at least
/// [`CellSettings::MIN_SUMMARY`]. Reading [`CellSettings`] applies it; a
/// hub applies it again to settings made in code.
pub fn check_summary(summary: Duration) -> Result<(), String> {
    if summary < CellSettings::MIN_SUMMARY {
        return Err(format!(
            "is {}; the period is at least {} ms",
            summary.as_millis(),
            CellSettings::MIN_SUMMARY.as_millis()
        ));
    }
    Ok(())
}

/// Whether `lease` can be the lease of a cell whose hub reports its record
/// to its parent's hub (the `relay` backend): at least
/// [`HubTiming::MIN_RELAY_LEASE`], or the record could lapse at the parent
/// between two reports. Reading [`CellSettings`] applies it to the `relay`
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

/// Whether `name` can name a cell: a DNS label.
pub fn check_cell_name(name: &str) -> Result<(), String> {
    check_label("cell", name)
}

/// Whether `name` can name a region: a DNS label.
pub fn check_region_name(name: &str) -> Result<(), String> {
    check_label("region", name)
}

/// The longest cell or region name: a DNS label's.
const LABEL_LIMIT: usize = 63;

/// Whether `name` can name a cell or a region: a DNS label. A cell's name
/// becomes a host name, a URL path segment, part of a hub's service names,
/// and part of a certificate's identity, and a region's is used beside it.
/// `kind` names what the name is for in the refusal.
fn check_label(kind: &str, name: &str) -> Result<(), String> {
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

/// The `[cell]` table as written. `directory`, `parent_hub`, and `dir` are
/// keys of one choice, so that the settings read flat (`GDL_CELL__DIR`),
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
    #[serde(default)]
    directory: Backend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_hub: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dir: Option<PathBuf>,
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
    #[serde(default, skip_serializing_if = "Caps::is_empty")]
    watch_caps: Caps,
}

/// The `[watch_caps]` table as written: each cap that is set.
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Caps {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cell: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    relay: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    follow: Option<usize>,
}

impl Caps {
    fn is_empty(&self) -> bool {
        self.cell.is_none() && self.relay.is_none() && self.follow.is_none()
    }
}

fn default_summary_ms() -> u64 {
    millis(CellSettings::DEFAULT_SUMMARY)
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
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Backend::Relay => "relay",
            Backend::Memory => "memory",
            Backend::Dir => "dir",
        }
    }
}

impl TryFrom<Table> for CellSettings {
    type Error = Problem;

    fn try_from(table: Table) -> Result<Self, Problem> {
        let at = |key: &str, message: String| {
            Problem::at(Some("cell".into()), Some(key.into()), message)
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
        let timing = hub_timing(&table).map_err(|(key, why)| at(key, why))?;
        let directory = match (table.directory, table.dir) {
            (Backend::Relay, None) => DirectoryBackend::Relay {
                parent_hub: table.parent_hub,
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
        if let Some(layout) = &table.layout {
            check_layout_name(layout).map_err(|why| at("layout", why))?;
        }
        if let Some(hub) = &table.hub {
            check_hub_address(hub).map_err(|why| at("hub", why))?;
        }
        let summary = Duration::from_millis(table.summary_ms);
        check_summary(summary).map_err(|why| at("summary_ms", why))?;
        Ok(Self {
            name: table.name,
            layout: table.layout,
            class: table.class,
            region: table.region,
            home: table.home,
            directory,
            ready: table.ready,
            requires: table.requires,
            hub: table.hub,
            summary,
            timing,
        })
    }
}

/// The hub's timing that `table` sets, each key it leaves out at its
/// default, or the key that is refused and why. The lease is checked
/// against the heartbeat as the hub will run them, defaults included, and
/// a refusal names the one of the two that was set, the lease when both
/// were.
fn hub_timing(table: &Table) -> Result<HubTiming, (&'static str, String)> {
    let at_least_one = |key: &'static str, value: u64| {
        if value == 0 {
            Err((key, "is 0; it is at least 1".to_string()))
        } else {
            Ok(())
        }
    };
    let defaults = HubTiming::default();
    let millis_or =
        |value: Option<u64>, default: Duration| value.map_or(default, Duration::from_millis);
    if let Some(heartbeat) = table.heartbeat_ms {
        at_least_one("heartbeat_ms", heartbeat)?;
    }
    if let Some(lease) = table.lease_ms {
        at_least_one("lease_ms", lease)?;
    }
    let heartbeat = millis_or(table.heartbeat_ms, defaults.heartbeat);
    let lease = millis_or(table.lease_ms, defaults.lease);
    if lease <= heartbeat {
        let (heartbeat_ms, lease_ms) = (millis(heartbeat), millis(lease));
        return Err(if table.lease_ms.is_some() {
            (
                "lease_ms",
                format!(
                    "is {lease_ms}; it must be longer than the heartbeat, {heartbeat_ms} ms, or readers let the record lapse between two publishes"
                ),
            )
        } else {
            (
                "heartbeat_ms",
                format!(
                    "is {heartbeat_ms}; it must be shorter than the lease, {lease_ms} ms by default: set `lease_ms` longer too"
                ),
            )
        });
    }
    let watch_buffer = table.watch_buffer.unwrap_or(defaults.watch_buffer);
    if watch_buffer == 0 {
        return Err(("watch_buffer", "is 0; it is at least 1".into()));
    }
    if watch_buffer > HubTiming::MAX_WATCH_BUFFER {
        return Err((
            "watch_buffer",
            format!(
                "is {watch_buffer}; it is at most {}",
                HubTiming::MAX_WATCH_BUFFER
            ),
        ));
    }
    let cap = |key: &'static str, value: Option<usize>, default: usize| match value {
        Some(0) => Err((
            key,
            "is 0, which refuses every watch; it is at least 1".to_string(),
        )),
        Some(cap) => Ok(cap),
        None => Ok(default),
    };
    let caps = &table.watch_caps;
    Ok(HubTiming {
        hold: millis_or(table.hold_ms, defaults.hold),
        heartbeat,
        lease,
        watch_buffer,
        watch_caps: WatchCaps {
            cell: cap("watch_caps.cell", caps.cell, defaults.watch_caps.cell)?,
            relay: cap("watch_caps.relay", caps.relay, defaults.watch_caps.relay)?,
            follow: cap("watch_caps.follow", caps.follow, defaults.watch_caps.follow)?,
        },
    })
}

impl From<CellSettings> for Table {
    fn from(settings: CellSettings) -> Self {
        let (directory, parent_hub, dir) = match settings.directory {
            DirectoryBackend::Relay { parent_hub } => (Backend::Relay, parent_hub, None),
            DirectoryBackend::Memory => (Backend::Memory, None, None),
            DirectoryBackend::Dir { path } => (Backend::Dir, None, Some(path)),
        };
        Self {
            name: settings.name,
            layout: settings.layout,
            class: settings.class,
            region: settings.region,
            home: settings.home,
            directory,
            parent_hub,
            dir,
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
            watch_caps: Caps {
                cell: set(settings.timing.watch_caps.cell, WatchCaps::DEFAULT_CELL),
                relay: set(settings.timing.watch_caps.relay, WatchCaps::DEFAULT_RELAY),
                follow: set(settings.timing.watch_caps.follow, WatchCaps::DEFAULT_FOLLOW),
            },
        }
    }
}

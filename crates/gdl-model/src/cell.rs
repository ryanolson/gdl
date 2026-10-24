use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{Layout, Problem, Problems};

/// A cell's own settings: the `[cell]` table of a segment's settings
/// (`GDL_CELL__NAME`, `GDL_CELL__READY__<SEGMENT>`, and so on), which tells
/// a process which cell it belongs to. The fleet file fills them in for
/// each cell ([`crate::Fleet::cell`]).
///
/// ```toml
/// name = "model-x-01-us-east-1"
/// class = "model_x"
/// region = "us-east-1"
/// home = "global-us-east-1"    # the parent cell, for a class that another sends to
/// directory = "relay"          # optional: "relay" (the default), "memory", or "dir"
/// # dir = "/var/lib/gdl/cells" # with "dir", and only with it: the shared directory
/// hub = "model-x-01-hub:1337"  # optional: where other cells' hubs reach this cell's hub
/// summary_ms = 10000           # optional: the load summary's period, at least 1000
/// [ready]                      # optional: the quorum of each segment of the class
/// e = 4
/// [requires]                   # optional: ready cells of each class it sends to that its health needs
/// model_y = 1
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Table", into = "Table")]
pub struct CellSettings {
    /// The cell's name.
    pub name: String,
    /// The class the cell deploys.
    pub class: String,
    /// The region the cell runs in.
    pub region: String,
    /// The parent cell: the cell of a class that sends to this one that
    /// follows this cell and offers it work. The fleet file requires it
    /// for a cell of a class that another class sends to, and refuses it
    /// for one of a class that nothing sends to. Nothing reads it yet: a
    /// cell's hub follows the cells of its region that its class sends to,
    /// not its children.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<String>,
    /// Where the cell's record is published and the other cells are found:
    /// the `directory` key, with the `dir` key for [`DirectoryBackend::Dir`];
    /// [`DirectoryBackend::DEFAULT`] when it is not set.
    pub directory: DirectoryBackend,
    /// Usable instances each segment of the class needs for the cell to be
    /// ready; see [`CellSettings::quorum`].
    pub ready: BTreeMap<String, u32>,
    /// How many ready cells of each class the cell's class sends to its
    /// hub must follow for the cell health endpoint to answer that the
    /// cell is healthy. Only the cell's hub reads it.
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
}

/// Where cell records are published and watched. Each backend's own
/// settings arrive with the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryBackend {
    /// Each cell reports to the global cells of its region, and the global
    /// cells relay what they receive to each other:
    /// [`DirectoryBackend::DEFAULT`].
    Relay,
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
    /// The backend when `directory` is not set: the relay.
    pub const DEFAULT: Self = Self::Relay;
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
    pub fn check_hub(&self, layout: &Layout) -> Result<(), Problems> {
        refuse(self.problems(layout, None))
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
            let sent: BTreeSet<&str> = layout
                .boundaries()
                .iter()
                .filter(|boundary| boundary.from == class)
                .map(|boundary| boundary.into.as_str())
                .collect();
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
            own.into_iter().chain(strays).chain(requires).collect()
        }
    }
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

/// Which of `variables`, the names of the variables that are set, set
/// `key` of a cell's settings: the one that names `key` itself, else the
/// one that names the table holding it, so that `GDL_CELL__READY` is
/// named for `ready.e` when it set the whole table. Matched without regard
/// to case, as the readers match them. `None` when none did: a key that is
/// missing is [`cell_variable`]'s to set.
pub fn cell_variable_of<'a>(
    key: &str,
    variables: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    const PREFIX: &str = "GDL_CELL__";
    variables
        .into_iter()
        .filter_map(|variable| {
            let rest = variable
                .get(..PREFIX.len())
                .filter(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
                .map(|_| &variable[PREFIX.len()..])?;
            let path = rest.to_ascii_lowercase().replace("__", ".");
            let holds = key == path || key.strip_prefix(&path)?.starts_with('.');
            holds.then_some((path.len(), variable))
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, variable)| variable)
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

/// The `[cell]` table as written. `directory` and `dir` are two keys of
/// one choice, so that the settings read flat (`GDL_CELL__DIR`), and they
/// are joined here, where a path given to a backend that reads none can be
/// refused rather than left unread.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    name: String,
    class: String,
    region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    home: Option<String>,
    #[serde(default)]
    directory: Backend,
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
}

fn default_summary_ms() -> u64 {
    millis(CellSettings::DEFAULT_SUMMARY)
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
        let directory = match (table.directory, table.dir) {
            (Backend::Relay, None) => DirectoryBackend::Relay,
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
        if let Some(hub) = &table.hub {
            check_hub_address(hub).map_err(|why| at("hub", why))?;
        }
        let summary = Duration::from_millis(table.summary_ms);
        check_summary(summary).map_err(|why| at("summary_ms", why))?;
        Ok(Self {
            name: table.name,
            class: table.class,
            region: table.region,
            home: table.home,
            directory,
            ready: table.ready,
            requires: table.requires,
            hub: table.hub,
            summary,
        })
    }
}

impl From<CellSettings> for Table {
    fn from(settings: CellSettings) -> Self {
        let (directory, dir) = match settings.directory {
            DirectoryBackend::Relay => (Backend::Relay, None),
            DirectoryBackend::Memory => (Backend::Memory, None),
            DirectoryBackend::Dir { path } => (Backend::Dir, Some(path)),
        };
        Self {
            name: settings.name,
            class: settings.class,
            region: settings.region,
            home: settings.home,
            directory,
            dir,
            ready: settings.ready,
            requires: settings.requires,
            hub: settings.hub,
            summary_ms: millis(settings.summary),
        }
    }
}

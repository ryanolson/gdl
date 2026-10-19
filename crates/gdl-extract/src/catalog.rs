//! The working catalogue: one file per crate version, written once, and
//! an index built in memory when the directory is loaded.
//!
//! ```text
//! <catalogue>/<name>/<version>.toml    an Entry: the Offer, and where it came from
//! ```
//!
//! A crate version never changes, so an entry is never rewritten. No
//! unified file is stored: the directory is the catalogue, and loading it
//! builds the index.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::{Error, NodeEntry, Offer, Origin};

/// A loaded working catalogue: its entries and their index.
#[derive(Debug, Default)]
pub struct Catalog {
    dir: PathBuf,
    /// By `name@version`.
    entries: BTreeMap<String, Entry>,
    /// Message name to each message of that name, defined or exported.
    messages: BTreeMap<String, BTreeSet<Origin>>,
    /// Node name to each node of that name.
    nodes: BTreeMap<String, Vec<NodeRef>>,
    /// Message to each port that carries it, as its head or its item.
    ports: BTreeMap<Origin, Vec<(NodeRef, String)>>,
}

/// One crate version in the catalogue: what it offers, and its provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// When it was added: RFC 3339, UTC, to the second.
    pub added: String,
    /// The registry's checksum of the `.crate` (SHA-256, as Cargo verifies
    /// it); none for a path or git package.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    /// What it offers.
    #[serde(flatten)]
    pub offer: Offer,
}

/// A node in the catalogue: its entry, by `name@version`, and its index
/// among the entry's nodes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeRef {
    /// The entry's `name@version`.
    pub entry: String,
    /// The node's index in the entry's offer.
    pub index: usize,
}

impl Entry {
    /// The entry's key: `name@version`.
    pub fn key(&self) -> String {
        format!("{}@{}", self.offer.package, self.offer.version)
    }

    /// The entry as its file holds it.
    pub fn to_toml(&self) -> String {
        toml::to_string(self).expect("an entry is representable in TOML")
    }

    /// Read an entry's file.
    pub fn from_toml(text: &str) -> Result<Self, Error> {
        toml::from_str(text).map_err(|e| Error(e.to_string()))
    }
}

impl Catalog {
    /// Load the catalogue in `dir`: every `<name>/<version>.toml`. A
    /// directory that does not exist is an empty catalogue.
    pub fn load(dir: &Path) -> Result<Self, Error> {
        let mut catalog = Self {
            dir: dir.to_path_buf(),
            ..Self::default()
        };
        let io = |path: &Path, e: std::io::Error| Error(format!("{}: {e}", path.display()));
        let packages = match std::fs::read_dir(dir) {
            Ok(packages) => packages,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(catalog),
            Err(e) => return Err(io(dir, e)),
        };
        let mut files = Vec::new();
        for package in packages {
            let package = package.map_err(|e| io(dir, e))?.path();
            if !package.is_dir() {
                continue;
            }
            for version in std::fs::read_dir(&package).map_err(|e| io(&package, e))? {
                let file = version.map_err(|e| io(&package, e))?.path();
                if file.extension().is_some_and(|e| e == "toml") {
                    files.push(file);
                }
            }
        }
        files.sort();
        for file in files {
            let text = std::fs::read_to_string(&file).map_err(|e| io(&file, e))?;
            let entry =
                Entry::from_toml(&text).map_err(|e| Error(format!("{}: {e}", file.display())))?;
            if file != catalog.file(&entry) {
                return Err(Error(format!(
                    "{}: holds {}, which belongs in {}",
                    file.display(),
                    entry.key(),
                    catalog.file(&entry).display()
                )));
            }
            catalog.index(entry);
        }
        Ok(catalog)
    }

    /// Add `entry`, writing its file. An entry already there is kept, file
    /// and all: a crate version never changes. The entry the catalogue
    /// holds, and its file.
    pub fn add(&mut self, entry: Entry) -> Result<(&Entry, PathBuf), Error> {
        let key = entry.key();
        let file = self.file(&entry);
        if !self.entries.contains_key(&key) {
            let dir = file
                .parent()
                .expect("an entry's file is in its package's directory");
            std::fs::create_dir_all(dir).map_err(|e| Error(format!("{}: {e}", dir.display())))?;
            // Written beside, then renamed: a reader never sees half a file.
            let partial = dir.join(format!(".{}.{}", entry.offer.version, std::process::id()));
            std::fs::write(&partial, entry.to_toml())
                .and_then(|()| std::fs::rename(&partial, &file))
                .map_err(|e| Error(format!("{}: {e}", file.display())))?;
            self.index(entry);
        }
        Ok((&self.entries[&key], file))
    }

    /// Every entry, by `name@version`.
    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.values()
    }

    /// Each message named `name`, by its origin.
    pub fn messages(&self, name: &str) -> impl Iterator<Item = &Origin> {
        self.messages.get(name).into_iter().flatten()
    }

    /// Every message name the catalogue offers.
    pub fn message_names(&self) -> impl Iterator<Item = &str> {
        self.messages.keys().map(String::as_str)
    }

    /// Each node named `name`, with its entry.
    pub fn nodes(&self, name: &str) -> impl Iterator<Item = (&Entry, &NodeEntry)> {
        self.nodes
            .get(name)
            .into_iter()
            .flatten()
            .map(|r| self.node(r))
    }

    /// Each port that carries `origin`, as its head or its item, with its
    /// node and entry. Two ports connect when they carry one origin.
    pub fn ports(&self, origin: &Origin) -> impl Iterator<Item = (&Entry, &NodeEntry, &str)> {
        self.ports
            .get(origin)
            .into_iter()
            .flatten()
            .map(|(r, port)| {
                let (entry, node) = self.node(r);
                (entry, node, port.as_str())
            })
    }

    fn node(&self, r: &NodeRef) -> (&Entry, &NodeEntry) {
        let entry = &self.entries[&r.entry];
        (entry, &entry.offer.nodes[r.index])
    }

    fn file(&self, entry: &Entry) -> PathBuf {
        self.dir
            .join(&entry.offer.package)
            .join(format!("{}.toml", entry.offer.version))
    }

    fn index(&mut self, entry: Entry) {
        let key = entry.key();
        let offer = &entry.offer;
        for m in &offer.messages {
            let origin = Origin {
                package: offer.package.clone(),
                version: offer.version.clone(),
                source: offer.source.clone(),
                path: m.path.clone(),
            };
            self.messages
                .entry(m.name.clone())
                .or_default()
                .insert(origin);
        }
        for e in &offer.exports {
            self.messages
                .entry(e.name.clone())
                .or_default()
                .insert(e.origin.clone());
        }
        for (index, node) in offer.nodes.iter().enumerate() {
            let r = NodeRef {
                entry: key.clone(),
                index,
            };
            self.nodes
                .entry(node.name.clone())
                .or_default()
                .push(r.clone());
            for port in &node.ports {
                for origin in port.head.iter().chain(&port.item) {
                    let carried = self.ports.entry(origin.clone()).or_default();
                    if !carried.iter().any(|(n, p)| *n == r && *p == port.port) {
                        carried.push((r.clone(), port.port.clone()));
                    }
                }
            }
        }
        self.entries.insert(key, entry);
    }
}

impl std::fmt::Display for Catalog {
    /// The index as text: each entry with its provenance, each message
    /// name with its messages, and each node with the messages at its
    /// ports, each message by origin.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let origin = |o: &Origin| format!("{} ({} {})", o.path, o.package, o.version);
        writeln!(f, "entries:")?;
        for entry in self.entries.values() {
            writeln!(
                f,
                "  {}  {}  added {}  checksum {}",
                entry.key(),
                entry.offer.source,
                entry.added,
                entry.checksum.as_deref().unwrap_or("-")
            )?;
        }
        writeln!(f, "messages:")?;
        for (name, origins) in &self.messages {
            let origins: Vec<String> = origins.iter().map(origin).collect();
            writeln!(f, "  {name}: {}", origins.join(", "))?;
        }
        writeln!(f, "nodes:")?;
        for refs in self.nodes.values() {
            for r in refs {
                let (entry, node) = self.node(r);
                writeln!(
                    f,
                    "  {}: {} {} ({})",
                    node.name,
                    node.role,
                    node.ty,
                    entry.key()
                )?;
                for port in &node.ports {
                    let head = port.head.as_ref().map_or("()".to_string(), origin);
                    match &port.item {
                        None => writeln!(f, "    {}: {head}", port.port)?,
                        Some(item) => {
                            writeln!(f, "    {}: {head} + stream {}", port.port, origin(item))?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// `time` as RFC 3339 in UTC, to the second: `2026-10-18T21:04:05Z`.
pub fn rfc3339(time: SystemTime) -> String {
    let seconds = time.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    let (year, month, day) = civil(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// The proleptic Gregorian date `days` after 1970-01-01 (Howard Hinnant's
/// `civil_from_days`).
fn civil(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn times_are_written_in_utc() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        // A leap day, and the end of a day; each checked with `date -u -d @<seconds>`.
        let leap = UNIX_EPOCH + Duration::from_secs(951_868_799);
        assert_eq!(rfc3339(leap), "2000-02-29T23:59:59Z");
        let now = UNIX_EPOCH + Duration::from_secs(1_792_357_445);
        assert_eq!(rfc3339(now), "2026-10-18T21:04:05Z");
    }
}

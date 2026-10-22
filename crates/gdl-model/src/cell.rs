use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Error, Layout};

/// A cell's own settings: the `[cell]` table of a segment's settings
/// (`GDL_CELL__NAME`, `GDL_CELL__READY__<SEGMENT>`, and so on), which tells
/// a process which cell it belongs to. The fleet file fills them in for
/// each cell ([`crate::Fleet::cell`]).
///
/// ```toml
/// name = "model-x-01-us-east-1"
/// class = "model_x"
/// region = "us-east-1"
/// home = "global-us-east-1"    # optional: the cell of a parent class the policy prefers
/// directory = "relay"          # optional: "relay" (the default) or "static"
/// seeds = ["hub-a:1337"]       # optional: hub addresses to join the relay mesh from
/// [ready]                      # optional: the quorum of each segment of the class
/// e = 4
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellSettings {
    /// The cell's name.
    pub name: String,
    /// The class the cell deploys.
    pub class: String,
    /// The region the cell runs in.
    pub region: String,
    /// The cell of a parent class that the cell policy prefers for this
    /// one. A preference, not a dependency of discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<String>,
    /// Where the cell's record is published and the other cells are found.
    #[serde(default)]
    pub directory: DirectoryBackend,
    /// Hub addresses, `host:port`, to join the relay mesh from; kept as
    /// given.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seeds: Vec<String>,
    /// Usable instances each segment of the class needs for the cell to be
    /// ready; see [`CellSettings::quorum`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ready: BTreeMap<String, u32>,
}

/// Where cell records are published and watched. Each backend's own
/// settings arrive with the backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectoryBackend {
    /// Each cell reports to the global cells of its region, and the global
    /// cells relay what they receive to each other.
    #[default]
    Relay,
    /// The cells are fixed by configuration.
    Static,
}

impl CellSettings {
    /// The usable instances `segment` needs for the cell to be ready: its
    /// `ready` entry, else 1. The fleet file supplies the class's default
    /// when it fills in a cell's settings, so the fallback applies only to
    /// a segment that neither the cell nor its class gives a quorum.
    pub fn quorum(&self, segment: &str) -> u32 {
        self.ready.get(segment).copied().unwrap_or(1)
    }

    /// Whether these settings fit an instance of `segment` in `layout`: the
    /// placement has classes, the cell's class is one of them, the segment
    /// is in it, and every `ready` key is a segment of it. Every problem is
    /// reported.
    pub fn check(&self, layout: &Layout, segment: &str) -> Result<(), Error> {
        let class = self.class.as_str();
        let classes = layout.class_segments();
        let problems: Vec<String> = if classes.is_empty() {
            vec![format!(
                "[cell] class = {class:?}, but the placement has no classes"
            )]
        } else if !classes.contains_key(class) {
            vec![format!(
                "[cell] class: the placement has no class `{class}`"
            )]
        } else {
            let own = match layout.class_of(segment) {
                Some(own) if own == class => None,
                Some(own) => Some(format!(
                    "[cell] class: this instance's segment `{segment}` is in class `{own}`, not `{class}`"
                )),
                None => Some(format!(
                    "[cell] class: this instance's segment `{segment}` is not in class `{class}`"
                )),
            };
            let strays = self
                .ready
                .keys()
                .filter(|key| layout.class_of(key) != Some(class))
                .map(|key| format!("[cell] ready: `{key}` is not a segment of class `{class}`"));
            own.into_iter().chain(strays).collect()
        };
        if problems.is_empty() {
            Ok(())
        } else {
            Err(Error::new(problems.join("; ")))
        }
    }
}

/// The longest cell or region name: a DNS label's.
const LABEL_LIMIT: usize = 63;

/// Whether `name` can name a cell or a region: a DNS label. A cell's name
/// becomes a host name, a URL path segment, part of a hub's service names,
/// and part of a certificate's identity, and a region's is used beside it.
/// `kind` names what the name is for in the refusal.
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

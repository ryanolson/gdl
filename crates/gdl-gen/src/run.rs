//! `cargo gdl run`: build a spec's segments and run them on this machine.

use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use crate::Offers;
#[cfg(unix)]
use crate::deployment::{merge_config, stdin_source};
use crate::{Build, Error, Spec};

/// Build `spec`'s segments as [`build`](crate::build) does, then run them
/// as processes on this machine until the source's segment ends, and stop
/// the others.
///
/// Every segment gets settings of its own in a new temporary directory: a
/// listener on a free loopback port, one roster directory that they share,
/// and its `[settings.<segment>]` table. A deployment's
/// `[deployment.segment.<name>.config]` is for its containers, and is not
/// read. The segment that hosts the source reads JSON requests from this process's stdin and
/// writes the answers to its stdout. The others write to its stderr. When
/// the source's segment ends, at the end of its input, each other segment
/// gets one SIGTERM and drains. A SIGINT, such as Ctrl-C at a terminal,
/// reaches every segment in the terminal's process group, so `run` sends
/// none of its own and waits for them to drain. A SIGTERM to `run` goes
/// once to each segment. A segment that ends before the source's segment
/// does stops the others, and `run` fails, naming it.
///
/// `run` catches SIGINT and SIGTERM for the life of the process. A stop
/// while it builds stops the build, and `run` fails. On Linux, each segment
/// gets a SIGTERM when `run` dies, even by SIGKILL. Unix only.
///
/// Errors: a problem of the spec; a spec of several layouts; a source that
/// is pinned to a frontend or cannot read stdin; a failed build; a stop
/// while it builds; a segment that failed or was killed.
#[cfg(unix)]
pub fn run(spec: &Spec, target: &Path, how: &Build) -> Result<(), Error> {
    // First: a stop that came before the handlers would end `run` at once,
    // and leave its build running and its settings directory behind.
    let stops = unix::Stops::caught()?;
    let dir = tempfile::Builder::new()
        .prefix("gdl-run-")
        .tempdir()
        .map_err(|e| Error::new(format!("a directory for the segments' settings: {e}")))?;
    let plan = unix::preparing(&stops, || prepare(spec, target, how, dir.path()))?;
    unix::supervise(&plan, &stops)
}

/// [`run`] on a system that is not Unix: refused.
#[cfg(not(unix))]
pub fn run(_: &Spec, _: &Path, _: &Build) -> Result<(), Error> {
    Err(Error::new("`run` needs Unix"))
}

/// What `run` starts: the segment that hosts the source, and the others.
#[cfg(unix)]
struct Plan {
    front: Segment,
    others: Vec<Segment>,
}

/// One segment as `run` starts it.
#[cfg(unix)]
struct Segment {
    name: String,
    binary: PathBuf,
    settings: PathBuf,
    /// How long it may take to stop: its drain bound and a margin.
    grace: Duration,
}

/// Check `spec`, build it, and write each segment's settings in `dir`.
#[cfg(unix)]
fn prepare(spec: &Spec, target: &Path, how: &Build, dir: &Path) -> Result<Plan, Error> {
    let laid = spec.only_layout("`run`")?;
    // Before the build: a spec `run` cannot serve is refused at once.
    let checked = crate::check(spec, target, Offers::Read)?;
    let source = match stdin_source(spec, &checked.manifest) {
        Ok(Some(source)) => source,
        Ok(None) => {
            return Err(Error::new(
                "`run` serves JSON requests on stdin, and this graph's source is pinned to a frontend: build the segments and run them yourself",
            ));
        }
        Err(_) => {
            return Err(Error::new(
                "`run` needs a source that reads JSON requests on stdin: the graph's only source, whose request is one value, and whose request and item cross segments",
            ));
        }
    };
    let binaries = crate::build(spec, target, how)?;
    let roster = dir.join("roster");
    std::fs::create_dir(&roster).map_err(|e| Error::new(format!("the roster directory: {e}")))?;
    let mut front = None;
    let mut others = Vec::new();
    for (name, nodes) in laid.placement.segments() {
        let package = laid.package(&checked.manifest.name, name);
        let file = format!("{package}{}", std::env::consts::EXE_SUFFIX);
        let binary = binaries
            .iter()
            .find(|path| path.file_name().is_some_and(|f| f == file.as_str()))
            .ok_or_else(|| Error::new(format!("the build made no binary `{file}`")))?
            .clone();
        let table = settings(&roster, spec.settings.get(name))?;
        let settings = dir.join(format!("{name}.toml"));
        let text = toml::to_string_pretty(&table).map_err(|e| Error::new(e.to_string()))?;
        std::fs::write(&settings, text)
            .map_err(|e| Error::new(format!("{}: {e}", settings.display())))?;
        let segment = Segment {
            name: name.to_string(),
            binary,
            settings,
            grace: Duration::from_secs(gdl_k8s::stop_grace(gdl_k8s::drain_bound(&table))),
        };
        if nodes.contains(&source) {
            front = Some(segment);
        } else {
            others.push(segment);
        }
    }
    let front = front.ok_or_else(|| Error::new(format!("no segment hosts source `{source}`")))?;
    Ok(Plan { front, others })
}

/// A segment's settings under `run`: a listener on a free loopback port,
/// the shared roster directory `roster`, and the spec's `config` table for
/// it.
#[cfg(unix)]
fn settings(roster: &Path, config: Option<&toml::Table>) -> Result<toml::Table, Error> {
    let roster = roster.to_str().ok_or_else(|| {
        Error::new(format!(
            "the roster directory {} is not UTF-8",
            roster.display()
        ))
    })?;
    let mut table = toml::Table::new();
    table.insert("listen".into(), "127.0.0.1:0".into());
    table.insert("roster".into(), roster.into());
    if let Some(config) = config {
        merge_config(&mut table, config)?;
    }
    Ok(table)
}

#[cfg(unix)]
mod unix;

#[cfg(all(test, unix))]
mod tests;

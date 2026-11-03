//! `cargo gdl run`: build a spec's segments and run them on this machine.

#[cfg(unix)]
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::process::Command;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use serde::{Deserialize, Serialize};

#[cfg(unix)]
use crate::Offers;
#[cfg(unix)]
use crate::deployment::{merge_config, stdin_source};
use crate::{Build, Error, Spec};

/// The file in the settings directory that a preparation writes its plan
/// to: internal, and read only by the binary that wrote it.
#[cfg(unix)]
const PLAN: &str = "plan.json";

/// Prepare a spec's segments with `prepare`, then run them as processes on
/// this machine until the source's segment ends, and stop the others.
///
/// `prepare` is this binary's `run` command, which `run` gives
/// `--prepare-into <dir>`: it runs [`prepare_run`] there, in a process
/// group of its own. A stop while it runs sends SIGTERM to that group,
/// and SIGKILL after a short bound, so that no Cargo or compiler outlives
/// `run`.
///
/// Every segment gets settings of its own in a new temporary directory: a
/// listener on a free loopback port, one roster directory that they share,
/// and its `[settings.<segment>]` table. A deployment's
/// `[deployment.segment.<name>.config]` is for its containers, and is not
/// read. The segment that hosts the source reads JSON requests from this
/// process's stdin and writes the answers to its stdout. The others write
/// to its stderr. When the source's segment ends, at the end of its input,
/// each other segment gets one SIGTERM and drains. A SIGINT, such as
/// Ctrl-C at a terminal, reaches every segment in the terminal's process
/// group, so `run` sends none of its own and waits for them to drain. A
/// SIGTERM to `run` goes once to each segment. A segment that ends before
/// the source's segment does stops the others, and `run` fails, naming it.
///
/// `run` catches SIGINT and SIGTERM for the life of the process. On Linux,
/// each segment gets a SIGTERM when `run` dies, even by SIGKILL. Unix only.
///
/// Errors: a preparation that failed or was stopped; a segment that failed
/// or was killed.
#[cfg(unix)]
pub fn run(mut prepare: Command) -> Result<(), Error> {
    // First: a stop that came before the handlers would end `run` at once,
    // and leave its preparation running and its settings directory behind.
    let stops = unix::Stops::caught()?;
    let dir = tempfile::Builder::new()
        .prefix("gdl-run-")
        .tempdir()
        .map_err(|e| Error::new(format!("a directory for the segments' settings: {e}")))?;
    prepare.arg("--prepare-into").arg(dir.path());
    if let Err(unprepared) = unix::preparing(&stops, prepare) {
        // The settings directory goes before any warning: a write to a full
        // stderr blocks, and must not keep `run` from its own cleanup.
        let _ = dir.close();
        if let Some(warning) = unprepared.warning {
            eprintln!("cargo gdl: {warning}");
        }
        return Err(unprepared.error);
    }
    let file = dir.path().join(PLAN);
    let text = std::fs::read_to_string(&file)
        .map_err(|e| Error::new(format!("{}: {e}", file.display())))?;
    let plan: Plan =
        serde_json::from_str(&text).map_err(|e| Error::new(format!("{}: {e}", file.display())))?;
    unix::supervise(&plan, &stops)
}

/// [`run`] on a system that is not Unix: refused.
#[cfg(not(unix))]
pub fn run(_: Command) -> Result<(), Error> {
    Err(Error::new("`run` needs Unix"))
}

/// What [`run`]'s preparation does, in a process of its own: check
/// `spec`, build its segments as [`build`](crate::build) does, and write
/// each segment's settings and the plan for `run` in `dir`.
///
/// Errors: a problem of the spec; a spec of several layouts; a source that
/// is pinned to a frontend or cannot read stdin; a failed build.
#[cfg(unix)]
pub fn prepare_run(spec: &Spec, target: &Path, how: &Build, dir: &Path) -> Result<(), Error> {
    let plan = prepare(spec, target, how, dir)?;
    let file = dir.join(PLAN);
    let text = serde_json::to_string(&plan).map_err(|e| Error::new(e.to_string()))?;
    std::fs::write(&file, text).map_err(|e| Error::new(format!("{}: {e}", file.display())))
}

/// [`prepare_run`] on a system that is not Unix: refused.
#[cfg(not(unix))]
pub fn prepare_run(
    _: &Spec,
    _: &std::path::Path,
    _: &Build,
    _: &std::path::Path,
) -> Result<(), Error> {
    Err(Error::new("`run` needs Unix"))
}

/// What `run` starts: the segment that hosts the source, and the others.
#[cfg(unix)]
#[derive(Serialize, Deserialize)]
struct Plan {
    front: Segment,
    others: Vec<Segment>,
}

/// One segment as `run` starts it.
#[cfg(unix)]
#[derive(Serialize, Deserialize)]
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

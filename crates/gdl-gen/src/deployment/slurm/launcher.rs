//! The launcher of a pool's job, as its recipe runs it: one
//! `launch_<segment>` service for each segment of the pool's class, on
//! each of its spare nodes, that runs the segment's own binary as
//! `--launch` (`gdl_runtime::launched`). It starts the segment as its own
//! child when `cargo gdl join --in-job` asks, and stops it when `leave` or
//! `down` asks, through records in the pool's directory
//! ([`gdl_model::launch`]). srt-slurm runs every `generic` service in a
//! container, which has no Slurm client, so no service can start a joiner
//! as a step of its own; a child of a service runs in the step srt-slurm
//! started, and no process outside the job holds it. See
//! `docs/src/guides/slurm.md`.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

/// The service that holds a pool's spare nodes, which each launcher rides
/// on.
pub(crate) const SPARE: &str = "spare";

/// The start of each launcher's service name: `launch_<segment>`.
pub(crate) const LAUNCH: &str = "launch_";

/// The launcher, as a service of a pool's recipe: the service `segment`
/// runs as in that recipe, with its image, environment, preamble, and
/// every field that shapes srt-slurm's `srun` (`cpus_per_task`,
/// `cpu_bind`, `srun_options`, `placement.per`), so that srt-slurm starts
/// its step as it starts the segment's, and a joiner, its child, has what
/// an instance of the segment has. It rides on the spare nodes, one
/// instance on each, and has no readiness of its own: srt-slurm moves on
/// once it starts. Its command is the segment's binary as the launcher,
/// with the segment's own command after it: the joiner's.
pub(crate) fn service(segment: &Value, root: &str) -> Value {
    let mut service = segment.clone();
    let name = segment["name"].as_str().unwrap_or_default().to_string();
    let object = service.as_object_mut().expect("a service is an object");
    for owned in ["nodes", "readiness", "terminal"] {
        object.remove(owned);
    }
    object.insert("name".into(), format!("{LAUNCH}{name}").into());
    let mut placement = object
        .remove("placement")
        .and_then(|placement| placement.as_object().cloned())
        .unwrap_or_default();
    placement.remove("node");
    placement.insert("pool".into(), SPARE.into());
    object.insert("placement".into(), placement.into());
    let own = segment["command"].as_array().cloned().unwrap_or_default();
    let binary = own.first().cloned().unwrap_or_default();
    let mut command = vec![
        binary,
        json!("--launch"),
        json!("--root"),
        json!(root),
        json!("--node"),
        json!("{node}"),
        json!("--segment"),
        json!(name),
        json!("--"),
    ];
    command.extend(own);
    object.insert("command".into(), command.into());
    service
}

/// Where the launcher of `segment` on node `node` of job `job` keeps its
/// records, in `dir`, its pool's directory.
pub(crate) fn records(dir: &Path, job: &str, node: &str, segment: &str) -> PathBuf {
    dir.join(gdl_model::launch::launcher_directory(job, node, segment))
}

/// Where the joiner of launch `launch` on node `node` of job `job` writes
/// what it prints, in `dir`, its pool's directory.
pub(crate) fn log(dir: &Path, job: &str, node: &str, launch: &str) -> PathBuf {
    dir.join(format!("join.{job}.{node}.{launch}.log"))
}

/// Record `name` in the launcher's records at `dir`, read by name: `None`
/// when it is not there yet.
pub(crate) fn read(dir: &Path, name: &str) -> Option<String> {
    std::fs::read_to_string(dir.join(name)).ok()
}

/// Write record `name` in the launcher's records at `dir`, once, holding
/// `text`: to a name of this process's own, flushed, then linked to
/// `name`, which fails if the name is taken, so that the launcher, which
/// opens it by name, sees all of it or nothing. A record that another
/// write made first stays as it is: `Ok(false)`.
pub(crate) fn write_once(dir: &Path, name: &str, text: &str) -> std::io::Result<bool> {
    let staged = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let target = dir.join(name);
    let written = (|| {
        let mut file = std::fs::File::create(&staged)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()
    })();
    let linked = written.and_then(|()| std::fs::hard_link(&staged, &target));
    let settled = settle(&staged, &target, linked);
    let _ = std::fs::remove_file(&staged);
    settled
}

/// Whether the link of `staged` to `target` made `target`, from what the
/// link returned. An NFS `LINK` is not idempotent: when its reply is
/// lost, the client sends it again, and the server can answer `EEXIST` to
/// a link that succeeded. So on `EEXIST`, `target` is this write's own
/// when it is `staged`'s inode; otherwise another write made it first.
pub(crate) fn settle(
    staged: &Path,
    target: &Path,
    linked: std::io::Result<()>,
) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt as _;
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let theirs = std::fs::metadata(target)?;
            Ok(std::fs::metadata(staged)
                .is_ok_and(|ours| ours.ino() == theirs.ino() && ours.dev() == theirs.dev()))
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests;

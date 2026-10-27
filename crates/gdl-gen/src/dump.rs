//! Reading what only compiled code knows: a contract's manifest, read by
//! building and running a throwaway program that depends on the contract
//! and prints it.
//!
//! The manifest is produced by the `graph!` expansion, so the only faithful
//! way to read it is to compile the contract. The program depends on the
//! contract alone, so this never compiles an implementation crate.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::Error;
use crate::spec::Contract;

/// The manifest of `contract`. `seed` is the lock file the program starts
/// from ([`write_program`]): for a contract crate, the lock of its
/// workspace ([`workspace_lock`]). `target` is the cargo target directory
/// to build in; reusing one across runs keeps the build incremental.
pub fn manifest(
    contract: &Contract,
    seed: Option<&str>,
    target: &Path,
) -> Result<gdl_model::Manifest, Error> {
    let path = absolute(&contract.path)?;
    let dependencies = format!(
        "{package} = {{ path = {path} }}\n",
        package = contract.package,
        path = toml_string(&path.to_string_lossy()),
    );
    let main = format!(
        "fn main() {{\n    print!(\"{{}}\", {module}::manifest().to_toml());\n}}\n",
        module = contract.module,
    );
    // One program per contract, as for catalogues: a run that reads another
    // contract does not relink this one.
    let name = format!("gdl-manifest-{:016x}", hash(&(&dependencies, &main, seed)));
    let text = run(
        target,
        &name,
        "the contract's manifest",
        &dependencies,
        &main,
        seed,
    )?;
    gdl_model::Manifest::from_toml(&text)
        .map_err(|e| Error::new(format!("reading the manifest: {e}")))
}

/// The lock file of the workspace that holds the crate in `dir`, if it has
/// one. Cargo names the workspace, so a crate that a parent workspace
/// excludes, or that names its own root, gets the lock its own build uses.
/// `None` when Cargo cannot place the crate in a workspace: the program
/// then builds unseeded, which needs the registry index but works.
pub(crate) fn workspace_lock(dir: &Path) -> Option<String> {
    // Cargo reads `--manifest-path` from where it runs.
    let dir = absolute(dir).ok()?;
    let output = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(&dir)
        .args(["locate-project", "--workspace", "--message-format", "plain"])
        .arg("--manifest-path")
        .arg(dir.join("Cargo.toml"))
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let manifest = PathBuf::from(String::from_utf8(output.stdout).ok()?.trim_end());
    std::fs::read_to_string(manifest.parent()?.join("Cargo.lock")).ok()
}

/// A name for generated content: the same content, the same name, within
/// one build of the generator.
pub(crate) fn hash(value: &impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Write the program `name` with these `[dependencies]` lines and this
/// `main.rs`, then build and run it, and return what it printed. `what` the
/// program prints names it in a failure. `seed` is the lock file the
/// program starts from ([`write_program`]); `name` must change with it.
pub(crate) fn run(
    target: &Path,
    name: &str,
    what: &str,
    dependencies: &str,
    main: &str,
    seed: Option<&str>,
) -> Result<String, Error> {
    // Every run of `name` in `target` builds the same program at the same
    // path. Holding the directory's lock from writing the program until it
    // has run keeps another run from replacing it in between, which would
    // make this one print the other's output.
    let _lock = lock(target)?;
    let work = target.join("gdl-gen").join(name);
    write_program(&work, name, dependencies, main, seed)?;
    // Built, then run, so that a failure says which of the two it was.
    // Cargo runs it: it knows where the target and the profile put the
    // program, and what it needs at run time (a dynamically linked standard
    // library, for one).
    cargo(&work, target, &["build", "--quiet"])
        .map_err(|e| Error::new(format!("building {what} failed:\n{e}")))?;
    cargo(&work, target, &["run", "--quiet"])
        .map_err(|e| Error::new(format!("reading {what} failed:\n{e}")))
}

/// Write the program's files into `work`.
///
/// `seed` is the text of a lock file, when the program's dependencies come
/// from a workspace that has one. Cargo then builds them at the versions
/// that workspace builds, and reads no registry index for crates the lock
/// pins: with no lock, Cargo resolves the program anew and reads crates.io
/// even when every crate is in its cache. It is written only when the
/// program has no lock yet: Cargo adds the program's own package to it, so
/// a seed written again on every run would differ from what Cargo left.
fn write_program(
    work: &Path,
    name: &str,
    dependencies: &str,
    main: &str,
    seed: Option<&str>,
) -> Result<(), Error> {
    std::fs::create_dir_all(work.join("src")).map_err(|e| io(work, e))?;
    let manifest = format!(
        "[package]\n\
         name = \"{name}\"\n\
         version = \"0.0.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\
         \n\
         # Its own workspace, wherever the target directory is.\n\
         [workspace]\n\
         \n\
         [dependencies]\n\
         {dependencies}"
    );
    write(&work.join("Cargo.toml"), &manifest)?;
    if let Some(seed) = seed
        && !work.join("Cargo.lock").exists()
    {
        write(&work.join("Cargo.lock"), seed)?;
    }
    write(&work.join("src/main.rs"), main)
}

/// Run `cargo <args>` on the program in `work`, and return its output.
fn cargo(work: &Path, target: &Path, args: &[&str]) -> Result<String, Error> {
    let output = command(work, target, args)?
        .output()
        .map_err(|e| Error::new(format!("running cargo: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(String::from_utf8_lossy(&output.stderr)));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| Error::new(format!("cargo's output is not UTF-8: {e}")))
}

/// `cargo <args>` on the program in `work`, run in `work`: Cargo reads its
/// configuration from where it runs, and the program must see the
/// configuration its dependencies were fetched under, as `cargo metadata`
/// does in `gdl-extract`. Paths are absolute, so they do not depend on it.
fn command(work: &Path, target: &Path, args: &[&str]) -> Result<Command, Error> {
    let work = std::path::absolute(work).map_err(|e| io(work, e))?;
    let target = std::path::absolute(target).map_err(|e| io(target, e))?;
    let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(&work)
        .args(args)
        .arg("--manifest-path")
        .arg(work.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target);
    Ok(command)
}

/// Write `text` unless the file already holds it: a rewrite with the same
/// text still changes the file's modification time, and cargo would rebuild
/// and relink the program for it. The text goes to a file of this process
/// and is renamed into place, so a reader never sees a file half written,
/// even when another thread or process writes the same path.
pub(crate) fn write(path: &Path, text: &str) -> Result<(), Error> {
    if std::fs::read_to_string(path).is_ok_and(|existing| existing == text) {
        return Ok(());
    }
    // Unique to this write: threads of one process write one path too.
    static WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let write = WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temporary = path.with_extension(format!("tmp-{}-{write}", std::process::id()));
    std::fs::write(&temporary, text).map_err(|e| io(&temporary, e))?;
    std::fs::rename(&temporary, path).map_err(|e| io(path, e))
}

pub(crate) fn absolute(path: &Path) -> Result<PathBuf, Error> {
    crate::paths::resolve(path).map_err(|e| io(path, e))
}

pub(crate) fn toml_string(text: &str) -> String {
    toml::Value::String(text.to_string()).to_string()
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::new(format!("{}: {error}", path.display()))
}

/// Exclusive use of the dump program in `target`, until dropped.
fn lock(target: &Path) -> Result<std::fs::File, Error> {
    let dir = target.join("gdl-gen");
    std::fs::create_dir_all(&dir).map_err(|e| io(&dir, e))?;
    let path = dir.join("dump.lock");
    let file = std::fs::File::create(&path).map_err(|e| io(&path, e))?;
    file.lock().map_err(|e| io(&path, e))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A program builds where its manifest is, so the configuration its
    /// dependencies were fetched under applies to its build too, whatever
    /// directory the generator runs in; its paths are absolute.
    #[test]
    fn a_program_builds_in_its_own_directory() {
        let command = command(
            Path::new("target/x/work"),
            Path::new("target/x"),
            &["build"],
        )
        .expect("command");
        let here = std::env::current_dir().expect("here");
        assert_eq!(
            command.get_current_dir(),
            Some(here.join("target/x/work").as_path())
        );
        let target = command
            .get_envs()
            .find(|(k, _)| *k == "CARGO_TARGET_DIR")
            .and_then(|(_, v)| v)
            .expect("target");
        assert_eq!(Path::new(target), here.join("target/x"));
    }

    /// A program starts from the lock it is given, and keeps the lock
    /// Cargo wrote from it on the next run: written again, the seed would
    /// drop the program's own package, and the program would be resolved
    /// again for nothing.
    #[test]
    fn a_program_starts_from_its_seed_and_keeps_its_lock() {
        let dir = tempfile::Builder::new()
            .prefix("gdl-gen-seed-")
            .tempdir()
            .expect("dir");
        let work = dir.path().join("program");
        let main = "fn main() {}\n";
        write_program(&work, "program", "", main, Some("# the seed\n")).expect("written");
        let lock = work.join("Cargo.lock");
        assert_eq!(
            std::fs::read_to_string(&lock).expect("lock"),
            "# the seed\n"
        );

        std::fs::write(&lock, "# what cargo wrote\n").expect("lock");
        write_program(&work, "program", "", main, Some("# the seed\n")).expect("written");
        assert_eq!(
            std::fs::read_to_string(&lock).expect("lock"),
            "# what cargo wrote\n"
        );

        let unseeded = dir.path().join("unseeded");
        write_program(&unseeded, "unseeded", "", main, None).expect("written");
        assert!(!unseeded.join("Cargo.lock").exists());
    }

    /// A contract's program starts from the lock of the workspace that
    /// builds the contract: a member's is its workspace's, and a crate that
    /// is its own workspace has its own, or none.
    #[test]
    fn the_seed_is_the_lock_of_the_workspace_that_holds_the_crate() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let asr = std::fs::read_to_string(root.join("examples/asr/Cargo.lock")).expect("lock");
        assert_eq!(
            workspace_lock(&root.join("examples/asr/contract")).as_deref(),
            Some(asr.as_str())
        );

        let dir = tempfile::Builder::new()
            .prefix("gdl-gen-workspace-lock-")
            .tempdir()
            .expect("dir");
        std::fs::create_dir_all(dir.path().join("src")).expect("src");
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname = \"alone\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n",
        )
        .expect("manifest");
        std::fs::write(dir.path().join("src/lib.rs"), "").expect("lib");
        assert_eq!(workspace_lock(dir.path()), None);
        std::fs::write(dir.path().join("Cargo.lock"), "# its own\n").expect("lock");
        assert_eq!(workspace_lock(dir.path()).as_deref(), Some("# its own\n"));
    }

    /// Every dump of one contract in a target directory builds and runs
    /// the same program path, so a dump must hold the directory's lock from
    /// its build until its program has run: otherwise another dump can
    /// rewrite the program in between.
    /// Threads of one process that write the same file at once each
    /// succeed, and the file holds the text.
    #[test]
    fn concurrent_writers_of_one_file_all_succeed() {
        let dir = tempfile::Builder::new()
            .prefix("gdl-gen-write-")
            .tempdir()
            .expect("dir");
        let path = dir.path().join("lib.rs");
        let text = "x".repeat(1024);
        let barrier = std::sync::Barrier::new(16);
        let results: Vec<Result<(), Error>> = std::thread::scope(|scope| {
            let writers: Vec<_> = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        write(&path, &text)
                    })
                })
                .collect();
            writers
                .into_iter()
                .map(|w| w.join().expect("writer"))
                .collect()
        });
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(std::fs::read_to_string(&path).expect("read"), text);
    }

    #[test]
    fn a_dump_waits_while_another_holds_the_target_directory() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let spec = crate::Spec::load(&root.join("examples/asr/gdl.toml")).expect("spec");
        let contract = spec.contract().expect("a contract spec").clone();
        let seed = workspace_lock(&contract.path);
        let target = root.join("target/gdl-gen-tests");
        // Built once, so the next dump neither compiles nor links; that one
        // is timed, and the wait below is many times as long. A dump that
        // ignored the lock would finish well inside it.
        manifest(&contract, seed.as_deref(), &target).expect("manifest");
        let started = std::time::Instant::now();
        manifest(&contract, seed.as_deref(), &target).expect("manifest");
        // Bounded above too: other tests share this target directory, and a
        // timed run that waited for their builds would hold the lock below
        // for tens of seconds. A dump that ignored the lock still finishes
        // in well under five seconds once built.
        let wait = (started.elapsed() * 10).clamp(Duration::from_secs(2), Duration::from_secs(5));

        let held = lock(&target).expect("lock");
        let (tx, rx) = std::sync::mpsc::channel();
        let waiting = target.clone();
        std::thread::spawn(move || {
            let _ = tx.send(manifest(&contract, seed.as_deref(), &waiting).map(|m| m.name));
        });
        assert!(
            rx.recv_timeout(wait).is_err(),
            "a dump ran while another held the target directory"
        );
        drop(held);
        let name = rx
            .recv_timeout(Duration::from_secs(120))
            .expect("the dump finished")
            .expect("manifest");
        assert_eq!(name, "asr");
    }
}

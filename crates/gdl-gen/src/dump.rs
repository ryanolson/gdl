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

/// The manifest of `contract`. `target` is the cargo target directory to
/// build in; reusing one across runs keeps the build incremental.
pub fn manifest(contract: &Contract, target: &Path) -> Result<gdl::Manifest, Error> {
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
    let name = format!("gdl-manifest-{:016x}", hash(&(&dependencies, &main)));
    let text = run(
        target,
        &name,
        "the contract's manifest",
        &dependencies,
        &main,
    )?;
    gdl::Manifest::from_toml(&text).map_err(|e| Error::new(format!("reading the manifest: {e}")))
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
/// program prints names it in a failure.
pub(crate) fn run(
    target: &Path,
    name: &str,
    what: &str,
    dependencies: &str,
    main: &str,
) -> Result<String, Error> {
    // Every run of `name` in `target` builds the same program at the same
    // path. Holding the directory's lock from writing the program until it
    // has run keeps another run from replacing it in between, which would
    // make this one print the other's output.
    let _lock = lock(target)?;
    let work = target.join("gdl-gen").join(name);
    let setup = || {
        std::fs::create_dir_all(work.join("src")).map_err(|e| io(&work, e))?;
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
        write(&work.join("src/main.rs"), main)
    };
    setup()?;
    // Built, then run, so that a failure says which of the two it was.
    // Cargo runs it: it knows where the target and the profile put the
    // program, and what it needs at run time (a dynamically linked standard
    // library, for one).
    cargo(&work, target, &["build", "--quiet"])
        .map_err(|e| Error::new(format!("building {what} failed:\n{e}")))?;
    cargo(&work, target, &["run", "--quiet"])
        .map_err(|e| Error::new(format!("reading {what} failed:\n{e}")))
}

/// Run `cargo <args>` on the program in `work`, and return its output.
fn cargo(work: &Path, target: &Path, args: &[&str]) -> Result<String, Error> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(args)
        .arg("--manifest-path")
        .arg(work.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        .output()
        .map_err(|e| Error::new(format!("running cargo: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(String::from_utf8_lossy(&output.stderr)));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| Error::new(format!("cargo's output is not UTF-8: {e}")))
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

    /// Every dump of one contract in a target directory builds and runs
    /// the same program path, so a dump must hold the directory's lock from
    /// its build until its program has run: otherwise another dump can
    /// rewrite the program in between.
    /// Threads of one process that write the same file at once each
    /// succeed, and the file holds the text.
    #[test]
    fn concurrent_writers_of_one_file_all_succeed() {
        let dir = std::env::temp_dir().join(format!("gdl-gen-write-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("lib.rs");
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
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_dump_waits_while_another_holds_the_target_directory() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let spec = crate::Spec::load(&root.join("examples/asr/gdl.toml")).expect("spec");
        let contract = spec.contract().expect("a contract spec").clone();
        let target = root.join("target/gdl-gen-tests");
        // Built once, so the next dump neither compiles nor links; that one
        // is timed, and the wait below is many times as long. A dump that
        // ignored the lock would finish well inside it.
        manifest(&contract, &target).expect("manifest");
        let started = std::time::Instant::now();
        manifest(&contract, &target).expect("manifest");
        // Bounded above too: other tests share this target directory, and a
        // timed run that waited for their builds would hold the lock below
        // for tens of seconds. A dump that ignored the lock still finishes
        // in well under five seconds once built.
        let wait = (started.elapsed() * 10).clamp(Duration::from_secs(2), Duration::from_secs(5));

        let held = lock(&target).expect("lock");
        let (tx, rx) = std::sync::mpsc::channel();
        let waiting = target.clone();
        std::thread::spawn(move || {
            let _ = tx.send(manifest(&contract, &waiting).map(|m| m.name));
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

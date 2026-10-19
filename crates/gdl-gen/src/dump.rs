//! Reading what only compiled code knows: a contract's manifest, and the
//! message catalogue of the crates a graph file draws on. Each is read by
//! building and running a throwaway program that depends on those crates
//! and prints it.
//!
//! The manifest is produced by the `graph!` expansion, and the catalogue by
//! `derive(Message)`, so the only faithful way to read either is to compile
//! the crates. Each program depends on the crates it reads alone, so this
//! never compiles an implementation crate.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::Error;
use crate::spec::{Contract, GdlDep, MessageCrate};

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

/// The catalogue of every message type linked from `crates`, read with the
/// `gdl-message` of the gdl that `gdl` names.
///
/// What the program links decides what it reads: no prediction from the
/// dependency graph stands in for the linker. A program that links a
/// second gdl's catalogue panics when it reads its own, because linkme
/// gives both one section name. That panic is reported as the conflict it
/// is, with every package named `gdl-message` and the crates that use it.
pub fn catalogue(
    crates: &[MessageCrate],
    gdl: &GdlDep,
    target: &Path,
) -> Result<gdl_message::Catalogue, Error> {
    // The program depends on `gdl-message`, not on the runtime, so it
    // builds only what the message crates need.
    let mut dependencies = match gdl.message()? {
        GdlDep::Path(path) => format!(
            "gdl-message = {{ path = {} }}\n",
            toml_string(&absolute(&path)?.to_string_lossy())
        ),
        GdlDep::Version(version) => {
            format!("gdl-message = {{ version = {} }}\n", toml_string(&version))
        }
    };
    let mut main = String::new();
    for (index, krate) in crates.iter().enumerate() {
        // Each crate under a name of the program's own: its library's name
        // may be a keyword (`gen`), or `gdl`.
        dependencies.push_str(&format!(
            "{} = {{ package = {}, path = {} }}\n",
            alias(index),
            toml_string(&krate.package),
            toml_string(&absolute(&krate.path)?.to_string_lossy())
        ));
        // A crate the program never names is never linked, and neither
        // are its catalogue entries.
        main.push_str(&format!("use {} as _;\n", alias(index)));
    }
    main.push_str(
        "\nfn main() {\n    print!(\"{}\", gdl_message::Catalogue::linked().to_toml());\n}\n",
    );
    // One program per set of dependencies: a spec that links other crates
    // builds its own, and switching between specs relinks neither.
    let name = format!("gdl-catalogue-{:016x}", hash(&dependencies));
    let text = match run(target, &name, "the message catalogue", &dependencies, &main) {
        Ok(text) => text,
        // Only the program itself, once built and running, can have
        // linked two catalogues: a duplicate inside a proc macro fails the
        // build, and is reported as the compiler reports it.
        Err(Failure::Run(text)) if text.contains(DUPLICATE) => {
            return Err(two_catalogues(&target.join("gdl-gen").join(&name), target));
        }
        Err(failure) => return Err(failure.into()),
    };
    gdl_message::Catalogue::from_toml(&text)
        .map_err(|e| Error::new(format!("reading the message catalogue: {e}")))
}

/// What linkme prints when a program links two catalogues.
const DUPLICATE: &str = "duplicate #[distributed_slice] with name \"GDL_MESSAGE_CATALOGUE\"";

/// The error for a catalogue program that linked two catalogues: each
/// package named `gdl-message` other than the program's own, with the packages
/// that depend on it. Only a program that did link two reaches here, so
/// listing a package that is not linked can only add detail.
fn two_catalogues(work: &Path, target: &Path) -> Error {
    #[derive(serde::Deserialize)]
    struct Metadata {
        packages: Vec<Package>,
        resolve: Resolve,
    }
    #[derive(serde::Deserialize)]
    struct Package {
        id: String,
        name: String,
        version: String,
        manifest_path: PathBuf,
    }
    #[derive(serde::Deserialize)]
    struct Resolve {
        root: String,
        nodes: Vec<Node>,
    }
    #[derive(serde::Deserialize)]
    struct Node {
        id: String,
        deps: Vec<Dep>,
    }
    #[derive(serde::Deserialize)]
    struct Dep {
        name: String,
        pkg: String,
    }

    let head = "the catalogue program links two message catalogues, and a program holds one";
    let tail = "make every message crate depend on the gdl-message of the gdl that [gdl] names";
    let metadata = cargo(work, target, &["metadata", "--format-version", "1"])
        .ok()
        .and_then(|text| serde_json::from_str::<Metadata>(&text).ok());
    let Some(metadata) = metadata else {
        return Error::new(format!("{head}; {tail}"));
    };
    let name = |id: &str| {
        metadata
            .packages
            .iter()
            .find(|p| p.id == id)
            .map_or_else(|| id.to_string(), |p| p.name.clone())
    };
    let ours = metadata
        .resolve
        .nodes
        .iter()
        .find(|n| n.id == metadata.resolve.root)
        .and_then(|root| root.deps.iter().find(|d| d.name == "gdl_message"))
        .map(|d| d.pkg.as_str());
    let others: Vec<String> = metadata
        .packages
        .iter()
        .filter(|p| p.name == "gdl-message" && Some(p.id.as_str()) != ours)
        .map(|p| {
            let users: Vec<String> = metadata
                .resolve
                .nodes
                .iter()
                .filter(|n| n.deps.iter().any(|d| d.pkg == p.id))
                .map(|n| format!("`{}`", name(&n.id)))
                .collect();
            format!(
                "`gdl-message {}` ({}) is used by {}",
                p.version,
                p.manifest_path.display(),
                users.join(", ")
            )
        })
        .collect();
    Error::new(format!("{head}: {}; {tail}", others.join("; ")))
}

/// A name for generated content: the same content, the same name, within
/// one build of the generator.
pub(crate) fn hash(value: &impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn alias(index: usize) -> String {
    format!("gdl_messages_{index}")
}

/// Write the program `name` with these `[dependencies]` lines and this
/// `main.rs`, then build and run it, and return what it printed. `what` the
/// program prints names it in a failure.
fn run(
    target: &Path,
    name: &str,
    what: &str,
    dependencies: &str,
    main: &str,
) -> Result<String, Failure> {
    // Every run of `name` in `target` builds the same program at the same
    // path. Holding the directory's lock from writing the program until it
    // has run keeps another run from replacing it in between, which would
    // make this one print the other's output.
    let _lock = lock(target).map_err(|e| Failure::Build(e.0))?;
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
    setup().map_err(|e| Failure::Build(e.0))?;
    // Built, then run, so that a failure says which of the two it was.
    // Cargo runs it: it knows where the target and the profile put the
    // program, and what it needs at run time (a dynamically linked standard
    // library, for one).
    cargo(&work, target, &["build", "--quiet"])
        .map_err(|e| Failure::Build(format!("building {what} failed:\n{e}")))?;
    cargo(&work, target, &["run", "--quiet"])
        .map_err(|e| Failure::Run(format!("reading {what} failed:\n{e}")))
}

/// Why a program of [`run`] produced no output.
enum Failure {
    /// It did not build.
    Build(String),
    /// It built, and failed as it ran.
    Run(String),
}

impl From<Failure> for Error {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Build(text) | Failure::Run(text) => Error::new(text),
        }
    }
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

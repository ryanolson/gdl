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
    let text = run(target, "gdl-manifest-dump", &dependencies, &main)
        .map_err(|e| Error::new(format!("building the contract's manifest failed:\n{e}")))?;
    gdl::Manifest::from_toml(&text).map_err(|e| Error::new(format!("reading the manifest: {e}")))
}

/// The catalogue of every message type linked from `crates`, read with the
/// gdl that `gdl` names: a crate that registers with another gdl registers
/// in another catalogue, and is reported as offering nothing.
pub fn catalogue(
    crates: &[MessageCrate],
    gdl: &GdlDep,
    target: &Path,
) -> Result<gdl::Catalogue, Error> {
    let mut dependencies = match gdl {
        GdlDep::Path(path) => format!(
            "gdl = {{ path = {} }}\n",
            toml_string(&absolute(path)?.to_string_lossy())
        ),
        GdlDep::Version(version) => format!("gdl = {{ version = {} }}\n", toml_string(version)),
    };
    let mut main = String::new();
    for krate in crates {
        dependencies.push_str(&format!(
            "{} = {{ path = {} }}\n",
            krate.package,
            toml_string(&absolute(&krate.path)?.to_string_lossy())
        ));
        // A crate the program never names is never linked, and neither
        // are its catalogue entries.
        main.push_str(&format!(
            "use {} as _;\n",
            crate::spec::default_module(&krate.lib)
        ));
    }
    main.push_str("\nfn main() {\n    print!(\"{}\", gdl::Catalogue::linked().to_toml());\n}\n");
    // One program per set of dependencies: a spec that links other crates
    // builds its own, and switching between specs relinks neither.
    let name = format!("gdl-catalogue-{:016x}", {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        dependencies.hash(&mut hasher);
        hasher.finish()
    });
    let text = run(target, &name, &dependencies, &main)
        .map_err(|e| Error::new(format!("building the message catalogue failed:\n{e}")))?;
    gdl::Catalogue::from_toml(&text)
        .map_err(|e| Error::new(format!("reading the message catalogue: {e}")))
}

/// Build and run the program `name` with these `[dependencies]` lines and
/// this `main.rs`, and return what it prints.
fn run(target: &Path, name: &str, dependencies: &str, main: &str) -> Result<String, Error> {
    // Every run of `name` in `target` builds the same program at the same
    // path. Holding the directory's lock from writing the program until it
    // has run keeps another run from replacing it in between, which would
    // make this one print the other's output.
    let _lock = lock(target)?;
    let work = target.join("gdl-gen").join(name);
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
    write(&work.join("src/main.rs"), main)?;

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["run", "--quiet", "--manifest-path"])
        .arg(work.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        .output()
        .map_err(|e| Error::new(format!("running cargo: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(String::from_utf8_lossy(&output.stderr)));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| Error::new(format!("the program's output is not UTF-8: {e}")))
}

/// Write `text` unless the file already holds it: a rewrite with the same
/// text still changes the file's modification time, and cargo would rebuild
/// and relink the program for it.
fn write(path: &Path, text: &str) -> Result<(), Error> {
    if std::fs::read_to_string(path).is_ok_and(|existing| existing == text) {
        return Ok(());
    }
    std::fs::write(path, text).map_err(|e| io(path, e))
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

    /// Every dump in one target directory builds and runs the same program
    /// path, so a dump must hold the directory's lock from its build until
    /// its program has run: otherwise another dump can replace the program
    /// in between, and this one reads the other contract's manifest.
    #[test]
    fn a_dump_waits_while_another_holds_the_target_directory() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let spec = crate::Spec::load(&root.join("examples/chat/gdl.toml")).expect("spec");
        let contract = spec.contract().expect("a contract spec").clone();
        let target = root.join("target/gdl-gen-tests");
        // Built once, so the next dump neither compiles nor links; that one
        // is timed, and the wait below is many times as long. A dump that
        // ignored the lock would finish well inside it.
        manifest(&contract, &target).expect("manifest");
        let started = std::time::Instant::now();
        manifest(&contract, &target).expect("manifest");
        let wait = (started.elapsed() * 10).max(Duration::from_secs(2));

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
        assert_eq!(name, "chat");
    }
}

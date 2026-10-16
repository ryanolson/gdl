//! Reading a contract's manifest: build and run a throwaway program that
//! depends on the contract crate and prints `manifest()`.
//!
//! The manifest is produced by the `graph!` expansion, so the only faithful
//! way to read it is to compile the contract. The program depends on the
//! contract alone, so this compiles the contract and gdl, never an
//! implementation crate.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::Error;
use crate::spec::Spec;

/// The manifest of `spec`'s contract. `target` is the cargo target directory
/// to build in; reusing one across runs keeps the build incremental.
pub fn manifest(spec: &Spec, target: &Path) -> Result<gdl::Manifest, Error> {
    // Every dump in `target` builds the same program at the same path.
    // Holding the directory's lock from writing the program until it has
    // run keeps another dump from replacing it in between, which would make
    // this one print the other contract's manifest.
    let _lock = lock(target)?;
    let work = target.join("gdl-gen").join("dump");
    std::fs::create_dir_all(work.join("src")).map_err(|e| io(&work, e))?;
    let contract = absolute(&spec.contract.path)?;
    let manifest = format!(
        "[package]\n\
         name = \"gdl-manifest-dump\"\n\
         version = \"0.0.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\
         \n\
         # Its own workspace, wherever the target directory is.\n\
         [workspace]\n\
         \n\
         [dependencies]\n\
         {package} = {{ path = {path} }}\n",
        package = spec.contract.package,
        path = toml_string(&contract.to_string_lossy()),
    );
    let main = format!(
        "fn main() {{\n    print!(\"{{}}\", {module}::manifest().to_toml());\n}}\n",
        module = spec.contract.module,
    );
    write(&work.join("Cargo.toml"), &manifest)?;
    write(&work.join("src/main.rs"), &main)?;

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["run", "--quiet", "--manifest-path"])
        .arg(work.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        .output()
        .map_err(|e| Error::new(format!("running cargo: {e}")))?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "building the contract's manifest failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|e| Error::new(format!("the manifest is not UTF-8: {e}")))?;
    gdl::Manifest::from_toml(&text).map_err(|e| Error::new(format!("reading the manifest: {e}")))
}

/// Write `text` unless the file already holds it: a rewrite with the same
/// text still changes the file's modification time, and cargo would rebuild
/// and relink the program for it.
fn write(path: &Path, text: &str) -> Result<(), Error> {
    if std::fs::read_to_string(path).is_ok_and(|old| old == text) {
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
        let spec = Spec::load(&root.join("examples/chat/gdl.toml")).expect("spec");
        let target = root.join("target/gdl-gen-tests");
        // Built once, so the next dump neither compiles nor links; that one
        // is timed, and the wait below is many times as long. A dump that
        // ignored the lock would finish well inside it.
        manifest(&spec, &target).expect("manifest");
        let started = std::time::Instant::now();
        manifest(&spec, &target).expect("manifest");
        let wait = (started.elapsed() * 10).max(Duration::from_secs(2));

        let held = lock(&target).expect("lock");
        let (tx, rx) = std::sync::mpsc::channel();
        let waiting = target.clone();
        std::thread::spawn(move || {
            let _ = tx.send(manifest(&spec, &waiting).map(|m| m.name));
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

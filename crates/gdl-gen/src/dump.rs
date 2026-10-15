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
    // The program's source is private to this run, so concurrent runs over
    // one target directory cannot rewrite each other's program between
    // writing it and building it. Only the build cache is shared.
    let private = Private::new(target.join("gdl-gen").join(format!(
        "dump-{}-{}-{}",
        spec.contract.package,
        std::process::id(),
        RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )))?;
    let work = private.path();
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

/// Runs in this process, so two renders in one process get two directories.
static RUNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A directory removed when dropped.
struct Private(PathBuf);

impl Private {
    fn new(path: PathBuf) -> Result<Self, Error> {
        std::fs::create_dir_all(path.join("src")).map_err(|e| io(&path, e))?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Private {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn write(path: &Path, text: &str) -> Result<(), Error> {
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

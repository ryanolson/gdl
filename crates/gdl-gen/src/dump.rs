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
    let work = target
        .join("gdl-gen")
        .join(dump_name(&spec.contract.package));
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
    write_if_changed(&work.join("Cargo.toml"), &manifest)?;
    write_if_changed(&work.join("src/main.rs"), &main)?;

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

/// One dump program per contract package, so two contracts sharing a
/// target directory do not rebuild each other's.
fn dump_name(package: &str) -> String {
    format!("dump-{package}")
}

/// Rewriting an unchanged file would make cargo rebuild the dump program.
fn write_if_changed(path: &Path, text: &str) -> Result<(), Error> {
    if std::fs::read_to_string(path).is_ok_and(|old| old == text) {
        return Ok(());
    }
    std::fs::write(path, text).map_err(|e| io(path, e))
}

pub(crate) fn absolute(path: &Path) -> Result<PathBuf, Error> {
    std::path::absolute(path)
        .map(|p| crate::paths::normalize(&p))
        .map_err(|e| io(path, e))
}

pub(crate) fn toml_string(text: &str) -> String {
    toml::Value::String(text.to_string()).to_string()
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::new(format!("{}: {error}", path.display()))
}

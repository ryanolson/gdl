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
    let ((), text) = run(
        target,
        &name,
        "the contract's manifest",
        &dependencies,
        &main,
        |_| Ok(()),
    )?;
    gdl::Manifest::from_toml(&text).map_err(|e| Error::new(format!("reading the manifest: {e}")))
}

/// The catalogue of every message type linked from `crates`, read with the
/// gdl that `gdl` names, and each listed crate's package identity, in the
/// order of `crates`.
///
/// Before it builds anything, this refuses a program that links a second
/// package named `gdl`: both register into a linker section of one name,
/// and the catalogue panics on first use. It also refuses a listed crate
/// that another package in the program shares a name and a version with,
/// because catalogue entries tell packages apart by those two alone.
pub fn catalogue(
    crates: &[MessageCrate],
    gdl: &GdlDep,
    target: &Path,
) -> Result<(gdl::Catalogue, Vec<Identity>), Error> {
    let mut dependencies = match gdl {
        GdlDep::Path(path) => format!(
            "gdl = {{ path = {} }}\n",
            toml_string(&absolute(path)?.to_string_lossy())
        ),
        GdlDep::Version(version) => format!("gdl = {{ version = {} }}\n", toml_string(version)),
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
    main.push_str("\nfn main() {\n    print!(\"{}\", gdl::Catalogue::linked().to_toml());\n}\n");
    // One program per set of dependencies: a spec that links other crates
    // builds its own, and switching between specs relinks neither.
    let name = format!("gdl-catalogue-{:016x}", hash(&dependencies));
    let check = |work: &Path| {
        let metadata =
            cargo(work, target, &["metadata", "--format-version", "1"]).map_err(|e| {
                Error::new(format!(
                    "resolving the message crates' dependencies failed:\n{e}"
                ))
            })?;
        let platform = Platform::host()?;
        identities(&metadata, crates, &platform)
    };
    let (identities, text) = run(
        target,
        &name,
        "the message catalogue",
        &dependencies,
        &main,
        check,
    )?;
    let catalogue = gdl::Catalogue::from_toml(&text)
        .map_err(|e| Error::new(format!("reading the message catalogue: {e}")))?;
    Ok((catalogue, identities))
}

/// A name for generated content: the same content, the same name, within
/// one build of the generator.
pub(crate) fn hash(value: &impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// A package as catalogue entries name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The package name.
    pub package: String,
    /// Its version.
    pub version: String,
}

/// The platform the catalogue program is built for: the host's target
/// triple and its `cfg` settings.
struct Platform {
    triple: String,
    /// Each setting: a name, with its value for `key = "value"` settings.
    settings: Vec<(String, Option<String>)>,
}

impl Platform {
    fn host() -> Result<Self, Error> {
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let run = |args: &[&str]| {
            let output = Command::new(&rustc)
                .args(args)
                .output()
                .map_err(|e| Error::new(format!("running rustc: {e}")))?;
            Ok::<_, Error>(String::from_utf8_lossy(&output.stdout).into_owned())
        };
        let triple = run(&["-vV"])?
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .map(str::to_string)
            .ok_or_else(|| Error::new("rustc -vV names no host"))?;
        let settings = Self::settings(&run(&["--print", "cfg"])?);
        Ok(Self { triple, settings })
    }

    /// `rustc --print cfg` lines: `unix`, `target_os="linux"`.
    fn settings(text: &str) -> Vec<(String, Option<String>)> {
        text.lines()
            .map(|line| match line.split_once('=') {
                Some((key, value)) => (key.to_string(), Some(value.trim_matches('"').to_string())),
                None => (line.to_string(), None),
            })
            .collect()
    }

    /// Whether a dependency's platform, a target triple or a `cfg(..)`,
    /// applies here. A form this does not read applies, so that a check
    /// built on it errs toward reporting a conflict, never hiding one.
    fn applies(&self, target: &str) -> bool {
        let Some(predicate) = target
            .strip_prefix("cfg(")
            .and_then(|t| t.strip_suffix(')'))
        else {
            return target == self.triple;
        };
        syn::parse_str::<syn::Meta>(predicate).map_or(true, |meta| self.holds(&meta))
    }

    fn holds(&self, meta: &syn::Meta) -> bool {
        use syn::punctuated::Punctuated;
        let name = |path: &syn::Path| path.get_ident().map(ToString::to_string);
        match meta {
            syn::Meta::Path(path) => name(path).is_some_and(|n| self.settings.contains(&(n, None))),
            syn::Meta::NameValue(pair) => match (&pair.value, name(&pair.path)) {
                (
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(value),
                        ..
                    }),
                    Some(key),
                ) => self.settings.contains(&(key, Some(value.value()))),
                _ => true,
            },
            syn::Meta::List(list) => {
                let Ok(args) =
                    list.parse_args_with(Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
                else {
                    return true;
                };
                match name(&list.path).as_deref() {
                    Some("all") => args.iter().all(|m| self.holds(m)),
                    Some("any") => args.iter().any(|m| self.holds(m)),
                    Some("not") => args.len() != 1 || !self.holds(&args[0]),
                    _ => true,
                }
            }
        }
    }
}

fn alias(index: usize) -> String {
    format!("gdl_messages_{index}")
}

/// The listed crates' identities, from the catalogue program's `cargo
/// metadata`, after checking that the program links one gdl and that each
/// listed crate's identity is its own.
fn identities(
    metadata: &str,
    crates: &[MessageCrate],
    platform: &Platform,
) -> Result<Vec<Identity>, Error> {
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
        dep_kinds: Vec<DepKind>,
    }
    #[derive(serde::Deserialize)]
    struct DepKind {
        /// `None` for a normal dependency, else `dev` or `build`.
        kind: Option<String>,
        /// The platforms it applies to: a `cfg(..)` or a target triple.
        target: Option<String>,
    }

    let metadata: Metadata = serde_json::from_str(metadata)
        .map_err(|e| Error::new(format!("reading cargo metadata: {e}")))?;
    let package = |id: &str| metadata.packages.iter().find(|p| p.id == id);
    let named = |id: &str| {
        package(id).map_or_else(|| id.to_string(), |p| format!("`{} {}`", p.name, p.version))
    };
    let root = metadata
        .resolve
        .nodes
        .iter()
        .find(|n| n.id == metadata.resolve.root)
        .ok_or_else(|| Error::new("cargo metadata has no root package"))?;
    let dependency = |name: &str| {
        root.deps
            .iter()
            .find(|d| d.name == name)
            .map(|d| d.pkg.as_str())
    };

    // The packages linked into the program: reached from it through normal
    // dependencies. A build script's dependencies run at build time and
    // never share the program's linker sections.
    let node = |id: &str| metadata.resolve.nodes.iter().find(|n| n.id == id);
    // Cargo lists a dependency for another platform too, marked with the
    // platform it applies to.
    let normal = |d: &Dep| {
        d.dep_kinds
            .iter()
            .any(|k| k.kind.is_none() && k.target.as_deref().is_none_or(|t| platform.applies(t)))
    };
    let mut linked: Vec<&str> = vec![root.id.as_str()];
    let mut next = 0;
    while let Some(id) = linked.get(next).copied() {
        next += 1;
        for dep in node(id).into_iter().flat_map(|n| &n.deps) {
            if normal(dep) && !linked.contains(&dep.pkg.as_str()) {
                linked.push(&dep.pkg);
            }
        }
    }
    let linked_packages = || {
        metadata
            .packages
            .iter()
            .filter(|p| linked.contains(&p.id.as_str()))
    };

    let mut problems = Vec::new();
    let gdl = dependency("gdl").ok_or_else(|| Error::new("the catalogue program has no gdl"))?;
    for stray in linked_packages().filter(|p| p.name == "gdl" && p.id != gdl) {
        let users: Vec<String> = metadata
            .resolve
            .nodes
            .iter()
            .filter(|n| linked.contains(&n.id.as_str()))
            .filter(|n| n.deps.iter().any(|d| d.pkg == stray.id && normal(d)))
            .map(|n| {
                format!(
                    "`{}`",
                    package(&n.id).map_or(n.id.as_str(), |p| p.name.as_str())
                )
            })
            .collect();
        problems.push(format!(
            "the message crates link more than one package named `gdl`, and a program holds one message catalogue: `gdl {}` ({}) is used by {}; make every message crate depend on the gdl that [gdl] names, {}",
            stray.version,
            stray.manifest_path.display(),
            users.join(", "),
            named(gdl),
        ));
    }
    let mut identities = Vec::with_capacity(crates.len());
    for index in 0..crates.len() {
        let id = dependency(&alias(index))
            .ok_or_else(|| Error::new(format!("cargo metadata lacks {}", alias(index))))?;
        let listed = package(id).ok_or_else(|| Error::new(format!("cargo metadata lacks {id}")))?;
        for twin in metadata
            .packages
            .iter()
            .filter(|p| p.id != listed.id && p.name == listed.name && p.version == listed.version)
        {
            problems.push(format!(
                "[messages] crate `{} {}` ({}) has the name and version of another package in the program ({}), and a message records only those two; give one of them another version",
                listed.name,
                listed.version,
                listed.manifest_path.display(),
                twin.manifest_path.display()
            ));
        }
        identities.push(Identity {
            package: listed.name.clone(),
            version: listed.version.clone(),
        });
    }
    if problems.is_empty() {
        Ok(identities)
    } else {
        Err(Error::new(problems.join("\n")))
    }
}

/// Write the program `name` with these `[dependencies]` lines and this
/// `main.rs`, pass its directory to `check`, then build and run it, and
/// return what `check` returned and what the program printed. `what` the
/// program prints names it in a build failure.
fn run<T>(
    target: &Path,
    name: &str,
    what: &str,
    dependencies: &str,
    main: &str,
    check: impl FnOnce(&Path) -> Result<T, Error>,
) -> Result<(T, String), Error> {
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
    let checked = check(&work)?;
    let printed = cargo(&work, target, &["run", "--quiet"])
        .map_err(|e| Error::new(format!("building {what} failed:\n{e}")))?;
    Ok((checked, printed))
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
    #[test]
    fn a_dependency_platform_applies_by_its_cfg_or_triple() {
        let linux = Platform {
            triple: "x86_64-unknown-linux-gnu".into(),
            settings: Platform::settings(
                "unix\ntarget_os=\"linux\"\ntarget_pointer_width=\"64\"\n",
            ),
        };
        for (target, applies) in [
            ("cfg(unix)", true),
            ("cfg(windows)", false),
            ("cfg(not(any(unix, windows)))", false),
            ("cfg(all(unix, target_os = \"linux\"))", true),
            ("cfg(target_os = \"macos\")", false),
            ("cfg(not(target_os = \"macos\"))", true),
            ("x86_64-unknown-linux-gnu", true),
            ("x86_64-pc-windows-msvc", false),
        ] {
            assert_eq!(linux.applies(target), applies, "{target}");
        }
    }

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

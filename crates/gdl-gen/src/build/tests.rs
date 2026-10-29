use std::path::{Path, PathBuf};

use super::*;

fn example(path: &str) -> Spec {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    Spec::load(&root.join(path)).expect("a spec")
}

/// Specs of one graph build in directories of their own, so that neither
/// marks the other's crates stale.
#[test]
fn each_spec_of_a_graph_builds_in_its_own_directory() {
    let target = Path::new("/t");
    assert_eq!(
        build_dir(&example("examples/film/gdl.toml"), "film", target),
        Path::new("/t/gdl/film/gdl")
    );
    assert_eq!(
        build_dir(&example("examples/film/compact.toml"), "film", target),
        Path::new("/t/gdl/film/compact")
    );
}

/// The root lists its members by name, so that a directory left behind
/// never joins the build, and holds the release profile, which Cargo reads
/// only at the root.
#[test]
fn the_root_lists_its_members_and_holds_the_profile() {
    let spec = example("examples/chat/gdl.toml");
    let members = [
        PathBuf::from("contract"),
        PathBuf::from("compact/all"),
        PathBuf::from("front"),
    ];
    let text = root_manifest(&spec, &members, "");
    assert!(text.starts_with(&format!("# {MARKER}.\n")), "{text}");
    assert!(text.contains("cargo gdl build --spec"), "{text}");
    let manifest: toml::Table = text.parse().expect("a manifest");
    assert_eq!(
        manifest["workspace"],
        toml::from_str::<toml::Value>(
            "resolver = \"3\"\nmembers = [\"contract\", \"compact/all\", \"front\"]"
        )
        .expect("the workspace")
    );
    assert_eq!(
        manifest["profile"],
        toml::from_str::<toml::Value>("release = { codegen-units = 1, lto = \"fat\" }")
            .expect("the profile")
    );
}

/// The members are every generated crate: the contract and each segment
/// of each layout, not the root.
#[test]
fn the_members_are_the_generated_crates() {
    let files = Files::from([
        (PathBuf::from("Cargo.toml"), String::new()),
        (PathBuf::from("contract/Cargo.toml"), String::new()),
        (PathBuf::from("contract/src/lib.rs"), String::new()),
        (PathBuf::from("a/front/Cargo.toml"), String::new()),
        (PathBuf::from("a/crossings.txt"), String::new()),
    ]);
    assert_eq!(
        members(&files),
        [PathBuf::from("a/front"), PathBuf::from("contract")]
    );
}

/// A generated segment says to regenerate it with `build`, which is what
/// wrote it.
#[test]
fn a_built_segment_says_build_regenerates_it() {
    let spec = example("examples/chat/gdl.toml");
    let temp = tempfile::Builder::new()
        .prefix("gdl-build-header-")
        .tempdir()
        .expect("scratch");
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/gdl-gen-tests");
    let (files, graph) = crate::render_with(
        &spec,
        &temp.path().join("workspace"),
        &target,
        Deps::Build,
        Offers::Read,
    )
    .expect("the files");
    assert_eq!(graph, "chat");
    let manifest = &files[Path::new("front/Cargo.toml")];
    assert!(
        manifest
            .lines()
            .nth(1)
            .is_some_and(|line| line.starts_with("# Regenerate with: cargo gdl build --spec ")),
        "{manifest}"
    );
    let parsed: toml::Table = manifest.parse().expect("a manifest");
    assert!(!parsed.contains_key("workspace"), "{manifest}");
    assert!(!parsed.contains_key("profile"), "{manifest}");
}

/// A scratch directory with a build's record, a destination, and two
/// fake binaries as Cargo would leave them.
struct Copies {
    dir: tempfile::TempDir,
}

impl Copies {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-build-copies-")
            .tempdir()
            .expect("scratch");
        std::fs::create_dir_all(dir.path().join("built")).expect("directory");
        Self { dir }
    }

    fn record(&self) -> PathBuf {
        self.dir.path().join("binaries.toml")
    }

    fn out(&self, profile: &str) -> PathBuf {
        self.dir.path().join("bin").join(profile)
    }

    /// Binaries `names`, each holding `text` and its name.
    fn built(&self, names: &[&str], text: &str) -> Vec<Binary> {
        names
            .iter()
            .map(|name| {
                let path = self.dir.path().join("built").join(name);
                std::fs::write(&path, format!("{text} {name}")).expect("a binary");
                Binary {
                    name: name.to_string(),
                    path,
                }
            })
            .collect()
    }

    fn copy(&self, profile: &str, built: &[Binary]) -> Result<Vec<PathBuf>, Error> {
        copy_out(&self.record(), &self.out(profile), built)
    }
}

/// A binary the build no longer makes is removed from where the build put
/// it; one that someone replaced there stays.
#[test]
fn a_dropped_binary_is_removed_unless_someone_replaced_it() {
    let copies = Copies::new();
    let out = copies.out("debug");
    let copied = copies
        .copy("debug", &copies.built(&["a", "b", "c"], "v1"))
        .expect("copied");
    assert_eq!(copied, [out.join("a"), out.join("b"), out.join("c")]);
    std::fs::write(out.join("c"), "mine").expect("a replacement");
    copies
        .copy("debug", &copies.built(&["a"], "v1"))
        .expect("copied");
    assert!(!out.join("b").exists(), "the dropped binary stayed");
    assert_eq!(
        std::fs::read_to_string(out.join("c")).expect("kept"),
        "mine"
    );
}

/// The build writes over a file only when it wrote that file and nobody
/// changed it since; it refuses any other, and writes nothing.
#[test]
fn a_build_refuses_to_write_over_a_file_it_did_not_write() {
    let copies = Copies::new();
    let out = copies.out("debug");
    std::fs::create_dir_all(&out).expect("directory");
    std::fs::write(out.join("b"), "mine").expect("a file of the user's");
    let error = copies
        .copy("debug", &copies.built(&["a", "b"], "v1"))
        .expect_err("refused");
    assert!(
        error
            .to_string()
            .contains("was not written by `cargo gdl build`"),
        "{error}"
    );
    assert!(!out.join("a").exists(), "the refused build wrote");
    assert_eq!(
        std::fs::read_to_string(out.join("b")).expect("kept"),
        "mine"
    );

    // A file that holds what the build would write is left as it is, and
    // stays the user's: the build does not write over it later.
    std::fs::write(out.join("b"), "v1 b").expect("the same bytes");
    copies
        .copy("debug", &copies.built(&["a", "b"], "v1"))
        .expect("copied");
    copies
        .copy("debug", &copies.built(&["a", "b"], "v2"))
        .expect_err("refused");
    assert_eq!(std::fs::read_to_string(out.join("b")).expect("b"), "v1 b");
}

/// Each destination keeps its own record: a release build leaves the debug
/// binaries, and a later debug build still removes what it dropped.
#[test]
fn each_destination_keeps_its_own_record() {
    let copies = Copies::new();
    copies
        .copy("debug", &copies.built(&["a", "b"], "v1"))
        .expect("debug");
    copies
        .copy("release", &copies.built(&["a"], "v1"))
        .expect("release");
    assert!(
        copies.out("debug").join("b").exists(),
        "release removed debug's"
    );
    copies
        .copy("debug", &copies.built(&["a"], "v1"))
        .expect("debug");
    assert!(
        !copies.out("debug").join("b").exists(),
        "debug forgot its own"
    );
}

/// A run with nothing new copies nothing and leaves the record as it was.
#[test]
fn an_unchanged_copy_writes_nothing() {
    let copies = Copies::new();
    let built = copies.built(&["a"], "v1");
    copies.copy("debug", &built).expect("copied");
    let times = |path: &Path| {
        std::fs::metadata(path)
            .and_then(|m| m.modified())
            .expect("time")
    };
    let (binary, record) = (
        times(&copies.out("debug").join("a")),
        times(&copies.record()),
    );
    copies.copy("debug", &built).expect("copied");
    assert_eq!(times(&copies.out("debug").join("a")), binary);
    assert_eq!(times(&copies.record()), record);
}

/// The lock is settled again when a workspace whose patch the build
/// carries changes its lock: `host_pins` reads that lock.
#[test]
fn the_lock_inputs_cover_each_carried_workspaces_lock() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-build-inputs-")
        .tempdir()
        .expect("scratch");
    std::fs::write(temp.path().join("Cargo.lock"), "version = 4\n").expect("a lock");
    let carried = [crate::deployment::context::HostPatch {
        package: "p".into(),
        workspace: temp.path().to_path_buf(),
    }];
    let files = Files::from([(PathBuf::from("a/Cargo.toml"), "[package]\n".to_string())]);
    let before = lock_inputs(Some("seed"), &files, &carried);
    assert_eq!(before, lock_inputs(Some("seed"), &files, &carried));
    std::fs::write(temp.path().join("Cargo.lock"), "version = 4\n# moved\n").expect("a lock");
    assert_ne!(before, lock_inputs(Some("seed"), &files, &carried));
    assert_ne!(
        lock_inputs(Some("seed"), &files, &carried),
        lock_inputs(Some("other"), &files, &carried)
    );
}

/// Equal bytes do not make a file the build's to remove later.
#[test]
fn a_matching_foreign_binary_is_kept_when_its_segment_is_removed() {
    let copies = Copies::new();
    let out = copies.out("debug");
    let built = copies.built(&["a", "b"], "binary");
    std::fs::create_dir_all(&out).expect("destination");
    std::fs::copy(&built[0].path, out.join("a")).expect("a file the user supplied");
    let before = Stamp::of(&out.join("a")).expect("the user's stamp");

    copies
        .copy("debug", &built)
        .expect("matching bytes accepted");
    assert_eq!(Stamp::of(&out.join("a")).expect("untouched"), before);
    copies.copy("debug", &[]).expect("remove dropped binaries");

    assert!(
        !out.join("b").exists(),
        "the build's binary must be removed"
    );
    assert!(
        out.join("a").exists(),
        "the build removed a matching binary it never wrote"
    );
}

/// Reading the runtime must use the same Cargo configuration as its build.
#[test]
fn the_runtime_context_reads_the_specs_registry_configuration() {
    let scratch = tempfile::Builder::new()
        .prefix("gdl-build-registry-")
        .tempdir()
        .expect("scratch");
    let runtime = scratch.path().join("runtime");
    let cwd = scratch.path().join("project");
    std::fs::create_dir_all(runtime.join("src")).expect("runtime source");
    std::fs::create_dir_all(cwd.join(".cargo")).expect("project configuration");
    std::fs::write(runtime.join("src/lib.rs"), "").expect("runtime source");
    std::fs::write(
        runtime.join("Cargo.toml"),
        "[package]\nname = \"gdl-runtime\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
         [dependencies]\nitoa = { version = \"1\", registry = \"review\" }\n",
    )
    .expect("runtime manifest");
    std::fs::write(
        cwd.join(".cargo/config.toml"),
        "[registries.review]\nindex = \"sparse+https://registry.invalid/index/\"\n",
    )
    .expect("registry configuration");

    // No dependency resolution: this only needs the registry's configured name.
    let control = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(&cwd)
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(runtime.join("Cargo.toml"))
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .expect("cargo metadata");
    assert!(
        control.status.success(),
        "{}",
        String::from_utf8_lossy(&control.stderr)
    );

    let context = Context::find(
        &scratch.path().join("target/workspace"),
        &Files::new(),
        Some(&runtime),
        Mode::Build { cwd },
    );
    assert!(
        context.is_ok(),
        "runtime metadata ignored the spec's Cargo configuration: {}",
        context.err().expect("failed context")
    );
}

/// A temporary copy must not write through a link that the build did not make.
#[test]
#[cfg(unix)]
fn copying_a_binary_keeps_a_source_behind_a_foreign_temporary_symlink() {
    let copies = Copies::new();
    let out = copies.out("debug");
    std::fs::create_dir_all(&out).expect("destination");
    let source = copies.dir.path().join("source.rs");
    let before = b"fn main() {}\n";
    std::fs::write(&source, before).expect("a file the build did not write");
    let temporary = out.join(format!(".a.tmp-{}", std::process::id()));
    std::os::unix::fs::symlink(&source, &temporary).expect("a foreign symlink");

    let result = copies.copy("debug", &copies.built(&["a"], "binary"));

    assert_eq!(
        std::fs::read(&source).expect("source"),
        before,
        "the copy overwrote a source through its temporary path: {result:?}"
    );
    assert!(
        std::fs::symlink_metadata(&temporary)
            .is_ok_and(|metadata| metadata.file_type().is_symlink()),
        "the copy removed a symlink it did not write"
    );
}

/// Two spellings of one destination, `bin` and `bin/../bin`, are one
/// record: a rebuild through either replaces the build's own binary.
#[test]
fn a_destination_is_one_record_however_it_is_spelled() {
    let copies = Copies::new();
    let out = copies.out("debug");
    copies
        .copy("debug", &copies.built(&["a"], "v1"))
        .expect("copied");
    let alias = out.join("../debug");
    copy_out(&copies.record(), &alias, &copies.built(&["a"], "v1")).expect("unchanged");
    copy_out(&copies.record(), &alias, &copies.built(&["a"], "v2")).expect("its own binary");
    assert_eq!(std::fs::read_to_string(out.join("a")).expect("a"), "v2 a");
}

/// A stopped copy must keep ownership of each binary it already replaced.
#[test]
#[cfg(unix)]
fn an_interrupted_copy_keeps_ownership_on_rebuild() {
    const COPY_DIR: &str = "GDL_REVIEW_COPY_DIR";
    if let Some(dir) = std::env::var_os(COPY_DIR) {
        let dir = PathBuf::from(dir);
        let built: Vec<Binary> = ["a", "b"]
            .into_iter()
            .map(|name| Binary {
                name: name.to_string(),
                path: dir.join("built").join(name),
            })
            .collect();
        copy_out(&dir.join("binaries.toml"), &dir.join("bin/debug"), &built)
            .expect("the copy waits for b");
        return;
    }

    let copies = Copies::new();
    copies
        .copy("debug", &copies.built(&["a", "b"], "start"))
        .expect("initial copies");
    copies.built(&["a", "b"], "changed");
    let waiting = copies.dir.path().join("built/b");
    std::fs::remove_file(&waiting).expect("replace the second source");
    // The pipe holds the child between the two copies until it is stopped.
    assert!(
        Command::new("mkfifo")
            .arg(&waiting)
            .status()
            .expect("a pipe")
            .success()
    );
    let mut child = Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            "build::tests::an_interrupted_copy_keeps_ownership_on_rebuild",
            "--include-ignored",
        ])
        .env(COPY_DIR, copies.dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("a copy in another process");
    let first = copies.out("debug").join("a");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let changed = loop {
        if std::fs::read_to_string(&first).is_ok_and(|text| text == "changed a") {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    };
    child.kill().expect("interrupt the copy");
    child.wait().expect("reap the copy");
    assert!(changed, "the child did not replace the first binary");
    std::fs::remove_file(&waiting).expect("remove the pipe");

    copies
        .copy("debug", &copies.built(&["a", "b"], "changed"))
        .expect("resume the interrupted build");
    copies
        .copy("debug", &copies.built(&["a", "b"], "again"))
        .expect("a later rebuild must replace its own binaries");
}

/// A copy failure must not let a later run remove a user's replacement.
#[test]
fn a_pending_copy_keeps_a_binary_replaced_after_a_copy_error() {
    let copies = Copies::new();
    copies
        .copy("debug", &copies.built(&["a", "b"], "start"))
        .expect("initial copies");
    let changed = copies.built(&["a", "b"], "changed");
    // A directory where the binary should be: the copy fails as it reads.
    std::fs::remove_file(&changed[1].path).expect("make the second source unreadable");
    std::fs::create_dir(&changed[1].path).expect("make the second source unreadable");
    copies
        .copy("debug", &changed)
        .expect_err("the second copy failed after the first succeeded");
    assert_eq!(
        std::fs::read_to_string(copies.out("debug").join("a")).expect("the first copy"),
        "changed a"
    );
    let replaced = copies.out("debug").join("b");
    std::fs::write(&replaced, "the user's replacement").expect("replace the second binary");

    copies
        .copy("debug", &changed[..1])
        .expect("the next run drops the second segment");

    assert_eq!(
        std::fs::read_to_string(&replaced).ok().as_deref(),
        Some("the user's replacement"),
        "the pending record claimed and removed a file replaced after the failed copy"
    );
}

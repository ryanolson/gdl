//! Code generation guards for the cell policies and the node boundaries.

use std::path::{Path, PathBuf};
use std::process::Command;

/// `HomeFirst` chooses at constant thresholds: its release code passes
/// no threshold object to `HomeFirstAt`'s `choose`, which loads its
/// thresholds as data. The configurable policy's own loads are the
/// control. It builds gdl's release IR, about 9 s cold and 1 s warm.
#[test]
fn home_first_keeps_its_thresholds_in_code() {
    let ir = release_ir("lib", "gdl");
    let definition = |policy: &str| {
        definitions(&ir)
            .find(|(signature, _)| signature.contains(policy) && signature.contains("6choose"))
            .map(|(_, body)| body)
            .expect("the policy's choose definition")
    };
    let configured = definition("home_first..HomeFirstAt$u20$");
    assert!(
        configured.contains("load i64, ptr %self"),
        "the configurable policy must provide the data-load control"
    );
    let unit = definition("home_first..HomeFirst$u20$");
    let loads_thresholds = unit.lines().any(|line| {
        line.contains("call ")
            && line.contains("HomeFirstAt$u20$")
            && line.contains("6choose")
            && line.contains("@alloc_")
    });
    assert!(
        !loads_thresholds,
        "HomeFirst passes its constant thresholds as data to a chooser that loads them:\n{unit}"
    );
}

/// A node boundary (`Fuse`, `Bounded`) and a streamed request's items
/// (`UntilError`) return their inner stream's poll as their own, so an item
/// is written once, where the consumer reads it. A boundary
/// that holds the poll across a call copies the whole poll, once per item
/// at each boundary: four copies of each chunk on an endpoint's two hops.
/// The `copies` example's 2 KiB items cross both boundaries, and its
/// `Copying` boundary is the control. It builds the example in release,
/// about 25 s cold and 2 s warm.
#[test]
fn a_node_boundary_does_not_copy_an_item() {
    let ir = release_ir("example", "copies");
    // Half an item: smaller copies move a field, not the item.
    const ITEM: u64 = 1024;
    let copies = |name: &str| -> Vec<u64> {
        let (_, body) = definitions(&ir)
            .find(|(signature, _)| signature.contains(name) && signature.contains("9poll_next"))
            .unwrap_or_else(|| panic!("{name}'s poll_next definition"));
        copy_sizes(body).filter(|size| *size >= ITEM).collect()
    };
    assert!(
        !copies("copies..Copying$u20$").is_empty(),
        "the Copying control must show a copy of the item"
    );
    assert_eq!(copies("gdl..items..Fuse$LT$"), Vec::<u64>::new(), "Fuse");
    assert_eq!(
        copies("gdl..items..Bounded$LT$"),
        Vec::<u64>::new(),
        "Bounded"
    );
    assert_eq!(
        copies("gdl..items..UntilError$LT$"),
        Vec::<u64>::new(),
        "UntilError"
    );
}

/// An operator's future, and a join's, is built on the heap: `boxed`
/// allocates first and builds the future in the allocation. `Box::pin`
/// builds it on the stack and copies it whole into its box: 4,640 B on each
/// call in an endpoint. The `copies` example's operator holds 16 KiB while
/// it waits, so a copy that large moves its future; its `boxed` function is
/// the control. Every other function is searched: the copy lands wherever
/// the compiler inlines the code that makes it.
#[test]
fn an_operators_future_is_built_on_the_heap() {
    let ir = release_ir("example", "copies");
    const FUTURE: u64 = 16 * 1024;
    const CONTROL: &str = "6copies5boxed";
    let copies = |control: bool| -> Vec<u64> {
        definitions(&ir)
            .filter(|(signature, _)| signature.contains(CONTROL) == control)
            .flat_map(|(_, body)| copy_sizes(body))
            .filter(|size| *size >= FUTURE)
            .collect()
    };
    assert!(
        !copies(true).is_empty(),
        "the boxed control must show a copy of the future"
    );
    assert_eq!(copies(false), Vec::<u64>::new());
}

#[test]
fn example_ir_ignores_a_newer_artifact_from_another_build() {
    use std::io::Write;
    use std::time::{Duration, SystemTime};

    struct Artifact(PathBuf);

    impl Drop for Artifact {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    let current = release_ir("example", "copies");
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target"));
    let path = target.join(format!(
        "release/examples/copies-review-{}.ll",
        std::process::id()
    ));
    let mut file = std::fs::File::create_new(&path).expect("another build's artifact");
    let _artifact = Artifact(path);
    const MARKER: &str = "\n; IR from another build\n";
    file.write_all(current.as_bytes()).expect("copy the IR");
    file.write_all(MARKER.as_bytes()).expect("mark the IR");
    file.set_times(
        std::fs::FileTimes::new().set_modified(SystemTime::now() + Duration::from_secs(60)),
    )
    .expect("make the other artifact newer without sleeping");
    drop(file);

    let selected = release_ir("example", "copies");
    assert!(
        !selected.ends_with(MARKER),
        "release_ir selected another build's newer .ll file instead of the cached Cargo artifact"
    );
}

/// The release IR of one of gdl's targets: `kind` is `lib` or `example`.
fn release_ir(kind: &str, name: &str) -> String {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the workspace contains crates/gdl");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target"));
    // An example builds as a release build does by default, in 16 units:
    // whether a future is built in place can depend on it. Asked for IR,
    // rustc would build in one unit.
    let (selected, units) = match kind {
        "lib" => (vec!["--lib"], vec![]),
        _ => (vec!["--example", name], vec!["-C", "codegen-units=16"]),
    };
    let built = Command::new(env!("CARGO"))
        .current_dir(workspace)
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_BUILD_JOBS", "6")
        .args(["rustc", "-p", "gdl", "--release"])
        .args(selected)
        .args(["--message-format=json", "--", "--emit=llvm-ir"])
        .args(units)
        .output()
        .expect("compile gdl's release IR");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = String::from_utf8_lossy(&built.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|value| value["reason"] == "compiler-artifact" && value["target"]["name"] == name)
        .find_map(|value| {
            value["filenames"].as_array()?.iter().find_map(|file| {
                let path = PathBuf::from(file.as_str()?);
                // A library's IR is named after its metadata, an
                // executable's after the executable.
                (kind != "lib" || path.extension()? == "rmeta").then_some(path)
            })
        })
        .expect("the target's artifact");
    match kind {
        "lib" => {
            let stem = artifact
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.strip_prefix("lib"))
                .expect("a Rust library name");
            std::fs::read_to_string(artifact.with_file_name(format!("{stem}.ll")))
                .expect("the emitted IR")
        }
        // Cargo reports the executable it copies out, named without the
        // hash that names this build's IR. The hashed executable with the
        // same bytes is this build's; other builds leave their own.
        _ => {
            let built = std::fs::read(&artifact).expect("the executable");
            let hashed = std::fs::read_dir(artifact.parent().expect("the artifact's directory"))
                .expect("the artifact's directory")
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| {
                    path.extension().is_none()
                        && path
                            .file_name()
                            .and_then(|file| file.to_str())
                            .is_some_and(|file| file.starts_with(&format!("{name}-")))
                })
                .find(|path| std::fs::read(path).is_ok_and(|bytes| bytes == built))
                .expect("the hashed executable of this build");
            // One file per unit, named after the hashed executable.
            let prefix = format!(
                "{}.",
                hashed
                    .file_name()
                    .and_then(|file| file.to_str())
                    .expect("a name")
            );
            let mut units: Vec<PathBuf> = std::fs::read_dir(hashed.parent().expect("a directory"))
                .expect("the artifact's directory")
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|file| file.to_str())
                        .is_some_and(|file| file.starts_with(&prefix) && file.ends_with(".rcgu.ll"))
                })
                .collect();
            units.sort();
            // Fewer units than asked for could leave another build's files
            // under the same names.
            assert_eq!(units.len(), 16, "the example's units: {units:?}");
            units
                .iter()
                .map(|unit| std::fs::read_to_string(unit).expect("the emitted IR"))
                .collect()
        }
    }
}

/// The size of each copy of constant size in a function's body.
fn copy_sizes(body: &str) -> impl Iterator<Item = u64> {
    body.lines()
        .filter(|line| line.contains("@llvm.memcpy") || line.contains("@llvm.memmove"))
        .filter_map(|line| {
            let (_, size) = line.rsplit_once(", i64 ")?;
            size.split(',').next()?.trim().parse().ok()
        })
}

/// Each function the IR defines, as its signature line and its body.
fn definitions(ir: &str) -> impl Iterator<Item = (&str, &str)> {
    ir.split("\ndefine ").filter_map(|item| {
        let signature = item.lines().next()?;
        let (body, _) = item.split_once("\n}")?;
        Some((signature, body))
    })
}

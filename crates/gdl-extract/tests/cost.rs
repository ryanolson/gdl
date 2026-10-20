//! What the reader costs grows with the source it reads, whatever the
//! source's shape: doubling a crate at most about doubles the reader's peak
//! memory and its allocations. Each test reads one shape at two sizes and
//! compares; a cost that multiplies two lengths quadruples instead. The
//! shapes are the ones that once did: a `use` that copied its attributes
//! and its path to every name it binds, modules that copied their parent's
//! path, and a wire check that walked every impl of the crate per message.
//! Its own test binary, so that the counting allocator sees only it.

// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::PathBuf;
use std::time::Duration;

use gdl_extract::{Cargo, Workspace};

/// The system allocator, counting per thread the bytes live, their peak,
/// and the number of allocations. Per thread, so tests that run at once
/// do not count each other.
struct Counting;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

fn grew(by: isize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + by;
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grew(layout.size() as isize);
        let _ = COUNT.try_with(|count| count.set(count.get() + 1));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        grew(-(layout.size() as isize));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        grew(new_size as isize - layout.size() as isize);
        let _ = COUNT.try_with(|count| count.set(count.get() + 1));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What one extraction cost: its peak live bytes, its allocations, and how
/// long it took.
#[derive(Debug, Clone, Copy)]
struct Cost {
    peak: isize,
    allocations: u64,
    elapsed: Duration,
}

/// Write a crate of these files, load it, and read it on a thread of its
/// own: with a large stack, as the reader recurses with module depth, and
/// bounded in time, so a reader that runs away fails the test instead of
/// hanging it. Returns the cost of the extraction alone and its problems.
fn read(name: &str, files: &[(String, String)]) -> (Cost, Vec<String>) {
    let dir: PathBuf = std::env::temp_dir().join(format!("gdl-cost-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("src")).expect("dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n"
        ),
    )
    .expect("manifest");
    for (path, source) in files {
        let path = dir.join("src").join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("dir");
        std::fs::write(path, source).expect("source");
    }
    let workspace = Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("metadata");
    let (done, finished) = std::sync::mpsc::channel();
    let package = name.to_string();
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(move || {
            let started = std::time::Instant::now();
            let extraction = workspace.extract(&package).expect("the package");
            let cost = Cost {
                peak: PEAK.with(Cell::get),
                allocations: COUNT.with(Cell::get),
                elapsed: started.elapsed(),
            };
            let _ = done.send((cost, extraction.problems));
        })
        .expect("thread");
    let result = finished
        .recv_timeout(Duration::from_secs(60))
        .expect("the reader finished");
    let _ = std::fs::remove_dir_all(&dir);
    result
}

/// Read a shape at size `n` and at `2n`, and assert that neither the peak
/// memory nor the allocations grew by more than three times: linear growth
/// doubles them, a product of two lengths quadruples them.
fn grows_linearly(name: &str, shape: impl Fn(usize) -> Vec<(String, String)>, n: usize) {
    let (small, problems) = read(&format!("{name}-small"), &shape(n));
    assert!(problems.is_empty(), "{problems:?}");
    let (large, problems) = read(&format!("{name}-large"), &shape(2 * n));
    assert!(problems.is_empty(), "{problems:?}");
    let memory = large.peak as f64 / small.peak as f64;
    let work = large.allocations as f64 / small.allocations as f64;
    assert!(
        memory < 3.0 && work < 3.0,
        "{name}: doubling the source multiplied the peak memory by {memory:.1} and the \
         allocations by {work:.1}\n  n:  {small:?}\n  2n: {large:?}"
    );
}

/// One `use` binds many names and carries a large attribute: each name
/// copied the attribute, so memory grew with names times attribute size.
/// Both grow with `n` here.
#[test]
fn a_use_shares_its_attributes_among_its_names() {
    grows_linearly(
        "attrs",
        |n| {
            let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
            let source = format!(
                "#[doc = \"{}\"]\npub use self::inner::{{{}}};\npub mod inner {{}}\n",
                "x".repeat(16 * n),
                names.join(", ")
            );
            vec![("lib.rs".into(), source)]
        },
        4096,
    );
}

/// One `use` binds many names under a long path: each name copied the
/// path, so memory grew with names times path length.
#[test]
fn a_use_shares_its_path_among_its_names() {
    grows_linearly(
        "prefix",
        |n| {
            let path: Vec<String> = (0..n).map(|i| format!("p{i}")).collect();
            let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
            let source = format!("pub use {}::{{{}}};\n", path.join("::"), names.join(", "));
            vec![("lib.rs".into(), source)]
        },
        2048,
    );
}

/// A chain of modules with long names, each in its own file: each module
/// copied its parent's path, so memory grew with depth times the length of
/// the path. A message at the bottom is still found at its full path.
#[test]
fn a_module_does_not_copy_its_parents_path() {
    let shape = |n: usize| {
        let mut files = vec![(
            "lib.rs".to_string(),
            "#[path = \"m0.rs\"]\npub mod chain;\n".to_string(),
        )];
        for level in 0..n {
            let next = level + 1;
            let name = format!("m{level}_{}", "x".repeat(2000));
            let body = if next == n {
                "#[derive(gdl_message::Message)]\npub struct Deep;\n".to_string()
            } else {
                format!("#[path = \"m{next}.rs\"]\npub mod {name};\n")
            };
            files.push((format!("m{level}.rs"), body));
        }
        files
    };
    grows_linearly("depth", shape, 200);
    let (_, problems) = read("depth-found", &shape(3));
    assert!(problems.is_empty(), "{problems:?}");
}

/// Many messages, each with a hand-written `Deserialize`: the wire check
/// walked and resolved every impl of the crate for each message, so the
/// work grew with messages times impls.
#[test]
fn the_wire_check_finds_a_types_impls_without_walking_the_crate() {
    grows_linearly(
        "impls",
        |n| {
            let mut source = String::from("use serde::{Deserialize, Serialize};\n");
            for i in 0..n {
                source.push_str(&format!(
                    "#[derive(Serialize, gdl_message::Message)]\npub struct M{i};\n\
                     impl<'de> Deserialize<'de> for M{i} {{}}\n"
                ));
            }
            vec![("lib.rs".into(), source)]
        },
        500,
    );
}

/// Read a shape at size `n` and at `2n`, the faster of two runs each, and
/// assert that the time grew by less than three times. For work no
/// allocation witnesses, such as a scan; a square would quadruple it.
fn time_grows_linearly(name: &str, shape: impl Fn(usize) -> Vec<(String, String)>, n: usize) {
    let fastest = |label: &str, n: usize| {
        (0..2)
            .map(|run| {
                let (cost, problems) = read(&format!("{name}-{label}-{run}"), &shape(n));
                assert!(problems.is_empty(), "{problems:?}");
                cost.elapsed
            })
            .min()
            .expect("two runs")
    };
    let (small, large) = (fastest("small", n), fastest("large", 2 * n));
    let ratio = large.as_secs_f64() / small.as_secs_f64();
    assert!(
        ratio < 3.0,
        "{name}: doubling the source multiplied the time by {ratio:.1}: {small:?} then {large:?}"
    );
}

/// Many names bound by `use`s carrying an attribute of a single name: the
/// export check looked each such attribute up among every `use` of the
/// module, so the time grew with the square of the names.
#[test]
fn the_export_check_does_not_scan_the_module_per_name() {
    time_grows_linearly(
        "scan",
        |n| {
            let mut source = String::from("pub mod inner {}\n");
            for i in 0..n {
                source.push_str(&format!("#[doc = \"d\"]\npub use self::inner::n{i};\n"));
            }
            vec![("lib.rs".to_string(), source)]
        },
        8000,
    );
}

/// One `use` item with many attributes binds many names: the export check
/// ran per name over every attribute, so the work grew with names times
/// attributes.
#[test]
fn the_export_check_runs_once_per_use_item() {
    grows_linearly(
        "marks",
        |n| {
            let attrs: String = (0..n).map(|_| "#[doc = \"d\"]\n").collect();
            let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
            let source = format!(
                "{attrs}pub use self::inner::{{{}}};\npub mod inner {{}}\n",
                names.join(", ")
            );
            vec![("lib.rs".into(), source)]
        },
        1024,
    );
}

/// Paths inside braces under a long prefix (`p0::...::pn::{x::a, x::b}`):
/// each copied the prefix to add its own segment.
#[test]
fn a_path_in_braces_adds_one_segment() {
    grows_linearly(
        "paths",
        |n| {
            let path: Vec<String> = (0..n).map(|i| format!("p{i}")).collect();
            let names: Vec<String> = (0..n).map(|i| format!("x::n{i}")).collect();
            let source = format!("pub use {}::{{{}}};\n", path.join("::"), names.join(", "));
            vec![("lib.rs".into(), source)]
        },
        2048,
    );
}

/// Groups nested in groups (`a0::{a1::{...{z}}}`): each group copied the
/// path before it.
#[test]
fn nested_groups_share_their_prefix() {
    grows_linearly(
        "groups",
        |n| {
            let mut source = String::from("pub use ");
            for i in 0..n {
                source.push_str(&format!("a{i}_{}::{{", "x".repeat(64)));
            }
            source.push('z');
            source.push_str(&"}".repeat(n));
            source.push_str(";\n");
            vec![("lib.rs".into(), source)]
        },
        256,
    );
}

/// Inline modules nested deep with long names: each level built the path
/// of its directory, though only a module file in it would need one.
#[test]
fn an_inline_module_does_not_build_its_directory() {
    grows_linearly(
        "inline",
        |n| {
            let name = format!("m{}", "x".repeat(2000));
            let mut source = String::new();
            for _ in 0..n {
                source.push_str(&format!("pub mod {name} {{\n"));
            }
            source.push_str("#[derive(gdl_message::Message)]\npub struct Deep;\n");
            source.push_str(&"}\n".repeat(n));
            vec![("lib.rs".into(), source)]
        },
        100,
    );
}

/// Many types, each with a serde impl: each impl's type was found by
/// scanning its module's types, so the time grew with types times impls.
/// A guard on time, not a proof: the scan it guards against grows the
/// time by 3.6 to 3.8 times here and is caught in most runs, not all.
#[test]
fn a_type_is_found_by_its_name() {
    time_grows_linearly(
        "types",
        |n| {
            // One message without derived serde makes the wire check
            // resolve every serde impl of the crate.
            let mut source = String::from(
                "use serde::Serialize;\n#[derive(gdl_message::Message)]\npub struct M;\n",
            );
            for i in 0..n {
                source.push_str(&format!("pub struct T{i};\nimpl Serialize for T{i} {{}}\n"));
            }
            vec![("lib.rs".into(), source)]
        },
        16000,
    );
}

/// Many nodes, each with its role impl: the role check walked and resolved
/// every impl of the crate for each node.
#[test]
fn the_role_check_finds_a_nodes_impl_without_walking_the_crate() {
    grows_linearly(
        "roles",
        |n| {
            let mut source = String::from(
                "#[derive(gdl_message::Message, serde::Serialize, serde::Deserialize)]\npub struct W;\n",
            );
            for i in 0..n {
                source.push_str(&format!(
                    "#[derive(gdl::Node)]\npub struct N{i};\n\
                     impl gdl::Sink for N{i} {{ type Req = W; type Item = W; }}\n"
                ));
            }
            vec![("lib.rs".into(), source)]
        },
        250,
    );
}

/// A file reached by a long path holds many names: each name's location
/// copied the file's path. A detour of 400 bytes in the path must not
/// add to the peak per name.
#[test]
fn locations_share_their_files_path() {
    let shape = |detour: &str| {
        let names: Vec<String> = (0..20_000).map(|i| format!("n{i}")).collect();
        vec![
            (
                "lib.rs".to_string(),
                format!("#[path = \"{detour}x.rs\"]\npub mod x;\n"),
            ),
            ("d/empty.rs".to_string(), String::new()),
            (
                "x.rs".to_string(),
                format!(
                    "pub use self::inner::{{{}}};\npub mod inner {{}}\n",
                    names.join(", ")
                ),
            ),
        ]
    };
    let detour = "d/../".repeat(80);
    let near = read("near", &shape(""));
    let far = read("far", &shape(&detour));
    assert!(
        near.1.is_empty() && far.1.is_empty(),
        "{:?} {:?}",
        near.1,
        far.1
    );
    let ratio = far.0.peak as f64 / near.0.peak as f64;
    assert!(
        ratio < 1.2,
        "a longer path to the file multiplied the peak by {ratio:.2}: {:?} then {:?}",
        near.0,
        far.0
    );
}

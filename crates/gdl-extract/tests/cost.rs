//! What the reader costs grows with the source it reads, whatever the
//! source's shape: doubling a crate at most about doubles the reader's peak
//! memory, its allocations, and its reads from its lookup structures
//! (`gdl_extract::lookup_reads`, which the `count-reads` feature builds
//! for these tests). Each test reads one shape at two sizes and compares; a
//! cost that multiplies two lengths quadruples instead. The shapes are the
//! ones that once did: a `use` that copied its attributes and its path to
//! every name it binds, modules that copied their parent's path, and a
//! wire check that walked every impl of the crate per message. Each cost
//! is a count of work, never a time, so a busy host cannot fail a test.
//! Its own test binary, so that the counting allocator sees only it.

// A counting global allocator needs `unsafe`; it only forwards to `System`.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use gdl_extract::{Cargo, Extraction, Workspace};

/// The system allocator, counting per thread live bytes, their peak,
/// the allocation count, and total allocated bytes. Per thread, so tests that run at once
/// do not count each other.
struct Counting;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
    static ALLOCATED: Cell<u64> = const { Cell::new(0) };
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
        let _ = ALLOCATED.try_with(|total| total.set(total.get() + layout.size() as u64));
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
        let _ = ALLOCATED.try_with(|total| total.set(total.get() + new_size as u64));
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What one extraction cost: peak live bytes, the allocation count and
/// volume, and its reads from its lookup structures. Each is a count of
/// work, not a time, so a busy host cannot change it.
#[derive(Debug, Clone, Copy)]
struct Cost {
    peak: isize,
    allocations: u64,
    allocated: u64,
    reads: u64,
}

/// Write a crate of these files, load it, and read it on a thread of its
/// own: with a large stack, as the reader recurses with module depth, and
/// bounded in time, so a reader that runs away fails the test instead of
/// hanging it. Returns the cost of the extraction alone and its problems.
fn read(name: &str, files: &[(String, String)]) -> (Cost, Vec<String>) {
    read_beside(name, files, None)
}

/// [`read`], with a path dependency `dep`, named `dep`, of these files.
fn read_beside(
    name: &str,
    files: &[(String, String)],
    dep: Option<&[(String, String)]>,
) -> (Cost, Vec<String>) {
    let (cost, extraction) = extract_beside(name, files, dep);
    (cost, extraction.problems)
}

/// [`read_beside`], with the whole extraction: what the crate offers, too.
fn extract_beside(
    name: &str,
    files: &[(String, String)],
    dep: Option<&[(String, String)]>,
) -> (Cost, Extraction) {
    let temp = tempfile::Builder::new()
        .prefix(&format!("gdl-cost-{name}-"))
        .tempdir()
        .expect("dir");
    let write = |dir: &std::path::Path, name: &str, files: &[(String, String)], deps: &str| {
        std::fs::create_dir_all(dir.join("src")).expect("dir");
        std::fs::write(
            dir.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n{deps}"
            ),
        )
        .expect("manifest");
        for (path, source) in files {
            let path = dir.join("src").join(path);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("dir");
            std::fs::write(path, source).expect("source");
        }
    };
    let dir = temp.path().join("crate");
    match dep {
        Some(dep) => {
            write(&temp.path().join("dep"), "dep", dep, "");
            write(
                &dir,
                name,
                files,
                "[dependencies]\ndep = { path = \"../dep\" }\n",
            );
        }
        None => write(&dir, name, files, ""),
    }
    let workspace = Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted()).expect("metadata");
    let (done, finished) = std::sync::mpsc::channel();
    let package = name.to_string();
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(move || {
            let extraction = workspace.extract(&package).expect("the package");
            let cost = Cost {
                peak: PEAK.with(Cell::get),
                allocations: COUNT.with(Cell::get),
                allocated: ALLOCATED.with(Cell::get),
                reads: gdl_extract::lookup_reads(),
            };
            let _ = done.send((cost, extraction));
        })
        .expect("thread");
    finished
        .recv_timeout(Duration::from_secs(60))
        .expect("the reader finished")
}

/// Read a shape at size `n` and at `2n`, and assert that neither the peak
/// memory, nor the allocations, nor the lookup reads grew by more
/// than three times: linear growth doubles them, a product of two lengths
/// quadruples them. The reads witness work that allocates nothing, such
/// as a scan of a module per name.
fn grows_linearly(name: &str, shape: impl Fn(usize) -> Vec<(String, String)>, n: usize) {
    let (small, problems) = read(&format!("{name}-small"), &shape(n));
    assert!(problems.is_empty(), "{problems:?}");
    let (large, problems) = read(&format!("{name}-large"), &shape(2 * n));
    assert!(problems.is_empty(), "{problems:?}");
    // No reads means the `count-reads` feature is off, not a free reader.
    assert!(small.reads > 0, "{name}: no lookup was read: {small:?}");
    let memory = large.peak as f64 / small.peak as f64;
    let work = large.allocations as f64 / small.allocations as f64;
    let reads = large.reads as f64 / small.reads as f64;
    assert!(
        memory < 3.0 && work < 3.0 && reads < 3.0,
        "{name}: doubling the source multiplied the peak memory by {memory:.1}, the \
         allocations by {work:.1}, and the lookup reads by {reads:.1}\n  n:  {small:?}\n  2n: {large:?}"
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

/// A chain of the crate's own generic message, each level an alias of the
/// one below (`L1 = Carry<L0>`, `L2 = Carry<L1>`, ...): the wire check
/// judged each argument once per serde direction, so each level doubled
/// the work while the type grew by one. 24 levels took minutes. The chain
/// is read at 8 and 16 levels first, where work that doubles per level
/// grows about 256 times and fails the test in seconds; then at 24. The
/// export at the top must still read as a wire type, so that the check
/// walks the whole chain.
#[test]
fn a_nested_instantiation_is_judged_once_per_level() {
    let chain = |n: usize| {
        let mut source = String::from(
            "use serde::{Deserialize, Serialize};\n\
             #[derive(Serialize, Deserialize, gdl_message::Message)]\n\
             pub struct Carry<T> {\n    pub value: T,\n}\n\
             pub type L0 = u8;\n",
        );
        for level in 1..=n {
            source.push_str(&format!("pub type L{level} = Carry<L{}>;\n", level - 1));
        }
        source.push_str(&format!("#[gdl_message::export]\npub type Deep = L{n};\n"));
        vec![("lib.rs".to_string(), source)]
    };
    grows_linearly("nested", chain, 8);
    let (_, extraction) = extract_beside("nested-deep", &chain(24), None);
    assert!(extraction.problems.is_empty(), "{:?}", extraction.problems);
    let wire: Vec<(&str, bool)> = extraction
        .offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.wire))
        .collect();
    assert_eq!(wire, [("Deep", true)]);
}

/// Many nodes whose ports carry one instantiation of a generic message of
/// many fields: each port ran the wire check, which read the serde
/// attributes of every field of the message to find what it bounds, so
/// the work grew with ports times fields. A port needs only the message's
/// identity. The message is generic, since the check reads no fields of a
/// type without arguments.
#[test]
fn a_port_does_not_read_its_messages_fields() {
    grows_linearly(
        "ports",
        |n| {
            let mut source = String::from(
                "use serde::{Deserialize, Serialize};\n\
                 #[derive(Serialize, Deserialize, gdl_message::Message)]\npub struct Big<T> {\n",
            );
            for i in 0..n {
                source.push_str(&format!(
                    "    #[serde(rename = \"x{i}\")]\n    pub f{i}: T,\n"
                ));
            }
            source.push_str("}\n");
            for i in 0..n {
                source.push_str(&format!(
                    "#[derive(gdl::Node)]\npub struct N{i};\n\
                     impl gdl::Forward for N{i} {{ type In = Big<u8>; type Out = Big<u8>; }}\n"
                ));
            }
            vec![("lib.rs".into(), source)]
        },
        300,
    );
}

/// A serde impl for an alias of a type whose argument a dependency
/// defines, beside a message whose wire check builds the crate's index of
/// serde impls: indexing the impl read the alias whole, and so read the
/// dependency, which nothing else names. Reading the crate must not cost
/// more as the dependency grows.
#[test]
fn indexing_an_impl_for_an_alias_does_not_read_its_arguments_crates() {
    let source: Vec<(String, String)> = vec![(
        "lib.rs".into(),
        "use serde::{Deserialize, Serialize};\n\
         pub struct Wrapper<T>(pub T);\n\
         pub type Unrelated = Wrapper<dep::Payload>;\n\
         impl Serialize for Unrelated {}\n\
         #[derive(gdl_message::Message)]\npub struct Words;\n\
         impl Serialize for Words {}\n\
         impl<'de> Deserialize<'de> for Words {}\n"
            .into(),
    )];
    let dep = |n: usize| {
        let mut files = vec![("lib.rs".to_string(), String::from("pub struct Payload;\n"))];
        for i in 0..n {
            files[0].1.push_str(&format!("pub mod m{i};\n"));
            let structs: String = (0..50).map(|j| format!("pub struct S{j};\n")).collect();
            files.push((format!("m{i}.rs"), structs));
        }
        files
    };
    let (small, problems) = read_beside("alias-small", &source, Some(&dep(50)));
    assert!(problems.is_empty(), "{problems:?}");
    let (large, problems) = read_beside("alias-large", &source, Some(&dep(400)));
    assert!(problems.is_empty(), "{problems:?}");
    let work = large.allocations as f64 / small.allocations as f64;
    assert!(
        work < 1.5,
        "an eightfold dependency multiplied the allocations by {work:.1}\n  small: {small:?}\n  large: {large:?}"
    );
}

/// Many names bound by `use`s carrying an attribute of a single name: the
/// export check looked each such attribute up among every `use` of the
/// module, so the work grew with the square of the names.
#[test]
fn the_export_check_does_not_scan_the_module_per_name() {
    grows_linearly(
        "scan",
        |n| {
            let mut source = String::from("pub mod inner {}\n");
            for i in 0..n {
                source.push_str(&format!("#[doc = \"d\"]\npub use self::inner::n{i};\n"));
            }
            vec![("lib.rs".to_string(), source)]
        },
        2000,
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
/// scanning its module's types, so the work grew with types times impls.
#[test]
fn a_type_is_found_by_its_name() {
    grows_linearly(
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
        2000,
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

#[test]
fn the_policy_scan_does_not_expand_a_shared_import_prefix_per_name() {
    let shape = |n: usize| {
        let path: Vec<String> = (0..n).map(|i| format!("p{i}")).collect();
        let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
        vec![(
            "lib.rs".into(),
            format!("pub use {}::{{{}}};\n", path.join("::"), names.join(", ")),
        )]
    };
    let (small, problems) = read("policy-prefix-small", &shape(2048));
    assert!(problems.is_empty(), "{problems:?}");
    let (large, problems) = read("policy-prefix-large", &shape(4096));
    assert!(problems.is_empty(), "{problems:?}");
    let ratio = large.allocated as f64 / small.allocated as f64;
    eprintln!("shared import prefix: n {small:?}; 2n {large:?}; allocated {ratio:.2}x");
    assert!(
        ratio < 3.0,
        "doubling a shared import prefix and its names multiplies allocated bytes by {ratio:.2}:\n  n: {small:?}\n  2n: {large:?}"
    );
}

/// The role index reads this impl head without matching its arguments.
/// Alias depth and argument size must not multiply its peak memory.
#[test]
fn alias_walk_releases_intermediate_arguments() {
    let shape = |n: usize| {
        let payload = std::iter::repeat_n("u8", 128 * n)
            .collect::<Vec<_>>()
            .join(", ");
        let nested = format!(
            "{}Wrapper<({payload},)>{}",
            "Identity<".repeat(n),
            ">".repeat(n)
        );
        let source = format!(
            "pub type Identity<T> = T;\npub struct Wrapper<T>(T);\n\
             #[derive(gdl_message::Message, serde::Serialize, serde::Deserialize)]\npub struct Words;\n\
             #[derive(gdl::Node)]\npub struct Trigger;\n\
             impl gdl::Sink for Trigger {{ type Req = Words; type Item = Words; }}\n\
             impl gdl::Sink for {nested} {{ type Req = Words; type Item = Words; }}\n"
        );
        vec![("lib.rs".to_string(), source)]
    };
    let (small, problems) = read("alias-peak-small", &shape(8));
    assert!(problems.is_empty(), "{problems:?}");
    let (large, problems) = read("alias-peak-large", &shape(16));
    assert!(problems.is_empty(), "{problems:?}");
    let ratio = large.peak as f64 / small.peak as f64;
    eprintln!("alias peak: n {small:?}; 2n {large:?}; peak {ratio:.2}x");
    assert!(
        ratio < 3.0,
        "doubling alias depth and argument size multiplies peak memory by {ratio:.2}: {small:?} then {large:?}"
    );
}

/// Reusing an alias body must not keep a copy live for every visit.
#[test]
fn alias_head_releases_repeated_targets() {
    let shape = |depth: usize| {
        let width = 64;
        let mut source: String = (0..width).map(|i| format!("pub mod m{i} {{\n")).collect();
        source.push_str("pub type Identity<T> = T;\n");
        source.push_str(&"}\n".repeat(width));
        let path: String = (0..width).map(|i| format!("m{i}::")).collect();
        source.push_str(&format!(
            "pub type Hop<T> = {path}Identity<T>;\npub struct Wrapper<T>(T);\n\
             #[derive(gdl_message::Message, serde::Serialize, serde::Deserialize)]\npub struct Words;\n\
             #[derive(gdl::Node)]\npub struct Trigger;\n\
             impl gdl::Sink for Trigger {{ type Req = Words; type Item = Words; }}\n"
        ));
        let nested = format!("{}Wrapper<u8>{}", "Hop<".repeat(depth), ">".repeat(depth));
        source.push_str(&format!(
            "impl gdl::Sink for {nested} {{ type Req = Words; type Item = Words; }}\n"
        ));
        vec![("lib.rs".to_string(), source)]
    };
    let (once, problems) = read("alias-target-once", &shape(1));
    assert!(problems.is_empty(), "{problems:?}");
    let (repeated, problems) = read("alias-target-repeated", &shape(16));
    assert!(problems.is_empty(), "{problems:?}");
    let ratio = repeated.peak as f64 / once.peak as f64;
    eprintln!("alias target reuse: once {once:?}; repeated {repeated:?}; peak {ratio:.2}x");
    assert!(
        ratio < 1.2,
        "reusing an alias target multiplies peak memory by {ratio:.2}: once {once:?}; repeated {repeated:?}"
    );
}

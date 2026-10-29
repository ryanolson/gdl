//! Generic messages and nodes, end to end: a generic carrier and a generic
//! forward node, each written once and instantiated twice by aliases, read
//! from source as their builds register them; a graph that names one
//! instantiation of each by its alias checks, generates, and builds; and
//! the carrier crosses between its two segments.

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use common::{Running, cargo, generated, root, target};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/generic")
}

fn extracted(name: &str) -> gdl_gen::Extracted {
    let krate = gdl_gen::Crate::Path(fixture().join(name));
    gdl_gen::extract(
        &krate,
        &target("gdl-gen-tests"),
        &gdl_extract::Cargo::trusted(),
    )
    .expect("extracted")
}

fn default_capabilities(source: &str) -> Vec<(String, bool, bool)> {
    let temp = tempfile::Builder::new()
        .prefix("gdl-default-projection-")
        .tempdir()
        .expect("scratch");
    std::fs::create_dir(temp.path().join("src")).expect("source directory");
    std::fs::write(
        temp.path().join("Cargo.toml"),
        format!(
            "[package]\nname = \"default-projection\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\
             [workspace]\n[dependencies]\n\
             gdl-message = {{ path = {:?} }}\n\
             serde = {{ version = \"1\", features = [\"derive\"] }}\n",
            root().join("crates/gdl-message")
        ),
    )
    .expect("manifest");
    std::fs::write(
        temp.path().join("src/lib.rs"),
        format!(
            "use std::marker::PhantomData;\n\
             use serde::{{Serialize, Deserialize}};\n\
             #[derive(Serialize, Deserialize, gdl_message::Message)]\n\
             pub struct Hidden<T> {{ #[serde(skip)] pub value: T }}\n{source}"
        ),
    )
    .expect("source");
    let target = target("gdl-gen-tests");
    let mut read = gdl_gen::extract(
        &gdl_gen::Crate::Path(temp.path().to_path_buf()),
        &target,
        &gdl_extract::Cargo::trusted(),
    )
    .expect("extracted");
    let notes = gdl_gen::probe(&mut read, &target).expect("the fixture builds");
    assert!(notes.is_empty(), "{notes:?}");
    read.offer
        .exports
        .into_iter()
        .map(|export| {
            (
                export.name,
                export.wire,
                export.probed.expect("the build probes the instantiation"),
            )
        })
        .collect()
}

#[test]
fn named_function_parameters_preserve_default_projection_bounds() {
    let capabilities = default_capabilities(
        r#"
pub trait Has { type Out; }
pub struct Bare;
#[derive(Default)]
pub struct Key;
impl Has for Key { type Out = Bare; }

#[derive(Default)]
pub struct Named<T: Has> { pub value: PhantomData<fn(value: T::Out)> }
#[derive(Default)]
pub struct Unnamed<T: Has> { pub value: PhantomData<fn(T::Out)> }
#[derive(Default)]
pub struct Concrete<T> {
    pub marker: PhantomData<T>,
    pub value: PhantomData<fn(T: u8)>,
}

#[gdl_message::export]
pub type HiddenNamed = Hidden<Named<Key>>;
#[gdl_message::export]
pub type HiddenUnnamed = Hidden<Unnamed<Key>>;
#[gdl_message::export]
pub type HiddenConcrete = Hidden<Concrete<u8>>;
"#,
    );
    assert_eq!(
        capabilities,
        [
            ("HiddenConcrete".into(), true, true),
            ("HiddenNamed".into(), false, false),
            ("HiddenUnnamed".into(), false, false),
        ],
        "(export, reader wire capability, build wire capability)"
    );
}

#[test]
fn a_const_inequality_is_not_a_type_macro() {
    let capabilities = default_capabilities(
        r#"
const SIZE: usize = 4;

#[derive(Default)]
pub struct Compared<T> {
    pub marker: PhantomData<T>,
    pub bytes: [u8; (SIZE != 0) as usize],
}
#[derive(Default)]
pub struct Equal<T> {
    pub marker: PhantomData<T>,
    pub bytes: [u8; (SIZE == 0) as usize],
}

#[gdl_message::export]
pub type HiddenCompared = Hidden<Compared<u8>>;
#[gdl_message::export]
pub type HiddenEqual = Hidden<Equal<u8>>;
"#,
    );
    assert_eq!(
        capabilities,
        [
            ("HiddenCompared".into(), true, true),
            ("HiddenEqual".into(), true, true),
        ],
        "(export, reader wire capability, build wire capability)"
    );
}

#[test]
fn a_concrete_qualified_self_type_does_not_project_a_parameter() {
    let capabilities = default_capabilities(
        r#"
pub trait Has { type Out; }
pub mod model { pub struct Value; }
impl Has for model::Value { type Out = u8; }

#[derive(Default)]
pub struct Qualified<Value> {
    pub marker: PhantomData<Value>,
    pub value: PhantomData<<model::Value as Has>::Out>,
}
#[derive(Default)]
pub struct Plain<Value> {
    pub marker: PhantomData<Value>,
    pub value: PhantomData<u8>,
}

#[gdl_message::export]
pub type HiddenQualified = Hidden<Qualified<u8>>;
#[gdl_message::export]
pub type HiddenPlain = Hidden<Plain<u8>>;
"#,
    );
    assert_eq!(
        capabilities,
        [
            ("HiddenPlain".into(), true, true),
            ("HiddenQualified".into(), true, true),
        ],
        "(export, reader wire capability, build wire capability)"
    );
}

#[test]
fn an_associated_binding_argument_preserves_default_projection_bounds() {
    let capabilities = default_capabilities(
        r#"
pub trait Has { type Out; }
pub struct Bare;
#[derive(Default)]
pub struct Key;
impl Has for Key { type Out = Bare; }
pub trait Gat { type Item<A> where Self: Sized; }

#[derive(Default)]
pub struct Projected<T: Has> {
    pub marker: PhantomData<T>,
    pub value: PhantomData<dyn Gat<Item<T::Out> = u8> + Send>,
}
#[derive(Default)]
pub struct Concrete<T> {
    pub marker: PhantomData<T>,
    pub value: PhantomData<dyn Gat<Item<u8> = u8> + Send>,
}

#[gdl_message::export]
pub type HiddenProjected = Hidden<Projected<Key>>;
#[gdl_message::export]
pub type HiddenConcrete = Hidden<Concrete<Key>>;
"#,
    );
    assert_eq!(
        capabilities,
        [
            ("HiddenConcrete".into(), true, true),
            ("HiddenProjected".into(), false, false),
        ],
        "(export, reader wire capability, build wire capability)"
    );
}

/// What the reader finds agrees with what each crate's build registers,
/// and each alias offers its instantiation: the two stamps carry the two
/// carriers, by type, and the concrete sink's port, written as the alias,
/// carries the plain one.
#[test]
fn the_instantiations_read_as_their_builds_register_them() {
    let carrier = extracted("carrier");
    let nodes = extracted("nodes");
    for crate_read in [&carrier, &nodes] {
        gdl_gen::verify(crate_read, &target("gdl-gen-tests")).expect("the build agrees");
    }
    let export = |name: &str| {
        carrier
            .offer
            .exports
            .iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("{name}"))
    };
    let (plain, loud) = (export("PlainCarried"), export("LoudCarried"));
    assert_eq!(
        plain.origin.path,
        "generic_carrier::Carried<generic_carrier::Prompt, generic_carrier::Plain>"
    );
    assert_eq!(plain.encoding, gdl_model::Encoding::Json);
    assert!(plain.wire && loud.wire);
    assert!(carrier.offer.messages.iter().all(|m| m.name != "Carried"));
    let ports = |name: &str| -> Vec<Option<gdl_extract::Origin>> {
        nodes
            .offer
            .nodes
            .iter()
            .find(|n| n.name == name)
            .unwrap_or_else(|| panic!("{name}"))
            .ports
            .iter()
            .map(|p| p.head.clone())
            .collect()
    };
    assert_eq!(
        ports("PlainStamp"),
        [Some(plain.origin.clone()), Some(plain.origin.clone())]
    );
    assert_eq!(
        ports("LoudStamp"),
        [Some(loud.origin.clone()), Some(loud.origin.clone())]
    );
    assert_eq!(ports("Echo")[0], Some(plain.origin.clone()));
    assert!(nodes.offer.nodes.iter().all(|n| n.name != "Stamp"));
}

/// A serde impl written for one instantiation makes that one a wire type,
/// and no other; and an argument must be a wire type only for a parameter
/// serde's derive bounds, which `PhantomData` and a field's or the
/// container's own `#[serde(bound)]` change; a field's own bound holds
/// under the container's, and though serde skips the field; a bound in
/// one direction leaves serde's inference in the other; a field serde
/// fills with its default bounds its parameters by `Default`; a variant's
/// own skip and bound hold for its fields; a direction written by hand
/// bounds what its impl bounds, not what serde would infer, and counts
/// only if it holds for every input lifetime, not `'static` alone; and an enum's
/// derived `Default` bounds no argument, where a struct's bounds each. The reader agrees with what
/// the build probes for each, including an instantiation that is
/// another's argument.
#[test]
fn an_instantiation_is_a_wire_type_as_serde_bounds_it() {
    let wire = extracted("wire");
    gdl_gen::verify(&wire, &target("gdl-gen-tests")).expect("the build agrees");
    let messages: Vec<(&str, bool)> = wire
        .offer
        .messages
        .iter()
        .map(|m| (m.name.as_str(), m.wire))
        .collect();
    assert_eq!(
        messages,
        [("Globbed", true), ("Handed", true), ("ModelName", true)],
        "messages that are not generic read as they did before generic ones"
    );
    let capability: Vec<(&str, bool)> = wire
        .offer
        .exports
        .iter()
        .map(|e| (e.name.as_str(), e.wire))
        .collect();
    assert_eq!(
        capability,
        [
            ("BodyString", true),
            ("ByteAnchored", true),
            ("ByteBoxed", true),
            ("ByteChosenDe", true),
            ("ByteCrossed", false),
            ("ByteFixed", false),
            ("ByteHalf", true),
            ("ByteNarrow", true),
            ("ByteOuter", true),
            ("BytePinned", true),
            ("ByteSkipped", true),
            ("ByteWrapped", true),
            ("CarryBodies", true),
            ("CarryMap", true),
            ("CarryMaybeOpaque", false),
            ("CarryOpaques", false),
            ("CarryString", true),
            ("ChosenOpaque", false),
            ("CopiedShared", false),
            ("DefaultedBare", false),
            ("DefaultedByte", true),
            ("DefaultedManual", true),
            ("GatedBoundByte", false),
            ("GatedByte", false),
            ("HiddenBare", false),
            ("HiddenBoxed", false),
            ("HiddenByte", true),
            ("HiddenDefaultProjected", false),
            ("HiddenFallbackByte", true),
            ("HiddenFallbackOpaque", false),
            ("HiddenMaybe", true),
            ("HiddenOpaques", true),
            ("HiddenOptional", false),
            ("HoldsName", true),
            ("LocalDefault", false),
            ("LocalSerde", false),
            ("OpaqueChoice", true),
            ("OpaqueChosen", true),
            ("OpaqueChosenDe", false),
            ("OpaqueHalf", false),
            ("OpaqueHandmade", true),
            ("OpaqueHeld", true),
            ("OpaqueHeldDe", false),
            ("OpaqueMarked", true),
            ("OpaquePinned", false),
            ("OpaqueSkipped", false),
            ("OpaqueVariant", true),
            ("OpaqueWrapped", false),
            ("OuterRemote", false),
            ("ProjectedOpaque", false),
            ("SyncedShared", false),
            ("TailBoxed", false),
            ("WholeByte", true),
            ("WholeOpaque", false),
            ("WordCrossed", false),
            ("WordNarrow", false),
            ("WordOuter", false),
        ]
    );
}

/// An impl written for an alias is the impl for the type it names: a role
/// impl through the alias of an instantiation, through a generic alias,
/// through an alias that names its argument (`Identity<Kept<Plain>>`), or
/// through the alias of a node that is not generic, finds its node as one
/// written out does; and serde impls through an alias make that
/// instantiation a wire type. The reader agrees with the build.
#[test]
fn an_impl_through_an_alias_is_the_impl_written_out() {
    let aliased = extracted("aliased");
    gdl_gen::verify(&aliased, &target("gdl-gen-tests")).expect("the build agrees");
    let nodes: Vec<String> = aliased
        .offer
        .nodes
        .iter()
        .map(|n| {
            let ports: Vec<String> = n
                .ports
                .iter()
                .map(|p| format!("{}={}", p.port, p.head.as_ref().expect("a head").path))
                .collect();
            format!("{} {}: {}", n.role, n.name, ports.join(", "))
        })
        .collect();
    let carried = |builder: &str| format!("generic_aliased::Carried<generic_aliased::{builder}>");
    let words = "generic_aliased::Words";
    assert_eq!(
        nodes,
        [
            format!("sink LoudKept: Req={}, Item={words}", carried("Loud")),
            format!("sink LoudStore: Req={}, Item={words}", carried("Loud")),
            format!("sink PlainKept: Req={}, Item={words}", carried("Plain")),
            format!(
                "forward PlainRelay: In={}, Out={}",
                carried("Plain"),
                carried("Plain")
            ),
            format!("sink PlainStore: Req={}, Item={words}", carried("Plain")),
            format!("sink Tally: Req={words}, Item={words}"),
        ]
    );
    let narrow = aliased
        .offer
        .exports
        .iter()
        .find(|e| e.name == "ByteNarrow")
        .expect("ByteNarrow");
    assert!(narrow.wire);
}

/// `()` for a parameter that a port names is no message at that port, as
/// `()` written at the port is: a forward of `()`, and a pure stream whose
/// head is the parameter; and a `Streamed` bound to the parameter a port
/// names is a streamed port. The reader agrees with the build.
#[test]
fn unit_for_a_ports_parameter_is_no_message() {
    let unit = extracted("unit");
    gdl_gen::verify(&unit, &target("gdl-gen-tests")).expect("the build agrees");
    let nodes: Vec<String> = unit
        .offer
        .nodes
        .iter()
        .map(|n| {
            let side = |o: &Option<gdl_extract::Origin>| {
                o.as_ref().map_or("()".to_string(), |o| o.path.clone())
            };
            let ports: Vec<String> = n
                .ports
                .iter()
                .map(|p| match &p.item {
                    Some(item) => format!("{}={}+{}", p.port, side(&p.head), item.path),
                    None => format!("{}={}", p.port, side(&p.head)),
                })
                .collect();
            format!("{} {}: {}", n.role, n.name, ports.join(", "))
        })
        .collect();
    let words = "generic_unit::Words";
    assert_eq!(
        nodes,
        [
            format!("sink HeadedFeed: Req={words}+{words}, Item={words}"),
            format!("sink HeadlessFeed: Req=()+{words}, Item={words}"),
            format!("sink StreamFed: Req=()+{words}, Item={words}"),
            "forward UnitPass: In=(), Out=()".to_string(),
            format!("forward WordsPass: In={words}, Out={words}"),
        ]
    );
}

/// A generic node whose `config` and `metrics` are its parameters: each
/// alias's instantiation has its arguments' fields, as the build
/// registers them, and `()` for the metrics is none. A node that is not
/// generic reads its settings' fields whatever their arguments, a default
/// or one the reader does not name.
#[test]
fn a_nodes_config_and_metrics_may_be_its_parameters() {
    let configured = extracted("configured");
    gdl_gen::verify(&configured, &target("gdl-gen-tests")).expect("the build agrees");
    let fields: Vec<(&str, Vec<&str>, Vec<&str>)> = configured
        .offer
        .nodes
        .iter()
        .map(|n| {
            (
                n.name.as_str(),
                n.config.iter().map(|f| f.name.as_str()).collect(),
                n.metrics.iter().map(|f| f.name.as_str()).collect(),
            )
        })
        .collect();
    assert_eq!(
        fields,
        [
            ("Echo", vec!["enabled"], vec![]),
            ("FastStage", vec!["batch"], vec!["batched_total"]),
            ("QuietStage", vec!["batch"], vec![]),
            ("Shared", vec!["enabled"], vec![]),
            ("SlowStage", vec!["patient"], vec!["waited_total"]),
        ]
    );
}

/// A generic service whose clients are its parameter: each alias's
/// instantiation hosts its own argument's endpoints, as the build
/// registers them.
#[test]
fn a_services_clients_may_be_its_parameter() {
    let service = extracted("service");
    gdl_gen::verify(&service, &target("gdl-gen-tests")).expect("the build agrees");
    let hosted: Vec<String> = service
        .offer
        .nodes
        .iter()
        .map(|n| {
            let head = |p: &gdl_extract::PortEntry| p.head.as_ref().expect("a head").path.clone();
            let entries: Vec<String> = n
                .entries
                .iter()
                .map(|e| {
                    format!(
                        "{} {} {} -> {}",
                        e.name,
                        e.path,
                        head(&e.request),
                        head(&e.response)
                    )
                })
                .collect();
            format!(
                "{} {}: {}",
                n.name,
                n.clients.as_deref().unwrap_or("-"),
                entries.join(", ")
            )
        })
        .collect();
    assert_eq!(
        hosted,
        [
            "AskServer generic_service::AskClients: ask /ask generic_service::Ask -> generic_service::Answer",
            "NoteServer generic_service::NoteClients: note /note generic_service::Note -> generic_service::Answer",
        ]
    );
}

/// Generate the spec `spec` of the fixture, and build the libraries of the
/// generated crates `members`, each to its end: the build's output.
fn build_segments(spec: &str, members: &[&str]) -> std::process::Output {
    let spec_file = fixture().join(spec);
    let temp = tempfile::Builder::new()
        .prefix("gdl-generic-build-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let spec = gdl_gen::Spec::load(&spec_file).expect("spec");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    let members: Vec<String> = members.iter().map(|m| format!("\"{m}\"")).collect();
    std::fs::write(
        out.join("Cargo.toml"),
        format!(
            "[workspace]\nmembers = [{}]\nresolver = \"3\"\n",
            members.join(", ")
        ),
    )
    .expect("workspace");
    // Resolved as gdl's own workspace is: its lock pins every crate the
    // runtime builds, so the build reads no registry and reuses artifacts.
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
    cargo()
        .arg("build")
        .arg("--manifest-path")
        .arg(out.join("Cargo.toml"))
        .arg("--lib")
        .arg("--keep-going")
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("cargo build")
}

/// A response the reader takes for a wire type, which serializes but does
/// not deserialize, and which an anchor carries back to the source in the
/// source's own segment: the segment that serves the source writes it as
/// JSON, which needs only `Serialize`, and its anchor check still needs a
/// wire type, and refuses it, once, saying that it returns through an
/// anchor, where the deployment would have refused it at start.
#[test]
fn a_misread_response_an_anchor_carries_fails_to_build_by_name() {
    let anchored = extracted("anchored");
    let export = anchored
        .offer
        .exports
        .iter()
        .find(|e| e.name == "ByteReply")
        .expect("ByteReply");
    assert!(export.wire, "the reader takes it for a wire type");
    let build = build_segments("anchored.toml", &["contract", "front", "relay"]);
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(!build.status.success(), "the build passed:\n{stderr}");
    assert!(
        stderr.contains("could not compile `anchored-front` (lib) due to 1 previous error"),
        "{stderr}"
    );
    assert!(
        !stderr.contains("could not compile `anchored-relay`"),
        "{stderr}"
    );
    assert!(
        stderr.contains("`Reply<u8>` returns through an anchor but is not a wire type"),
        "{stderr}"
    );
    assert!(
        stderr.contains("or place the whole hop in one segment"),
        "the advice fits the case:\n{stderr}"
    );
    assert!(!stderr.contains("crosses between segments"), "{stderr}");
    assert!(
        stderr.contains("anchors::<Take_to_Front>()"),
        "the edge is named:\n{stderr}"
    );
}

/// A carrier whose "serde" impls are another crate's traits under serde's
/// name, a trick the source reader does not chase because the segment's crossing check catches it, reads as
/// a wire type, so the generator lets it cross a cut; each segment's
/// crossing check then refuses it at build time, naming the message and
/// the crossing, where the deployment would have refused it at start.
///
/// It builds the generated segments: the first run takes as long as the
/// chat example's build, whose target directory it shares.
#[test]
fn a_crossing_the_reader_misreads_fails_to_build_by_name() {
    let misread = extracted("misread");
    let export = misread
        .offer
        .exports
        .iter()
        .find(|e| e.name == "MisreadByte")
        .expect("MisreadByte");
    assert!(export.wire, "the reader takes it for a wire type");

    let build = build_segments("misread.toml", &["back", "contract", "front"]);
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(!build.status.success(), "the build passed:\n{stderr}");
    // One error in each segment, for the one type, from its crossing
    // check: the sending segment's binary also reads it as JSON, which the
    // crossing check bounds already.
    for segment in ["misread-front", "misread-back"] {
        assert!(
            stderr.contains(&format!(
                "could not compile `{segment}` (lib) due to 1 previous error"
            )),
            "{segment}:\n{stderr}"
        );
    }
    assert!(
        !stderr.contains("which this segment's binary reads as JSON lines"),
        "{stderr}"
    );
    assert!(
        stderr.contains(
            "`generic_misread::Misread<u8>` crosses between segments but is not a wire type"
        ),
        "{stderr}"
    );
    assert!(
        stderr.contains("crosses::<Front_to_Take>()"),
        "the crossing is named:\n{stderr}"
    );
}

/// The same carrier as the source's request in a segment of its own, where
/// it crosses no cut: the segment's binary reads it as JSON lines, and the
/// segment asserts that it can, once, where the binary would have refused
/// every request at run time.
#[test]
fn a_misread_request_no_cut_carries_fails_to_build_by_name() {
    let build = build_segments("misread-alone.toml", &["contract", "front"]);
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(!build.status.success(), "the build passed:\n{stderr}");
    assert!(
        stderr.contains("could not compile `misread-front` (lib) due to 1 previous error"),
        "{stderr}"
    );
    assert!(
        stderr.contains(
            "is the source's request, which this segment's binary reads as JSON lines, but is not a wire type"
        ),
        "{stderr}"
    );
    assert!(
        stderr.contains("reads_json::<contract::SourceReq>()"),
        "the assertion is named:\n{stderr}"
    );
}

/// A response item the reader misreads, in a segment that holds the source
/// and the sink: no cut carries it, the segment's binary writes it as JSON
/// lines, and the segment asserts that it can, once.
#[test]
fn a_misread_item_no_cut_carries_fails_to_build_by_name() {
    let build = build_segments("mute.toml", &["contract", "front"]);
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(!build.status.success(), "the build passed:\n{stderr}");
    assert!(
        stderr.contains("could not compile `mute-front` (lib) due to 1 previous error"),
        "{stderr}"
    );
    assert!(
        stderr.contains(
            "is the source's response item, which this segment's binary writes as JSON lines, but is not a wire type"
        ),
        "{stderr}"
    );
    assert!(
        stderr.contains("writes_json::<contract::SourceItem>()"),
        "the assertion is named:\n{stderr}"
    );
}

/// `cargo gdl extract --verify --catalog` adds both crates, and the
/// catalogue's index shows each instantiation under its alias.
#[test]
fn the_catalogue_lists_each_instantiation_by_its_alias() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-generic-catalog-")
        .tempdir()
        .expect("scratch");
    let catalogue = temp.path().join("catalogue");
    let gdl = |args: &[&std::ffi::OsStr]| {
        let output = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .arg("gdl")
            .args(args)
            .env("CARGO_TARGET_DIR", target("gdl-gen-tests"))
            .output()
            .expect("cargo gdl");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("utf-8")
    };
    for name in ["carrier", "nodes"] {
        gdl(&[
            "extract".as_ref(),
            fixture().join(name).as_os_str(),
            "--verify".as_ref(),
            "--catalog".as_ref(),
            catalogue.as_os_str(),
        ]);
    }
    let index = gdl(&["catalog".as_ref(), catalogue.as_os_str()]);
    let plain = "generic_carrier::Carried<generic_carrier::Prompt, generic_carrier::Plain>";
    for line in [
        format!("  PlainCarried: {plain} (generic-carrier 0.0.0)"),
        "  PlainStamp: forward generic_nodes::PlainStamp (generic-nodes@0.0.0)".to_string(),
        "  LoudStamp: forward generic_nodes::LoudStamp (generic-nodes@0.0.0)".to_string(),
        format!("    In: {plain} (generic-carrier 0.0.0)"),
    ] {
        assert!(index.contains(&line), "{line:?} in:\n{index}");
    }
}

/// The spec checks; the generated contract names each message by its
/// alias; the segments build; and a request crosses from the stamp's
/// segment to the sink's as the carrier, in JSON, and back.
///
/// It builds two segment binaries: the first run takes as long as the
/// chat example's build, whose target directory it shares, and later runs
/// a few seconds.
#[test]
fn the_carrier_crosses_between_two_generated_segments() {
    let spec_file = fixture().join("gdl.toml");
    let problems = gdl_gen::check_files(&spec_file, None, &target("gdl-gen-tests"));
    assert!(problems.is_empty(), "{:?}", problems.0);

    let temp = tempfile::Builder::new()
        .prefix("gdl-generic-")
        .tempdir()
        .expect("scratch");
    let out = temp.path().join("segments");
    let spec = gdl_gen::Spec::load(&spec_file).expect("spec");
    let files = gdl_gen::render(&spec, &out, &target("gdl-gen-tests")).expect("render");
    gdl_gen::write(&out, &files).expect("write");
    let contract = std::fs::read_to_string(out.join("contract/src/lib.rs")).expect("contract");
    assert!(
        contract.contains(
            "forward  PlainStamp: ::generic_carrier::PlainCarried -> ::generic_carrier::PlainCarried;"
        ),
        "{contract}"
    );

    // One workspace for the generated crates.
    std::fs::write(
        out.join("Cargo.toml"),
        "[workspace]\nmembers = [\"back\", \"contract\", \"front\"]\nresolver = \"3\"\n",
    )
    .expect("workspace");
    // gdl's own lock pins every crate the runtime builds.
    std::fs::copy(root().join("Cargo.lock"), out.join("Cargo.lock")).expect("lockfile");
    let build = cargo()
        .arg("build")
        .arg("--manifest-path")
        .arg(out.join("Cargo.toml"))
        .arg("--bins")
        .env("CARGO_TARGET_DIR", target("examples"))
        .output()
        .expect("cargo build");
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );
    let bin = target("examples").join("debug");

    let roster = temp.path().join("roster");
    std::fs::create_dir_all(&roster).expect("roster");
    let config = |name: &str| {
        let path = temp.path().join(format!("{name}.toml"));
        let text = format!(
            "listen = \"127.0.0.1:0\"\nroster = {}\n",
            toml::Value::String(roster.display().to_string())
        );
        std::fs::write(&path, text).expect("config");
        path
    };
    let back = config("back");
    let front = config("front");
    let _back = Running(
        generated(bin.join("generic-back"))
            .arg(&back)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("back"),
    );
    let mut front = Running(
        generated(bin.join("generic-front"))
            .arg(&front)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("front"),
    );
    let mut stdin = front.0.stdin.take().expect("stdin");
    for request in [
        r#"{"payload": {"text": "hi"}, "builder": null}"#,
        r#"{"payload": {"text": "yo"}, "marks": ["pre"], "builder": null}"#,
    ] {
        writeln!(stdin, "{request}").expect("request");
    }
    // End of input: the front answers what it has read, then exits.
    drop(stdin);
    let stdout = front.stdout();
    let lines = stdout.rest(Duration::from_secs(60));
    let status = front.exits_within(Duration::from_secs(10));
    let (tx, rx) = mpsc::channel();
    if let Some(mut err) = front.0.stderr.take() {
        std::thread::spawn(move || {
            let mut text = String::new();
            let _ = std::io::Read::read_to_string(&mut err, &mut text);
            let _ = tx.send(text);
        });
    }
    let stderr = rx.recv_timeout(Duration::from_secs(5)).unwrap_or_default();
    assert!(status.success(), "front failed: {stderr}");
    assert_eq!(
        lines,
        [
            r#"{"item":{"text":"hi [plain!]"}}"#,
            r#"{"done":null}"#,
            r#"{"item":{"text":"yo [pre plain!]"}}"#,
            r#"{"done":null}"#,
        ],
        "front stderr: {stderr}"
    );
}

/// Run the carrier test with a blocked proxy so a cached crate cannot hide a fetch.
#[test]
fn the_carrier_test_runs_with_only_the_documented_cache() {
    let run = Command::new("timeout")
        .arg(common::CARGO_LIMIT)
        .arg(std::env::current_exe().expect("the test binary"))
        .args([
            "the_carrier_crosses_between_two_generated_segments",
            "--exact",
            "--nocapture",
        ])
        .env_remove("CARGO_NET_OFFLINE")
        .env("CARGO_HTTP_PROXY", "http://127.0.0.1:9")
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("https_proxy", "http://127.0.0.1:9")
        .env("CARGO_NET_RETRY", "0")
        .output()
        .expect("the carrier test with the proxy blocked");
    assert!(
        run.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
}

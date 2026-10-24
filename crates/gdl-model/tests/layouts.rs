//! A spec's layouts: one graph placed in more than one way, each placement
//! under `[layout.<name>]`, with its own segments, classes, routing, and
//! cell policies. A spec without `[layout]` tables has one layout, named
//! `default`, read as a placement always was.

use gdl_model::{
    HopInfo, Manifest, NodeInfo, Placement, PlacementErrors, Placements, Problem, Role, TypeInfo,
    check_layout_name, layout,
};

/// `Ask => Preprocess { Truncate => Engine => Detokenize => Format } =>
/// Number` and `Tell => Spare`: two roots, so that one layout can have
/// three classes (each root crosses one boundary at most) and another can
/// make a cycle. The generator's tests read the same graph from
/// `crates/gdl-gen/tests/fixtures/layouts/trio.gdl`.
fn trio() -> Manifest {
    let ty = || TypeInfo::new("T", true, Default::default());
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, forward: &[&str], inner: &str, backward: &[&str]| HopInfo {
        outer: outer.into(),
        forward: forward.iter().map(|&n| n.into()).collect(),
        inner: inner.into(),
        backward: backward.iter().map(|&n| n.into()).collect(),
        forward_types: (0..=forward.len()).map(|_| ty()).collect(),
        backward_types: (0..=backward.len()).map(|_| ty()).collect(),
    };
    Manifest {
        name: "trio".into(),
        schema: 1,
        nodes: vec![
            node("Ask", Role::Source),
            node("Tell", Role::Source),
            node("Preprocess", Role::Operator),
            node("Truncate", Role::Forward),
            node("Engine", Role::Sink),
            node("Spare", Role::Sink),
            node("Detokenize", Role::Backward),
            node("Format", Role::Backward),
            node("Number", Role::Backward),
        ],
        hops: vec![
            hop("Ask", &[], "Preprocess", &["Number"]),
            hop(
                "Preprocess",
                &["Truncate"],
                "Engine",
                &["Detokenize", "Format"],
            ),
            hop("Tell", &[], "Spare", &[]),
        ],
    }
}

const SEGMENTS: &str = r#"
[segment.front]
nodes = ["Ask", "Preprocess", "Truncate", "Detokenize", "Format", "Number"]
[segment.side]
nodes = ["Tell"]
[segment.engine]
nodes = ["Engine", "Spare"]
"#;

/// Three classes: each root crosses into `model_x`, from `global` and
/// from `batch`.
const THREE: &str = r#"
[class.global]
segments = ["front"]
[class.batch]
segments = ["side"]
[class.model_x]
segments = ["engine"]
"#;

/// Two classes: both entries in `global`.
const TWO: &str = r#"
[class.global]
segments = ["front", "side"]
[class.model_x]
segments = ["engine"]
[cells.model_x]
policy = "home_first"
"#;

/// `text`, a placement, as the tables of layout `name`.
fn nest(name: &str, text: &str) -> String {
    text.lines()
        .map(|line| match line.strip_prefix('[') {
            Some(header) => format!("[layout.{name}.{header}"),
            None => line.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn read(text: &str) -> Placements {
    Placements::from_toml(text).unwrap_or_else(|problems| panic!("{problems}\nin:\n{text}"))
}

fn placement(text: &str) -> Placement {
    Placement::from_toml(text).expect("a placement")
}

fn refused(placement: &Placement) -> PlacementErrors {
    match layout(&trio(), placement) {
        Ok(_) => panic!("laid out {placement:?}"),
        Err(errors) => errors,
    }
}

#[test]
fn todays_form_is_one_layout_named_default() {
    let text = format!("{SEGMENTS}{THREE}");
    let placements = read(&text);
    assert!(!placements.tabled());
    assert_eq!(placements.len(), 1);
    assert_eq!(
        placements.names().collect::<Vec<_>>(),
        [Placements::DEFAULT]
    );
    assert_eq!(Placements::DEFAULT, "default");
    let only = placements.get(Placements::DEFAULT).expect("the one layout");
    assert_eq!(*only, placement(&text));
    assert_eq!(only.hash(), placement(&text).hash());
    // Its problems are where they always were.
    let unplaced = refused(&placement(
        "[segment.front]\nnodes = [\"Ask\"]\n[class.global]\nsegments = [\"front\"]\n",
    ));
    let problems = placements.problems(Placements::DEFAULT, &unplaced);
    assert_eq!(problems, gdl_model::Problems::from(unplaced).0);
}

#[test]
fn two_layouts_place_one_graph_two_ways() {
    let text = nest("regional", &format!("{SEGMENTS}{THREE}"))
        + &nest("compact", &format!("{SEGMENTS}{TWO}"));
    let placements = read(&text);
    assert!(placements.tabled());
    assert_eq!(
        placements.names().collect::<Vec<_>>(),
        ["compact", "regional"]
    );
    let regional = placements.get("regional").expect("regional");
    let compact = placements.get("compact").expect("compact");
    // Each is the placement its tables would be at the top of a spec, with
    // that placement's hash.
    assert_eq!(*regional, placement(&format!("{SEGMENTS}{THREE}")));
    assert_eq!(*compact, placement(&format!("{SEGMENTS}{TWO}")));
    assert_ne!(regional.hash(), compact.hash());
    let classes = |p: &Placement| {
        p.classes()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(classes(regional), ["batch", "global", "model_x"]);
    assert_eq!(classes(compact), ["global", "model_x"]);
    // Each validates as a complete graph on its own.
    for (name, placement) in placements.iter() {
        let laid = layout(&trio(), placement).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(laid.placement_hash(), placement.hash(), "{name}");
    }
    assert_eq!(placements.iter().count(), 2);
}

#[test]
fn two_layouts_that_place_alike_hash_alike() {
    // The second spells out the default router and lists its nodes in
    // another order: the same placement.
    let spelled = format!(
        "{}[routing]\nEngine = \"round_robin\"\n",
        SEGMENTS.replace("[\"Engine\", \"Spare\"]", "[\"Spare\", \"Engine\"]")
    );
    let text =
        nest("east", &format!("{SEGMENTS}{TWO}")) + &nest("west", &format!("{spelled}{TWO}"));
    let placements = read(&text);
    let east = placements.get("east").expect("east");
    let west = placements.get("west").expect("west");
    // Equal as hashed: node order in a segment is not part of a
    // placement's identity.
    assert_eq!(east.hash(), west.hash());
    assert_eq!(east.hash(), placement(&format!("{SEGMENTS}{TWO}")).hash());
}

/// Every layout is a complete graph on its own: a layout's problems are
/// its own, located in its tables, and they do not stop the others.
#[test]
fn each_layout_is_validated_on_its_own() {
    let unplaced = SEGMENTS.replace("\"Engine\", \"Spare\"", "\"Engine\"");
    let unclassed = THREE.replace("[class.batch]\nsegments = [\"side\"]\n", "");
    // Format answers Preprocess's hop into `model_x` from another segment
    // than Preprocess's: only the inner end may be elsewhere.
    let boundary = format!(
        "{}[segment.post]\nnodes = [\"Format\"]\n",
        SEGMENTS.replace(", \"Format\"", "")
    );
    let boundary_classes = "[class.global]\nsegments = [\"front\", \"post\", \"side\"]\n";
    let boundary_classes = format!("{boundary_classes}[class.model_x]\nsegments = [\"engine\"]\n");
    // Ask's root crosses from `global` into `model_x`, and Tell's from
    // `model_x` into `global`.
    let cycle = format!(
        "{}[segment.spare]\nnodes = [\"Spare\"]\n",
        SEGMENTS.replace("\"Engine\", \"Spare\"", "\"Engine\"")
    );
    let cycle_classes = "[class.global]\nsegments = [\"front\", \"spare\"]\n\
                         [class.model_x]\nsegments = [\"engine\", \"side\"]\n";
    let text = nest("good", &format!("{SEGMENTS}{THREE}"))
        + &nest("unplaced", &format!("{unplaced}{TWO}"))
        + &nest("unclassed", &format!("{SEGMENTS}{unclassed}"))
        + &nest("boundary", &format!("{boundary}{boundary_classes}"))
        + &nest("cycle", &format!("{cycle}{cycle_classes}"));
    let placements = read(&text);
    assert_eq!(placements.len(), 5);
    let problems = |name: &str| -> Vec<Problem> {
        placements.problems(name, &refused(placements.get(name).expect(name)))
    };
    layout(&trio(), placements.get("good").expect("good")).expect("the good layout");
    for (name, table, key, message) in [
        (
            "unplaced",
            "layout.unplaced.segment",
            None,
            "node `Spare` is in no segment",
        ),
        (
            "unclassed",
            "layout.unclassed.class",
            None,
            "segment `side` is in no class",
        ),
        (
            "boundary",
            "layout.boundary.segment.post",
            Some("nodes"),
            "hop `Preprocess` -> `Engine` crosses",
        ),
        (
            "cycle",
            "layout.cycle.class",
            None,
            "the class boundaries make a cycle through `global`, `model_x`",
        ),
    ] {
        let found = problems(name);
        assert!(
            found.iter().any(|p| p.table.as_deref() == Some(table)
                && p.key.as_deref() == key
                && p.message.starts_with(message)),
            "{name}: {found:?}"
        );
        // Every problem of a layout is in its tables.
        for problem in &found {
            let table = problem.table.as_deref().unwrap_or_default();
            assert!(table.starts_with(&format!("layout.{name}")), "{problem:?}");
        }
    }
}

/// Which edges cross depends on the layout, so a type that cannot cross
/// is the layout's problem as well as the graph's: it names the layout.
#[test]
fn a_problem_of_no_table_names_its_layout() {
    let mut graph = trio();
    graph.hops[2].forward_types[0] = TypeInfo::new("Local", false, Default::default());
    let text = nest("split", &format!("{SEGMENTS}{TWO}"));
    let placements = read(&text);
    let errors = layout(&graph, placements.get("split").expect("split")).expect_err("not wire");
    let problems = placements.problems("split", &errors);
    assert!(
        problems
            .iter()
            .any(|p| p.table.as_deref() == Some("layout.split") && p.message.contains("`Local`")),
        "{problems:?}"
    );
    // A graph that is wrong whatever its layout is not any layout's.
    graph.hops.clear();
    let errors = layout(&graph, placements.get("split").expect("split")).expect_err("no hops");
    let problems = placements.problems("split", &errors);
    assert!(
        problems
            .iter()
            .all(|p| p.table.is_none() && p.message.starts_with("invalid manifest")),
        "{problems:?}"
    );
}

#[test]
fn a_spelled_default_inside_a_layout_is_still_checked() {
    let text = nest(
        "a",
        &format!("{SEGMENTS}{TWO}[routing]\nNobody = \"round_robin\"\n"),
    );
    let placements = read(&text);
    let problems = placements.problems("a", &refused(placements.get("a").expect("a")));
    assert!(
        problems
            .iter()
            .any(|p| p.table.as_deref() == Some("layout.a.routing")
                && p.key.as_deref() == Some("Nobody")),
        "{problems:?}"
    );
}

/// What is wrong with how layouts are written is reported where it is
/// written, every problem at once where the file reads.
#[test]
fn reading_layouts_locates_their_problems() {
    let good = nest("a", &format!("{SEGMENTS}{TWO}"));
    let cases: Vec<(String, &str, Option<&str>, &str)> = vec![
        (
            format!("{good}[layout.a.bind.Engine]\ncrate = 'x'\n"),
            "layout.a",
            Some("bind"),
            "unknown field `bind`",
        ),
        (
            good.replace("[layout.a.class.global]", "[layout.a.class.Global]"),
            "layout.a.class.Global",
            None,
            "class name `Global`",
        ),
        (
            format!("{good}[layout.a.routing]\nEngine = {{ selector = \"Least\" }}\n"),
            "layout.a.routing",
            Some("Engine"),
            "selector name `Least`",
        ),
        (
            format!("{good}{}", nest("East", &format!("{SEGMENTS}{TWO}"))),
            "layout.East",
            None,
            "layout name `East`",
        ),
        (
            format!("{good}[segment.extra]\nnodes = [\"Ask\"]\n"),
            "segment",
            None,
            "a spec with [layout] tables",
        ),
        (
            "layout = {}\n".to_string(),
            "layout",
            None,
            "lists no layout",
        ),
    ];
    for (text, table, key, message) in cases {
        let Err(problems) = Placements::from_toml(&text) else {
            panic!("read:\n{text}");
        };
        assert!(
            problems.0.iter().any(|p| p.table.as_deref() == Some(table)
                && p.key.as_deref() == key
                && p.message.contains(message)),
            "{problems:?}\nin:\n{text}"
        );
    }
    // A bad name and a misplaced table are both reported.
    let text = format!(
        "{good}{}[routing]\nEngine = \"random\"\n",
        nest("East", &format!("{SEGMENTS}{TWO}"))
    );
    let problems = Placements::from_toml(&text).expect_err("two problems");
    assert_eq!(problems.0.len(), 2, "{problems}");
}

#[test]
fn a_layout_name_has_a_settings_keys_shape() {
    for good in ["us", "us_east_1", "compact"] {
        check_layout_name(good).unwrap_or_else(|e| panic!("{good}: {e}"));
    }
    for bad in [
        "",
        "US",
        "us-east",
        "us__east",
        "_us",
        "us_",
        "1us",
        &"a".repeat(65),
    ] {
        let why = check_layout_name(bad).expect_err(bad);
        assert!(why.starts_with(&format!("layout name `{bad}`")), "{why}");
    }
}

/// A layout's hash covers each of its classes' effective cell policy, as a
/// placement's does: a layout that spells the default policy hashes as one
/// that leaves it unsaid, and one that names another policy does not. A
/// layout's `[cells.<class>]` takes `policy` alone.
#[test]
fn a_layouts_hash_covers_its_effective_cell_policy() {
    let unsaid = TWO.replace("[cells.model_x]\npolicy = \"home_first\"\n", "");
    let spelled = TWO.replace("\"home_first\"", "\"round_robin\"");
    assert_ne!(unsaid, TWO);
    let text = nest("unsaid", &format!("{SEGMENTS}{unsaid}"))
        + &nest("spelled", &format!("{SEGMENTS}{spelled}"))
        + &nest("other", &format!("{SEGMENTS}{TWO}"));
    let placements = read(&text);
    let hash = |name: &str| placements.get(name).expect(name).hash();
    assert_eq!(hash("unsaid"), hash("spelled"));
    assert_ne!(hash("unsaid"), hash("other"));
    // Each is the hash of its tables at the top of a spec.
    assert_eq!(
        hash("unsaid"),
        placement(&format!("{SEGMENTS}{unsaid}")).hash()
    );
    let removed = nest("a", &format!("{SEGMENTS}{TWO}max_near = 4\n"));
    let problems = Placements::from_toml(&removed).expect_err("max_near is gone");
    assert!(
        problems
            .0
            .iter()
            .any(|p| p.table.as_deref() == Some("layout.a.cells.model_x")
                && p.key.as_deref() == Some("max_near")),
        "{problems:?}"
    );
}

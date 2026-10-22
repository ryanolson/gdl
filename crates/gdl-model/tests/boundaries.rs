//! The boundary rule between classes, the class cycle check, and the WAN
//! round trips the crossing report counts at each class boundary. Each
//! placement is checked as parsed and as built in code.

use gdl_model::{
    ClassBoundary, HopInfo, Manifest, NodeInfo, Placement, RequestRouter, Role, TypeInfo, layout,
};

/// One hop: outer end, forward chain, inner end, backward chain.
type Hop<'a> = (&'a str, &'a [&'a str], &'a str, &'a [&'a str]);

/// A manifest of `nodes` and `hops`, every edge a wire type.
fn manifest(nodes: &[(&str, Role)], hops: &[Hop]) -> Manifest {
    let ty = || TypeInfo::new("T", true);
    Manifest {
        name: "bounded".into(),
        schema: 1,
        nodes: nodes
            .iter()
            .map(|&(name, role)| NodeInfo {
                name: name.into(),
                role,
                binding: (role != Role::Source).then(|| name.to_lowercase()),
            })
            .collect(),
        hops: hops
            .iter()
            .map(|&(outer, forward, inner, backward)| HopInfo {
                outer: outer.into(),
                forward: forward.iter().map(|&n| n.into()).collect(),
                inner: inner.into(),
                backward: backward.iter().map(|&n| n.into()).collect(),
                forward_types: (0..=forward.len()).map(|_| ty()).collect(),
                backward_types: (0..=backward.len()).map(|_| ty()).collect(),
            })
            .collect(),
    }
}

/// The graph of `agent-docs/cells.md` section 3.1,
/// `Http => Pre { Route { Model { Engine => Detok } } }`, with its roles
/// and types, and with `forward` and `backward` stages added to Route's
/// hop. `gdl-syntax`'s `tests/classes.rs` parses the same graph from its
/// text and pins the same report.
fn cells(forward: &[&str], backward: &[&str]) -> Manifest {
    let mut nodes = vec![
        ("Http", Role::Source),
        ("Pre", Role::Operator),
        ("Route", Role::Operator),
        ("Model", Role::Operator),
        ("Engine", Role::Sink),
        ("Detok", Role::Backward),
    ];
    nodes.extend(forward.iter().map(|&n| (n, Role::Forward)));
    nodes.extend(backward.iter().map(|&n| (n, Role::Backward)));
    let mut graph = manifest(
        &nodes,
        &[
            ("Http", &[], "Pre", &[]),
            ("Pre", &[], "Route", &[]),
            ("Route", forward, "Model", backward),
            ("Model", &[], "Engine", &["Detok"]),
        ],
    );
    let named = |names: &[&str]| names.iter().map(|&n| TypeInfo::new(n, true)).collect();
    graph.hops[0].forward_types = named(&["ChatRequest"]);
    graph.hops[0].backward_types = named(&["ChatChunk"]);
    graph.hops[1].forward_types = named(&["Prompt"]);
    graph.hops[1].backward_types = named(&["Text"]);
    graph.hops[2].forward_types = named(&vec!["Tokens"; forward.len() + 1]);
    graph.hops[2].backward_types = named(&vec!["Text"; backward.len() + 1]);
    graph.hops[3].forward_types = named(&["Tokens"]);
    graph.hops[3].backward_types = named(&["Token", "Text"]);
    graph
}

type Segments<'a> = &'a [(&'a str, &'a [&'a str])];

/// The placement `segments` and `classes` describe, parsed from text with
/// `extra` appended, and built in code with `build` applied; asserted
/// equal, so a case checks both.
fn both(
    segments: Segments,
    classes: Segments,
    extra: &str,
    build: impl Fn(Placement) -> Placement,
) -> [Placement; 2] {
    let list = |items: &[&str]| {
        items
            .iter()
            .map(|i| format!("'{i}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut text = String::new();
    let mut built = Placement::new();
    for &(name, nodes) in segments {
        text.push_str(&format!("[segment.{name}]\nnodes = [{}]\n", list(nodes)));
        built = built.segment(name, nodes.iter().copied());
    }
    for &(name, members) in classes {
        text.push_str(&format!("[class.{name}]\nsegments = [{}]\n", list(members)));
        built = built.class(name, members.iter().copied());
    }
    text.push_str(extra);
    let parsed = Placement::from_toml(&text).expect("a placement");
    let built = build(built);
    assert_eq!(parsed, built, "{text}");
    [parsed, built]
}

/// The segments of `agent-docs/cells.md` section 3.2, with room for the
/// stages the counterexamples add: Post and Tok in B, Fix in C.
const CELL_SEGMENTS: Segments = &[
    ("A", &["Http"]),
    ("B", &["Pre", "Post", "Tok"]),
    ("C", &["Route", "Fix"]),
    ("D", &["Model"]),
    ("E", &["Engine"]),
    ("F", &["Detok"]),
];

/// The classes of section 3.2.
const CELL_CLASSES: Segments = &[("global", &["A", "B", "C"]), ("model_x", &["D", "E", "F"])];

/// The segments of section 3.2 holding only the nodes `graph` has.
fn cell_segments(graph: &Manifest) -> Vec<(&'static str, Vec<&'static str>)> {
    CELL_SEGMENTS
        .iter()
        .map(|&(name, nodes)| {
            let nodes = nodes
                .iter()
                .copied()
                .filter(|n| graph.node(n).is_some())
                .collect();
            (name, nodes)
        })
        .collect()
}

/// `graph` under the cell segments and `classes`, parsed and built.
fn cell_placements(graph: &Manifest, classes: Segments) -> [Placement; 2] {
    routed_cell_placements(graph, classes, "", |p| p)
}

/// The same, with `extra` appended to the text and `build` applied to the
/// built placement.
fn routed_cell_placements(
    graph: &Manifest,
    classes: Segments,
    extra: &str,
    build: impl Fn(Placement) -> Placement,
) -> [Placement; 2] {
    let segments = cell_segments(graph);
    let segments: Vec<(&str, &[&str])> = segments.iter().map(|(n, s)| (*n, s.as_slice())).collect();
    both(&segments, classes, extra, build)
}

/// Each problem `layout` reports, in its order.
fn refusals(graph: &Manifest, placement: &Placement) -> Vec<String> {
    match layout(graph, placement) {
        Ok(_) => panic!("laid out {placement:?}"),
        Err(errors) => errors.0.iter().map(ToString::to_string).collect(),
    }
}

fn refused(graph: &Manifest, placement: &Placement) -> String {
    refusals(graph, placement).join("; ")
}

#[test]
fn the_recommended_shape_crosses_once_at_the_choosers_hop() {
    let graph = cells(&[], &[]);
    for placement in cell_placements(&graph, CELL_CLASSES) {
        let laid = layout(&graph, &placement).expect("a layout");
        assert_eq!(
            laid.boundaries(),
            [ClassBoundary {
                hop: 2,
                from: "global".into(),
                into: "model_x".into(),
            }]
        );
        let report = laid.report();
        assert_eq!(report.wan_round_trips(), 1);
        assert_eq!(
            report.to_string(),
            "hop 0: 2 crossings, 1 anchors, 0 WAN round trips\n\
             hop 1: 2 crossings, 1 anchors, 0 WAN round trips\n\
             hop 2: 2 crossings, 1 anchors, 1 WAN round trips (global -> model_x)\n\
             hop 3: 3 crossings, 2 anchors, 0 WAN round trips\n\
             per request: 9 crossings, 5 anchors, 1 WAN round trips"
        );
    }
}

/// Counterexample 1 of review `cells-r2`: Route provisions Post first, so
/// Model's output would go to Post's instance, which never sent to Model.
#[test]
fn a_backward_stage_in_another_segment_of_the_outer_class_is_refused() {
    let graph = cells(&[], &["Post"]);
    for placement in cell_placements(&graph, CELL_CLASSES) {
        assert_eq!(
            refused(&graph, &placement),
            "hop `Route` -> `Model` crosses from class `global` into `model_x`, but its backward stage `Post` is in segment `B`; every stage but the inner end must be in `Route`'s segment `C`"
        );
    }
}

/// Counterexample 2 of review `cells-r2`: the forward path leaves C and
/// returns to it, possibly on another instance than the one that chose.
#[test]
fn a_forward_path_that_leaves_and_returns_to_the_outer_segment_is_refused() {
    let graph = cells(&["Tok", "Fix"], &[]);
    for placement in cell_placements(&graph, CELL_CLASSES) {
        assert_eq!(
            refused(&graph, &placement),
            "hop `Route` -> `Model` crosses from class `global` into `model_x`, but its forward stage `Tok` is in segment `B`; every stage but the inner end must be in `Route`'s segment `C`"
        );
    }
}

/// A hop that reaches another class and comes back: its inner end is in
/// its outer end's class. The stage in the other class is also outside the
/// outer end's segment, and both are reported.
#[test]
fn a_hop_whose_inner_end_stays_in_the_outer_class_may_not_cross() {
    let segments: Segments = &[("a", &["S", "D"]), ("m", &["X"])];
    let classes: Segments = &[("global", &["a"]), ("model_x", &["m"])];
    let nodes = [("S", Role::Source), ("D", Role::Sink)];
    for (role, hop) in [
        (Role::Forward, ("S", &["X"][..], "D", &[][..])),
        (Role::Backward, ("S", &[][..], "D", &["X"][..])),
    ] {
        let mut nodes = nodes.to_vec();
        nodes.push(("X", role));
        let graph = manifest(&nodes, &[hop]);
        let stage = if role == Role::Forward {
            "forward"
        } else {
            "backward"
        };
        for placement in both(segments, classes, "", |p| p) {
            assert_eq!(
                refused(&graph, &placement),
                format!(
                    "hop `S` -> `D` reaches class `model_x` at `X`, but its inner end `D` is in `S`'s class `global`; a hop may cross into another class only at its inner end; \
                     hop `S` -> `D` crosses from class `global` into `model_x`, but its {stage} stage `X` is in segment `m`; every stage but the inner end must be in `S`'s segment `a`"
                )
            );
        }
    }
}

#[test]
fn a_hop_under_the_inner_end_stays_in_the_inner_class() {
    let graph = cells(&[], &[]);
    let third: Segments = &[
        ("global", &["A", "B", "C"]),
        ("model_x", &["D", "F"]),
        ("other", &["E"]),
    ];
    for placement in cell_placements(&graph, third) {
        assert_eq!(
            refused(&graph, &placement),
            "hop `Route` -> `Model` crosses from class `global` into `model_x`, but `Engine`, under `Model`, is in segment `E` of class `other`; every hop under the inner end must stay in `model_x`"
        );
    }
    let back: Segments = &[("global", &["A", "B", "C", "F"]), ("model_x", &["D", "E"])];
    for placement in cell_placements(&graph, back) {
        assert_eq!(
            refused(&graph, &placement),
            "hop `Route` -> `Model` crosses from class `global` into `model_x`, but `Detok`, under `Model`, is in segment `F` of class `global`; every hop under the inner end must stay in `model_x`"
        );
    }
}

/// Two roots, `Sa -> Da` and `Sb -> Db`.
fn two_roots() -> Manifest {
    manifest(
        &[
            ("Sa", Role::Source),
            ("Da", Role::Sink),
            ("Sb", Role::Source),
            ("Db", Role::Sink),
        ],
        &[("Sa", &[], "Da", &[]), ("Sb", &[], "Db", &[])],
    )
}

const TWO_ROOTS: Segments = &[
    ("a", &["Sa"]),
    ("b", &["Sb"]),
    ("m", &["Da"]),
    ("n", &["Db"]),
];

#[test]
fn boundaries_that_cross_back_make_a_refused_cycle() {
    let graph = two_roots();
    let cycle: Segments = &[("global", &["a", "n"]), ("model_x", &["b", "m"])];
    for placement in both(TWO_ROOTS, cycle, "", |p| p) {
        assert_eq!(
            refused(&graph, &placement),
            "the class boundaries make a cycle through `global`, `model_x`; work may cross between two classes in one direction only"
        );
    }
}

#[test]
fn two_boundaries_into_one_class_are_not_a_cycle() {
    let graph = two_roots();
    let one_way: Segments = &[("global", &["a", "b"]), ("model_x", &["m", "n"])];
    for placement in both(TWO_ROOTS, one_way, "", |p| p) {
        let laid = layout(&graph, &placement).expect("a layout");
        let into = |hop| ClassBoundary {
            hop,
            from: "global".into(),
            into: "model_x".into(),
        };
        assert_eq!(laid.boundaries(), [into(0), into(1)]);
        assert_eq!(
            laid.report().to_string(),
            "hop 0: 2 crossings, 1 anchors, 1 WAN round trips (global -> model_x)\n\
             hop 1: 2 crossings, 1 anchors, 1 WAN round trips (global -> model_x)\n\
             one request per root: 4 crossings, 2 anchors, 2 WAN round trips"
        );
    }
}

/// A streamed request's items wait for the acknowledgement that brings back
/// the inner end's input anchor: a second round trip.
#[test]
fn a_streamed_request_into_another_class_costs_two_round_trips() {
    let mut graph = cells(&[], &[]);
    graph.hops[2].forward_types = vec![TypeInfo::streamed("Tokens", true, "Token", true)];
    for placement in cell_placements(&graph, CELL_CLASSES) {
        let report = layout(&graph, &placement).expect("a layout").report();
        assert_eq!(report.wan_round_trips(), 2);
        assert_eq!(
            report.to_string(),
            "hop 0: 2 crossings, 1 anchors, 0 WAN round trips\n\
             hop 1: 2 crossings, 1 anchors, 0 WAN round trips\n\
             hop 2: 2 crossings, 2 anchors, 2 WAN round trips (global -> model_x)\n\
             hop 3: 3 crossings, 2 anchors, 0 WAN round trips\n\
             per request: 9 crossings, 6 anchors, 2 WAN round trips"
        );
    }
}

const POST_IN_B: &str = "hop `Route` -> `Model` crosses from class `global` into `model_x`, but its backward stage `Post` is in segment `B`; every stage but the inner end must be in `Route`'s segment `C`";

/// `errors` are `expected`, in any order.
fn assert_all(mut errors: Vec<String>, expected: &[&str]) {
    let mut expected: Vec<String> = expected.iter().map(ToString::to_string).collect();
    errors.sort();
    expected.sort();
    assert_eq!(errors, expected);
}

#[test]
fn a_boundary_problem_is_reported_with_every_other_problem() {
    let graph = cells(&[], &["Post"]);
    // `Http` is the source: no request boundary enters it.
    for placement in
        routed_cell_placements(&graph, CELL_CLASSES, "[routing]\nHttp = 'random'\n", |p| {
            p.route("Http", RequestRouter::Random)
        })
    {
        assert_all(
            refusals(&graph, &placement),
            &[
                "request router `Http` must target a request boundary",
                POST_IN_B,
            ],
        );
    }
    let mut opaque = graph.clone();
    opaque.hops[0].forward_types[0] = TypeInfo::new("ChatRequest", false);
    for placement in cell_placements(&opaque, CELL_CLASSES) {
        assert_all(
            refusals(&opaque, &placement),
            &[
                "edge `Http` -> `Pre` crosses a segment boundary, but its type `ChatRequest` is not serializable",
                POST_IN_B,
            ],
        );
    }
    let unknown: Segments = &[
        ("global", &["A", "B", "C", "zz"]),
        ("model_x", &["D", "E", "F"]),
    ];
    for placement in cell_placements(&graph, unknown) {
        assert_all(
            refusals(&graph, &placement),
            &["class `global` lists unknown segment `zz`", POST_IN_B],
        );
    }
    let back: Segments = &[("global", &["A", "B", "C", "F"]), ("model_x", &["D", "E"])];
    for placement in cell_placements(&graph, back) {
        assert_all(
            refusals(&graph, &placement),
            &[
                POST_IN_B,
                "hop `Route` -> `Model` crosses from class `global` into `model_x`, but `Detok`, under `Model`, is in segment `F` of class `global`; every hop under the inner end must stay in `model_x`",
            ],
        );
    }
}

/// Without a class table, the counterexamples are what they were before
/// classes existed: valid placements, with the same report.
#[test]
fn without_classes_a_hop_may_cut_anywhere() {
    for (graph, hop2, total) in [
        (
            cells(&[], &["Post"]),
            "hop 2: 3 crossings, 2 anchors",
            "per request: 10 crossings, 6 anchors",
        ),
        (
            cells(&["Tok", "Fix"], &[]),
            "hop 2: 4 crossings, 1 anchors",
            "per request: 11 crossings, 5 anchors",
        ),
    ] {
        for placement in cell_placements(&graph, &[]) {
            let laid = layout(&graph, &placement).expect("a layout");
            assert_eq!(laid.class_of("C"), None);
            assert_eq!(
                laid.report().to_string(),
                format!(
                    "hop 0: 2 crossings, 1 anchors\n\
                     hop 1: 2 crossings, 1 anchors\n\
                     {hop2}\n\
                     hop 3: 3 crossings, 2 anchors\n\
                     {total}"
                )
            );
        }
    }
}

/// `S -> D` with a backward stage `Post`, under `S`'s class `global`, in
/// another segment than `S`'s: a rule (b) refusal.
fn posted() -> Manifest {
    manifest(
        &[
            ("S", Role::Source),
            ("D", Role::Sink),
            ("Post", Role::Backward),
        ],
        &[("S", &[], "D", &["Post"])],
    )
}

const POST_OFF_A: &str = "hop `S` -> `D` crosses from class `global` into `model_x`, but its backward stage `Post` is in segment `b`; every stage but the inner end must be in `S`'s segment `a`";

/// Rule (c) holds for the inner end of a hop under the boundary's inner
/// end, and for hops nested deeper.
#[test]
fn rule_c_reaches_every_hop_under_the_inner_end() {
    let shallow = manifest(
        &[
            ("S", Role::Source),
            ("Entry", Role::Operator),
            ("D", Role::Sink),
        ],
        &[("S", &[], "Entry", &[]), ("Entry", &[], "D", &[])],
    );
    let deep = manifest(
        &[
            ("S", Role::Source),
            ("Entry", Role::Operator),
            ("Mid", Role::Operator),
            ("D", Role::Sink),
        ],
        &[
            ("S", &[], "Entry", &[]),
            ("Entry", &[], "Mid", &[]),
            ("Mid", &[], "D", &[]),
        ],
    );
    let expected = "hop `S` -> `Entry` crosses from class `global` into `model_x`, but `D`, under `Entry`, is in segment `o` of class `other`; every hop under the inner end must stay in `model_x`";
    for placement in both(
        &[("a", &["S"]), ("m", &["Entry"]), ("o", &["D"])],
        &[("global", &["a"]), ("model_x", &["m"]), ("other", &["o"])],
        "",
        |p| p,
    ) {
        assert_eq!(refused(&shallow, &placement), expected);
    }
    for placement in both(
        &[("a", &["S"]), ("m", &["Entry", "Mid"]), ("o", &["D"])],
        &[("global", &["a"]), ("model_x", &["m"]), ("other", &["o"])],
        "",
        |p| p,
    ) {
        assert_eq!(refused(&deep, &placement), expected);
    }
}

/// A class table's problem, here an empty class no segment needs, leaves
/// the boundary rule judged.
#[test]
fn a_class_table_problem_does_not_hide_a_boundary_problem() {
    let graph = posted();
    let spare: Segments = &[("global", &["a", "b"]), ("model_x", &["m"]), ("spare", &[])];
    for placement in both(
        &[("a", &["S"]), ("b", &["Post"]), ("m", &["D"])],
        spare,
        "",
        |p| p,
    ) {
        assert_all(
            refusals(&graph, &placement),
            &["class `spare` has no segments", POST_OFF_A],
        );
    }
}

/// A boundary that breaks the rule is still a boundary, so it still closes
/// a cycle.
#[test]
fn a_boundary_problem_does_not_hide_a_cycle() {
    let graph = manifest(
        &[
            ("Sa", Role::Source),
            ("Da", Role::Sink),
            ("Pa", Role::Backward),
            ("Sb", Role::Source),
            ("Db", Role::Sink),
        ],
        &[("Sa", &[], "Da", &["Pa"]), ("Sb", &[], "Db", &[])],
    );
    for placement in both(
        &[
            ("a", &["Sa"]),
            ("p", &["Pa"]),
            ("m", &["Da"]),
            ("b", &["Sb"]),
            ("n", &["Db"]),
        ],
        &[("global", &["a", "p", "n"]), ("model_x", &["m", "b"])],
        "",
        |p| p,
    ) {
        assert_all(
            refusals(&graph, &placement),
            &[
                "hop `Sa` -> `Da` crosses from class `global` into `model_x`, but its backward stage `Pa` is in segment `p`; every stage but the inner end must be in `Sa`'s segment `a`",
                "the class boundaries make a cycle through `global`, `model_x`; work may cross between two classes in one direction only",
            ],
        );
    }
}

/// With classes, the report counts WAN round trips even where no hop
/// crosses a class boundary; without, it reads as before classes existed.
#[test]
fn a_classed_report_counts_round_trips_without_a_boundary() {
    let graph = manifest(
        &[("S", Role::Source), ("D", Role::Sink)],
        &[("S", &[], "D", &[])],
    );
    let segments: Segments = &[("a", &["S"]), ("b", &["D"])];
    for placement in both(segments, &[("global", &["a", "b"])], "", |p| p) {
        let laid = layout(&graph, &placement).expect("a layout");
        assert!(laid.boundaries().is_empty());
        assert_eq!(
            laid.report().to_string(),
            "hop 0: 2 crossings, 1 anchors, 0 WAN round trips\n\
             per request: 2 crossings, 1 anchors, 0 WAN round trips"
        );
    }
    for placement in both(segments, &[], "", |p| p) {
        assert_eq!(
            layout(&graph, &placement)
                .expect("a layout")
                .report()
                .to_string(),
            "hop 0: 2 crossings, 1 anchors\nper request: 2 crossings, 1 anchors"
        );
    }
}

/// Each cycle is reported once, with its classes in name order, however
/// long it is.
#[test]
fn each_cycle_is_named_once() {
    let mut nodes = Vec::new();
    let mut hops = Vec::new();
    let names: Vec<(String, String)> = (0..5).map(|i| (format!("S{i}"), format!("D{i}"))).collect();
    for (source, sink) in &names {
        nodes.push((source.as_str(), Role::Source));
        nodes.push((sink.as_str(), Role::Sink));
        hops.push((source.as_str(), &[][..], sink.as_str(), &[][..]));
    }
    let graph = manifest(&nodes, &hops);
    let segments: Segments = &[
        ("s0", &["S0"]),
        ("s1", &["S1"]),
        ("s2", &["S2"]),
        ("s3", &["S3"]),
        ("s4", &["S4"]),
        ("d0", &["D0"]),
        ("d1", &["D1"]),
        ("d2", &["D2"]),
        ("d3", &["D3"]),
        ("d4", &["D4"]),
    ];
    // a -> b -> c -> a, and d -> e -> d.
    let classes: Segments = &[
        ("c", &["s2", "d1"]),
        ("a", &["s0", "d2"]),
        ("b", &["s1", "d0"]),
        ("e", &["s4", "d3"]),
        ("d", &["s3", "d4"]),
    ];
    for placement in both(segments, classes, "", |p| p) {
        assert_eq!(
            refusals(&graph, &placement),
            [
                "the class boundaries make a cycle through `a`, `b`, `c`; work may cross between two classes in one direction only",
                "the class boundaries make a cycle through `d`, `e`; work may cross between two classes in one direction only",
            ]
        );
    }
}

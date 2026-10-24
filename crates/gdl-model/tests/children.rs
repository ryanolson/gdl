//! A fleet is complete per parent cell: every cell whose class sends to
//! another class has a child, a cell whose `home` is that cell, of every
//! class it sends to, in its own layout. With three classes in a chain,
//! this holds at every level.

use std::collections::BTreeMap;

use gdl_model::{
    Fleet, HopInfo, Layout, Manifest, NodeInfo, Placement, Placements, Role, TypeInfo, layout,
};

/// `S => F` and `T => G`: two roots, so that one layout can chain three
/// classes (each root crosses one boundary at most).
fn manifest() -> Manifest {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, inner: &str| HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![TypeInfo::new("T", true, Default::default())],
        backward_types: vec![TypeInfo::new("T", true, Default::default())],
    };
    Manifest {
        name: "roots".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("T", Role::Source),
            node("F", Role::Sink),
            node("G", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("T", "G")],
    }
}

const SEGMENTS: &str = "[segment.s]\nnodes = ['S']\n[segment.t]\nnodes = ['T']\n\
                        [segment.f]\nnodes = ['F']\n[segment.g]\nnodes = ['G']\n";

/// Two classes: `global` sends to `model_x`.
const TWO: &str = "[class.global]\nsegments = ['s', 't']\n[class.model_x]\nsegments = ['f', 'g']\n";

/// Three classes in a chain over the two roots: `S => F` crosses from
/// `global` into `model_x`, and `T => G` from `model_x` into `model_y`.
const CHAIN: &str = "[class.global]\nsegments = ['s']\n[class.model_x]\nsegments = ['f', 't']\n\
                     [class.model_y]\nsegments = ['g']\n";

type Layouts = BTreeMap<String, Option<Layout>>;

fn placed(classes: &str) -> Layout {
    let placement = Placement::from_toml(&format!("{SEGMENTS}{classes}")).expect("a placement");
    layout(&manifest(), &placement).expect("a layout")
}

/// The one layout of a spec without `[layout]` tables, with `classes`.
fn one(classes: &str) -> Layouts {
    BTreeMap::from([(Placements::DEFAULT.to_string(), Some(placed(classes)))])
}

/// Each problem the check reports, in its order.
fn problems(text: &str, layouts: &Layouts) -> Vec<String> {
    match Fleet::from_toml(text)
        .expect("a fleet")
        .check(Some(layouts))
    {
        Ok(()) => panic!("accepted:\n{text}"),
        Err(errors) => errors.0.iter().map(ToString::to_string).collect(),
    }
}

fn clean(text: &str, layouts: &Layouts) {
    if let Err(errors) = Fleet::from_toml(text)
        .expect("a fleet")
        .check(Some(layouts))
    {
        panic!("{errors}\nin:\n{text}");
    }
}

/// The cell `name` of `class`, with `home` when it is not empty, and
/// `layout` when it is not empty.
fn cell(name: &str, class: &str, home: &str, layout: &str) -> String {
    let mut text = format!("[cell.{name}]\nclass = '{class}'\nregion = 'us-east-1'\n");
    if !home.is_empty() {
        text += &format!("home = '{home}'\n");
    }
    if !layout.is_empty() {
        text += &format!("layout = '{layout}'\n");
    }
    text
}

/// The problem of the parent cell `cell` of `layout` without a child of
/// `class`.
fn childless(cell: &str, layout: &str, class: &str) -> String {
    format!(
        "[cell.{cell}]: no child of class `{class}` in layout `{layout}`; a cell of a class that sends to `{class}` has a child of it, a cell whose `home` is this one"
    )
}

#[test]
fn a_global_with_a_model_child_checks_clean() {
    let text = cell("g", "global", "", "") + &cell("m", "model_x", "g", "");
    clean(&text, &one(TWO));
}

/// Each parent cell without a child of a class its class sends to is one
/// problem, located at the parent's table, naming its layout and the
/// class it lacks.
#[test]
fn a_global_without_a_model_child_is_refused() {
    assert_eq!(
        problems(&cell("g", "global", "", ""), &one(TWO)),
        [childless("g", "default", "model_x")]
    );
    let text = cell("g", "global", "", "")
        + &cell("h", "global", "", "")
        + &cell("k", "global", "", "")
        + &cell("m", "model_x", "k", "");
    assert_eq!(
        problems(&text, &one(TWO)),
        [
            childless("g", "default", "model_x"),
            childless("h", "default", "model_x"),
        ]
    );
}

/// A child counts for its own home alone, and in its own layout alone.
#[test]
fn a_child_counts_for_its_home_in_its_layout() {
    let text =
        cell("g", "global", "", "") + &cell("h", "global", "", "") + &cell("m", "model_x", "h", "");
    assert_eq!(
        problems(&text, &one(TWO)),
        [childless("g", "default", "model_x")]
    );

    // Two layouts with the same classes, so that the layout alone differs.
    let two = BTreeMap::from([
        ("east".to_string(), Some(placed(TWO))),
        ("west".to_string(), Some(placed(TWO))),
    ]);
    let text = cell("g-east", "global", "", "east")
        + &cell("g-west", "global", "", "west")
        + &cell("m-east", "model_x", "g-east", "east");
    assert_eq!(
        problems(&text, &two),
        [childless("g-west", "west", "model_x")]
    );
    // A model cell of the other layout at that home is its own problem,
    // and is not the home's child either.
    let text = text + &cell("m-west", "model_x", "g-west", "east");
    let found = problems(&text, &two);
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found[0].starts_with("[cell.g-west]: no child"), "{found:?}");
    assert!(
        found[1].starts_with("[cell.m-west] home: cell `g-west` runs layout `west`"),
        "{found:?}"
    );
    // The control: the child in the home's own layout.
    clean(
        &text.replace(
            "home = 'g-west'\nlayout = 'east'",
            "home = 'g-west'\nlayout = 'west'",
        ),
        &two,
    );
}

/// A cell that would be a child but whose `home` is missing or names no
/// cell is reported, and does not count for any parent.
#[test]
fn a_cell_without_a_home_is_no_ones_child() {
    let text = cell("g", "global", "", "") + &cell("m", "model_x", "", "");
    let found = problems(&text, &one(TWO));
    assert_eq!(found.len(), 2, "{found:?}");
    assert_eq!(found[0], childless("g", "default", "model_x"));
    assert!(found[1].starts_with("[cell.m] home: missing"), "{found:?}");
}

/// With three classes in a chain, a middle cell is a parent too: it has a
/// child of the bottom class, and the top cell has one of the middle.
#[test]
fn every_level_of_a_chain_has_its_children() {
    let chain = one(CHAIN);
    let complete = cell("g", "global", "", "")
        + &cell("x", "model_x", "g", "")
        + &cell("y", "model_y", "x", "");
    clean(&complete, &chain);

    let middle = cell("g", "global", "", "") + &cell("x", "model_x", "g", "");
    assert_eq!(
        problems(&middle, &chain),
        [childless("x", "default", "model_y")]
    );
    let top = cell("g", "global", "", "");
    assert_eq!(
        problems(&top, &chain),
        [childless("g", "default", "model_x")]
    );
    // A second middle cell needs its own bottom child.
    let text = complete.clone() + &cell("x2", "model_x", "g", "");
    assert_eq!(
        problems(&text, &chain),
        [childless("x2", "default", "model_y")]
    );
}

/// Without a layout, which class sends to which is not known, so the rule
/// is not judged, and the fleet says so once, in no table of its own.
#[test]
fn without_a_layout_children_are_not_judged() {
    let fleet = Fleet::from_toml(&(cell("g", "global", "", "") + &cell("h", "global", "", "")))
        .expect("a fleet");
    fleet
        .check(None)
        .expect("nothing to judge without a layout");
    let note = fleet.children_unchecked(None).expect("a note");
    assert_eq!((note.table, note.key), (None, None));
    assert_eq!(
        note.message,
        "the cells' children are not checked: which class sends to which is a layout's, and the spec's layouts did not read"
    );

    let unfit = BTreeMap::from([(Placements::DEFAULT.to_string(), None)]);
    fleet
        .check(Some(&unfit))
        .expect("nothing to judge without the layout's classes");
    assert_eq!(
        fleet
            .children_unchecked(Some(&unfit))
            .expect("a note")
            .message,
        "the cells' children are not checked in layout `default`: which class sends to which is a layout's, and the layout has problems"
    );

    // Only a layout that a cell runs is named.
    let mut layouts = BTreeMap::from([
        ("east".to_string(), None),
        ("west".to_string(), None),
        ("north".to_string(), Some(placed(TWO))),
    ]);
    let named = Fleet::from_toml(
        &(cell("g", "global", "", "east")
            + &cell("n", "global", "", "north")
            + &cell("m", "model_x", "n", "north")),
    )
    .expect("a fleet");
    assert_eq!(
        named
            .children_unchecked(Some(&layouts))
            .expect("a note")
            .message,
        "the cells' children are not checked in layout `east`: which class sends to which is a layout's, and the layout has problems"
    );
    let both =
        Fleet::from_toml(&(cell("g", "global", "", "east") + &cell("w", "global", "", "west")))
            .expect("a fleet");
    assert_eq!(
        both.children_unchecked(Some(&layouts))
            .expect("one note")
            .message,
        "the cells' children are not checked in layouts `east`, `west`: which class sends to which is a layout's, and the layouts have problems"
    );
    layouts.insert("east".into(), Some(placed(TWO)));
    // The control: every layout a cell runs is known, and nothing goes
    // unchecked.
    let complete =
        Fleet::from_toml(&(cell("g", "global", "", "east") + &cell("m", "model_x", "g", "east")))
            .expect("a fleet");
    assert_eq!(complete.children_unchecked(Some(&layouts)), None);
    assert_eq!(Fleet::default().children_unchecked(None), None, "no cells");
}

//! What a cell's health requires of the classes its class sends to: by
//! default, one ready cell of each; an explicit `requires` replaces the
//! default, to raise a count or to name a subset.

use std::collections::BTreeMap;

use gdl_model::{
    CellSettings, HopInfo, Layout, Manifest, NodeInfo, Placement, Role, TypeInfo, layout,
};

/// `S => F` and `T => G`, so that `global` sends to two classes.
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
        name: "two-classes".into(),
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

/// `global` sends to `model_x` and `model_y`.
fn two_classes() -> Layout {
    let placement = Placement::new()
        .segment("a", ["S", "T"])
        .segment("f", ["F"])
        .segment("g", ["G"])
        .class("global", ["a"])
        .class("model_x", ["f"])
        .class("model_y", ["g"]);
    layout(&manifest(), &placement).expect("a layout")
}

fn settings(text: &str) -> CellSettings {
    toml::from_str(text).expect("a cell's settings")
}

const GLOBAL: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";

fn counts(pairs: &[(&str, u32)]) -> BTreeMap<String, u32> {
    pairs.iter().map(|(c, n)| (c.to_string(), *n)).collect()
}

/// Unsaid, `requires` is one ready cell of each class the cell's class
/// sends to, and a cell of a class that sends to none requires nothing.
#[test]
fn unsaid_requires_each_class_sent_to_once() {
    let default = CellSettings::DEFAULT_REQUIRES;
    assert_eq!(default, 1);
    assert_eq!(
        settings(GLOBAL).requirements(&two_classes()),
        counts(&[("model_x", default), ("model_y", default)])
    );
    let model = settings("name = 'm'\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n");
    assert_eq!(model.requirements(&two_classes()), BTreeMap::new());
}

/// The default is unsaid when the settings are written, and an explicit
/// `requires` equal to it requires the same.
#[test]
fn the_default_is_unsaid_and_spelling_it_out_changes_nothing() {
    let unsaid = settings(GLOBAL);
    let text = toml::to_string(&unsaid).expect("toml");
    assert!(!text.contains("requires"), "{text}");
    assert_eq!(settings(&text), unsaid, "{text}");
    let spelled = settings(&format!("{GLOBAL}[requires]\nmodel_x = 1\nmodel_y = 1\n"));
    spelled
        .check_hub(&two_classes())
        .expect("the default, spelled out");
    assert_eq!(
        spelled.requirements(&two_classes()),
        unsaid.requirements(&two_classes())
    );
}

/// An explicit `requires` replaces the default whole: it raises a count,
/// or names a subset of the classes sent to.
#[test]
fn an_explicit_requires_replaces_the_default() {
    let raised = settings(&format!("{GLOBAL}[requires]\nmodel_x = 3\nmodel_y = 1\n"));
    assert_eq!(
        raised.requirements(&two_classes()),
        counts(&[("model_x", 3), ("model_y", 1)])
    );
    let subset = settings(&format!("{GLOBAL}[requires]\nmodel_x = 2\n"));
    assert_eq!(
        subset.requirements(&two_classes()),
        counts(&[("model_x", 2)])
    );
}

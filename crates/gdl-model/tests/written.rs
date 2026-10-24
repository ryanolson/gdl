//! Each config surface reads back what it writes: a placement (the spec's
//! `[segment]`, `[routing]`, `[class]`, and `[cells]` tables), a fleet
//! file, and a cell's settings, each with every optional key set. Where a
//! surface leaves a default unsaid, its written form does too.

use std::collections::BTreeMap;
use std::time::Duration;

use gdl_model::{CellSettings, DirectoryBackend, Fleet, Placement};

const PLACEMENT: &str = "[segment.a]\nnodes = ['S', 'F']\n[segment.d]\nnodes = ['D']\n\
    [class.global]\nsegments = ['a']\n[class.model_x]\nsegments = ['d']\n\
    [routing]\nD = { selector = 'least_loaded' }\nF = 'random'\n\
    [cells.model_x]\npolicy = 'nearest'\n";

#[test]
fn a_placement_reads_back_what_it_writes() {
    let placement = Placement::from_toml(PLACEMENT).expect("a placement");
    let text = toml::to_string(&placement).expect("toml");
    let back = Placement::from_toml(&text).expect("reads what it wrote");
    assert_eq!(back, placement, "{text}");
    assert_eq!(back.hash(), placement.hash(), "{text}");
    // The hub reads it as JSON.
    let json = serde_json::to_string(&placement).expect("json");
    assert_eq!(
        serde_json::from_str::<Placement>(&json).expect("reads what it wrote"),
        placement,
        "{json}"
    );
}

/// A default router and a table of default cell policy values are the
/// placement that leaves them unsaid, and are not written.
#[test]
fn a_placement_leaves_its_defaults_unsaid() {
    let spelled = Placement::from_toml(
        "[segment.a]\nnodes = ['S']\n[segment.d]\nnodes = ['D']\n\
         [class.global]\nsegments = ['a']\n[class.model_x]\nsegments = ['d']\n\
         [routing]\nD = 'round_robin'\n\
         [cells.model_x]\npolicy = 'round_robin'\n",
    )
    .expect("a placement");
    let text = toml::to_string(&spelled).expect("toml");
    for unsaid in ["routing", "round_robin", "cells"] {
        assert!(!text.contains(unsaid), "{unsaid} in:\n{text}");
    }
    assert_eq!(Placement::from_toml(&text).expect("reads"), spelled);
}

const FLEET: &str = "[class.global]\nready = { a = 2 }\nrequires = { model_x = 1 }\n\
    [class.model_x]\nready = { d = 4 }\n\
    [cell.g]\nclass = 'global'\nregion = 'us-east-1'\nready = { a = 3 }\n\
    [cell.m]\nlayout = 'regional'\nclass = 'model_x'\nregion = 'us-east-1'\nhome = 'g'\nready = { d = 8 }\n";

#[test]
fn a_fleet_reads_back_what_it_writes() {
    let fleet = Fleet::from_toml(FLEET).expect("a fleet");
    let text = toml::to_string(&fleet).expect("toml");
    assert_eq!(Fleet::from_toml(&text).expect("reads"), fleet, "{text}");
    // Nothing optional is written when it is not set.
    let least = Fleet::from_toml("[class.model_x]\n[cell.g]\nclass = 'global'\nregion = 'r'\n")
        .expect("a fleet");
    let text = toml::to_string(&least).expect("toml");
    for unsaid in ["ready", "requires", "home", "layout"] {
        assert!(!text.contains(unsaid), "{unsaid} in:\n{text}");
    }
    assert_eq!(Fleet::from_toml(&text).expect("reads"), least, "{text}");
}

#[test]
fn a_cells_settings_read_back_what_they_write() {
    let every = CellSettings {
        name: "model-x-01-us-east-1".into(),
        layout: Some("regional".into()),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some("global-us-east-1".into()),
        directory: DirectoryBackend::Dir {
            path: "/srv/cells".into(),
        },
        ready: BTreeMap::from([("d".into(), 4)]),
        requires: BTreeMap::from([("model_y".into(), 1)]),
        hub: Some("model-x-01-hub:1337".into()),
        summary: Duration::from_millis(2500),
    };
    let text = toml::to_string(&every).expect("toml");
    let value: toml::Value = toml::from_str(&text).expect("toml");
    assert_eq!(CellSettings::read(value).expect("reads"), every, "{text}");
    let least = CellSettings {
        layout: None,
        home: None,
        directory: DirectoryBackend::default(),
        ready: BTreeMap::new(),
        requires: BTreeMap::new(),
        hub: None,
        summary: CellSettings::DEFAULT_SUMMARY,
        ..every
    };
    let text = toml::to_string(&least).expect("toml");
    for unsaid in ["layout", "home", "dir =", "ready", "requires", "hub"] {
        assert!(!text.contains(unsaid), "{unsaid} in:\n{text}");
    }
    let value: toml::Value = toml::from_str(&text).expect("toml");
    assert_eq!(CellSettings::read(value).expect("reads"), least, "{text}");
}

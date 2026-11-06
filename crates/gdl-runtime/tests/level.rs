//! The gdl level in this crate's manifest, `[package.metadata.gdl] level`.
//! The generator reads it from a pinned gdl, without building it, to
//! refuse a pin older than what it writes. A pin whose manifest has no
//! level is level 0, so the key must parse here, as a positive integer.

#[test]
fn the_manifest_states_a_gdl_level() {
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("the manifest");
    let manifest: toml::Table = toml::from_str(&text).expect("the manifest parses");
    let level = manifest
        .get("package")
        .and_then(|package| package.get("metadata"))
        .and_then(|metadata| metadata.get("gdl"))
        .and_then(|gdl| gdl.get("level"))
        .unwrap_or_else(|| panic!("no [package.metadata.gdl] level in\n{text}"));
    let level = level
        .as_integer()
        .unwrap_or_else(|| panic!("[package.metadata.gdl] level is {level}, not an integer"));
    assert!(level >= 1, "[package.metadata.gdl] level is {level}");
}

//! A fleet's Compose bundle, pinned byte for byte: what clusters in the
//! fleet file must not change.

use std::path::Path;

use super::{Mirror, ONE_FLEET, SPLIT, assert_pinned, fleet_bundle, pinned, placed};

/// The pin of [`ONE_FLEET`]'s bundle over [`SPLIT`].
const FLEET_PIN: &str = "tests/fixtures/bundle/fleet.txt";

/// A fleet without clusters composes as it always has, byte for byte:
/// every file of the bundle of a parent pool and its two children is
/// pinned, so that a fleet's other tables change nothing of it.
#[test]
fn a_fleets_bundle_is_pinned() {
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let (files, _) = fleet_bundle(&mirror, &placed(&mirror, SPLIT), &fleet);
    assert_pinned(&pinned(&mirror, &files), Path::new(FLEET_PIN));
}

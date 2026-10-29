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

/// Compose reads only a cluster's region, which its pools take: the same
/// fleet with its region in a cluster, and every other key of a cluster
/// set, writes the pinned bundle unchanged.
#[test]
fn a_fleet_with_clusters_composes_the_same_bundle() {
    let clustered = format!(
        "[fleet]\nname = \"api\"\n\
         [cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\n\
         registry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\
         service_account_annotations = {{ \"eks.amazonaws.com/role-arn\" = \"arn:aws:iam::0:role/gdl\" }}\n\
         secrets = {{ fingerprint = \"api-fingerprint\" }}\n\
         env = {{ SET_RECORDS_TABLE = \"api-pins\" }}\npins_local = true\n{}",
        ONE_FLEET.replace("region = \"us-east-1\"", "cluster = \"use1\"")
    );
    assert!(
        !clustered.contains("region = \"us-east-1\"\nhome"),
        "{clustered}"
    );
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(&clustered).expect("a fleet");
    let (files, _) = fleet_bundle(&mirror, &placed(&mirror, SPLIT), &fleet);
    assert_pinned(&pinned(&mirror, &files), Path::new(FLEET_PIN));
}

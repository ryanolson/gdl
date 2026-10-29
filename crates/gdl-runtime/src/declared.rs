//! A segment's property declarations, as its graph says them: the kinds
//! its nodes provide, which its instance may set, and the graph's kinds,
//! which its nodes and pool policies may read. See
//! `docs/src/concepts/pools.md`.

use gdl::{Error, ErrorKind, FollowedPools, InstanceProperties, PropertyType, Result};
use gdl_model::{Manifest, Placement};

/// Hold `properties` and `followed` to the declarations of `manifest`
/// for `segment` under `placement`, each kind with its type in `types`,
/// the contract's `property_types()`: `properties` sets only the kinds a
/// node of the segment provides, and `followed` registers only the
/// graph's kinds, each with its declared type. What either took before is
/// held to them too. A segment binary calls it before it builds its nodes,
/// and a generated `start` before it deploys.
pub fn declare_properties(
    manifest: &Manifest,
    placement: &Placement,
    segment: &str,
    types: &[PropertyType],
    properties: &InstanceProperties,
    followed: &FollowedPools,
) -> Result<()> {
    let typed = |kind: &str| {
        types
            .iter()
            .find(|ty| ty.kind() == kind)
            .copied()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Placement,
                    format!("property kind `{kind}` is declared without a type: the types are the contract's `property_types()`"),
                )
            })
    };
    let hosted: Vec<&String> = placement
        .segments()
        .filter(|(name, _)| *name == segment)
        .flat_map(|(_, nodes)| nodes)
        .collect();
    let mut provided = Vec::new();
    let mut declared = Vec::with_capacity(manifest.properties.len());
    for property in &manifest.properties {
        let ty = typed(&property.kind)?;
        if property.providers.iter().any(|node| hosted.contains(&node)) {
            provided.push(ty);
        }
        declared.push(ty);
    }
    properties.declare(provided)?;
    followed.kinds().declare(declared)
}

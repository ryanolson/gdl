//! A segment's property declarations, as its graph says them: the kinds
//! its nodes provide, which its instance may set, and the graph's kinds,
//! which its nodes and pool policies may read, and the class each reader's
//! need names. See `docs/src/concepts/pools.md`.

use gdl::{Error, ErrorKind, FollowedPools, InstanceProperties, PropertyType, Result};
use gdl_model::{Manifest, Placement};

/// What one entry of a service says it needs: a kind, the class whose
/// pools its adapters read it from, and those adapters, each as the
/// binary's `build` names it. An entry with no adapter has none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryNeed {
    /// The property kind.
    pub kind: &'static str,
    /// The class that supplies it.
    pub class: &'static str,
    /// The entry's adapters.
    pub adapters: &'static [&'static str],
}

/// Hold `properties` and `followed` to the declarations of `manifest`
/// for `segment` under `placement`, each kind with its type in `types`,
/// the contract's `property_types()`: `properties` sets only the kinds a
/// node of the segment provides, and `followed` registers only the
/// graph's kinds, each with its declared type, and for a node only those
/// that a node of the segment or one of `entry_needs`, what its entry
/// adapters need, says it needs. What either took before is held to them
/// too. Each node and entry adapter gets the class its own need names
/// ([`FollowedPools::needed_class`]), read through
/// [`Builder::build`](crate::Builder::build). A segment binary calls it before it builds its nodes, a generated
/// `start` before it deploys, and a generated `assemble` again
/// ([`Deployment::declare_properties`](crate::Deployment::declare_properties)),
/// so a deployment made by hand is held to the contract as well.
pub fn declare_properties(
    manifest: &Manifest,
    placement: &Placement,
    segment: &str,
    types: &[PropertyType],
    entry_needs: &[EntryNeed],
    properties: &InstanceProperties,
    followed: &FollowedPools,
) -> Result<()> {
    let hosted: Vec<&String> = placement
        .segments()
        .filter(|(name, _)| *name == segment)
        .flat_map(|(_, nodes)| nodes)
        .collect();
    let hosts = |node: &str| hosted.iter().any(|hosted| *hosted == node);
    declare(manifest, &hosts, types, entry_needs, properties, followed)
}

/// [`declare_properties`] for the segment whose nodes `hosts` tells.
pub(crate) fn declare(
    manifest: &Manifest,
    hosts: &dyn Fn(&str) -> bool,
    types: &[PropertyType],
    entry_needs: &[EntryNeed],
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
    let mut provided = Vec::new();
    let mut needed: Vec<&str> = entry_needs.iter().map(|need| need.kind).collect();
    let mut readers: Vec<(&str, &str, &str)> = entry_needs
        .iter()
        .flat_map(|need| {
            need.adapters
                .iter()
                .map(|adapter| (*adapter, need.kind, need.class))
        })
        .collect();
    let mut declared = Vec::with_capacity(manifest.properties.len());
    for property in &manifest.properties {
        let ty = typed(&property.kind)?;
        if property.providers.iter().any(|node| hosts(node)) {
            provided.push(ty);
        }
        for need in property.needs.iter().filter(|need| hosts(&need.node)) {
            needed.push(&property.kind);
            readers.push((&need.node, &property.kind, &need.class));
        }
        declared.push(ty);
    }
    properties.declare(provided)?;
    followed.kinds().declare(declared, &needed)?;
    for (reader, kind, class) in readers {
        followed.kinds().declare_need(reader, kind, class);
    }
    Ok(())
}

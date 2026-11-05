//! Unary nodes and `calls` against a manifest and a placement. A caller
//! calls the copy of a service in its own segment when the segment lists
//! one; otherwise the one other segment that does. See
//! `docs/src/concepts/placement.md`.

use std::collections::{BTreeMap, BTreeSet};

use crate::manifest::{Manifest, TypeInfo};
use crate::{PlacementError, Role, WireReason, check_calls};

/// The segments that list each unary node.
pub(super) type Services = BTreeMap<String, BTreeSet<String>>;

/// The manifest's own rules for unary nodes: each has its types, and only
/// it; neither type is streamed; no `calls` lists a name twice; and
/// [`check_calls`].
pub(super) fn check_graph(manifest: &Manifest) -> Vec<PlacementError> {
    let mut errors = Vec::new();
    let mut bad = |why: String| errors.push(PlacementError::InvalidManifest(why));
    for node in &manifest.nodes {
        match (&node.unary, node.role) {
            (None, Role::Unary) => bad(format!(
                "unary node `{}` has no request and response types",
                node.name
            )),
            (Some(_), Role::Unary) | (None, _) => {}
            (Some(_), role) => bad(format!(
                "node `{}` is a {role}, but has a unary node's types",
                node.name
            )),
        }
        if let Some(unary) = &node.unary {
            for (what, ty) in [("request", &unary.req), ("response", &unary.resp)] {
                if ty.stream.is_some() {
                    bad(format!(
                        "unary node `{}`: its {what} `{ty}` is streamed; a unary node takes one value and returns one",
                        node.name
                    ));
                }
            }
        }
        for (index, service) in node.calls.iter().enumerate() {
            if node.calls[..index].contains(service) {
                bad(format!("node `{}` has `calls {service}` twice", node.name));
            }
        }
    }
    let unary = |name: &str| manifest.node(name).is_some_and(|n| n.role == Role::Unary);
    for property in &manifest.properties {
        let kind = &property.kind;
        let provides = property
            .providers
            .iter()
            .map(|node| (node, format!("provides {kind}")));
        let needs = property
            .needs
            .iter()
            .map(|need| (&need.node, format!("needs {kind} from {}", need.class)));
        for (node, clause) in provides.chain(needs) {
            if unary(node) {
                errors.push(PlacementError::UnaryProperty {
                    node: node.clone(),
                    clause,
                });
            }
        }
    }
    let nodes: Vec<(&str, Role, &[String])> = manifest
        .nodes
        .iter()
        .map(|node| (node.name.as_str(), node.role, node.calls.as_slice()))
        .collect();
    errors.extend(check_calls(&nodes));
    errors
}

/// Resolve each call from each segment that holds its caller: to the copy
/// in the caller's segment, or else to the one other segment that lists
/// the service. A remote call encodes its request and its response, and
/// stays in its class. Callers are judged in name order, so the errors
/// read in an order that does not depend on the graph's.
pub(super) fn judge(
    manifest: &Manifest,
    segment_of: &BTreeMap<String, String>,
    services: &Services,
    class_of: &BTreeMap<String, String>,
    errors: &mut Vec<PlacementError>,
) {
    let callers: BTreeMap<&str, _> = manifest
        .nodes
        .iter()
        .filter(|node| !node.calls.is_empty())
        .map(|node| (node.name.as_str(), node))
        .collect();
    let mut found = Vec::new();
    for (caller, node) in callers {
        let segments: Vec<&String> = match services.get(caller) {
            Some(segments) => segments.iter().collect(),
            None => segment_of.get(caller).into_iter().collect(),
        };
        for segment in segments {
            for service in &node.calls {
                // A service in no segment is reported as unplaced.
                let Some(copies) = services.get(service) else {
                    continue;
                };
                if copies.contains(segment) {
                    continue;
                }
                let [other] = copies.iter().collect::<Vec<_>>()[..] else {
                    found.push(PlacementError::CallAmbiguous {
                        node: caller.to_string(),
                        segment: segment.clone(),
                        service: service.clone(),
                        segments: copies.iter().cloned().collect(),
                    });
                    continue;
                };
                if let Some(unary) = manifest.node(service).and_then(|n| n.unary.as_ref()) {
                    for ty in [&unary.req, &unary.resp] {
                        require_wire(caller, service, ty, &mut found);
                    }
                }
                if let (Some(from), Some(into)) = (class_of.get(segment), class_of.get(other))
                    && from != into
                {
                    found.push(PlacementError::CallAcrossClasses {
                        node: caller.to_string(),
                        segment: segment.clone(),
                        service: service.clone(),
                        from: from.clone(),
                        into: into.clone(),
                    });
                }
            }
        }
    }
    // A caller in several segments that calls one copy from each finds the
    // same unencodable type once per segment.
    for error in found {
        if !errors.contains(&error) {
            errors.push(error);
        }
    }
}

fn require_wire(caller: &str, service: &str, ty: &TypeInfo, found: &mut Vec<PlacementError>) {
    if !ty.wire {
        found.push(PlacementError::NotWire {
            from: caller.to_string(),
            to: service.to_string(),
            ty: ty.name.clone(),
            reason: WireReason::Call,
        });
    }
}

/// Each segment's calls to a service in another segment: by the caller's
/// segment, each service it calls remotely and the one segment that hosts
/// the copy it calls. A call to the copy in the caller's own segment is not
/// here. [`judge`] has refused every call this cannot resolve.
pub(super) type Remote = BTreeMap<String, BTreeMap<String, String>>;

pub(super) fn remote(
    manifest: &Manifest,
    segment_of: &BTreeMap<String, String>,
    services: &Services,
) -> Remote {
    let mut remote = Remote::new();
    for node in manifest.nodes.iter().filter(|node| !node.calls.is_empty()) {
        let segments: Vec<&String> = match services.get(&node.name) {
            Some(segments) => segments.iter().collect(),
            None => segment_of.get(&node.name).into_iter().collect(),
        };
        for segment in segments {
            for service in &node.calls {
                let Some(copies) = services.get(service) else {
                    continue;
                };
                if copies.contains(segment) {
                    continue;
                }
                if let [other] = copies.iter().collect::<Vec<_>>()[..] {
                    remote
                        .entry(segment.clone())
                        .or_default()
                        .insert(service.clone(), other.clone());
                }
            }
        }
    }
    remote
}

//! The graph's rules for `calls`, without a placement: every name a
//! `calls` clause lists is a unary node, and no chain of calls leads back
//! to where it started. `graph!`, a graph file's parser, and a manifest's
//! check share them, so the three cannot disagree.

use std::collections::{BTreeMap, BTreeSet};

use crate::{PlacementError, Role};

/// Check each node's `calls`: `nodes` holds each node's name, role, and
/// the names its `calls` lists. Returns every unknown name, every node of
/// the topology named in `calls`, and every cycle, in that order.
pub fn check_calls<'a>(nodes: &[(&'a str, Role, &'a [String])]) -> Vec<PlacementError> {
    let roles: BTreeMap<&str, Role> = nodes.iter().map(|(name, role, _)| (*name, *role)).collect();
    let mut errors = Vec::new();
    let mut edges: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (node, _, calls) in nodes {
        for service in *calls {
            match roles.get(service.as_str()) {
                None => errors.push(PlacementError::UnknownService {
                    node: node.to_string(),
                    service: service.clone(),
                }),
                Some(Role::Unary) => edges.entry(node).or_default().push(service),
                Some(&role) => errors.push(PlacementError::CallsPipelineNode {
                    node: node.to_string(),
                    service: service.clone(),
                    role,
                }),
            }
        }
    }
    for nodes in cycles(&edges) {
        errors.push(PlacementError::CallCycle {
            nodes: nodes.into_iter().map(str::to_string).collect(),
        });
    }
    errors
}

/// Each cycle of `edges` that a depth-first walk in name order finds, each
/// starting at its first node in name order. Only unary nodes are called,
/// so a cycle holds only unary nodes. The walk keeps its own stack, so no
/// graph can overflow the caller's.
fn cycles<'a>(edges: &BTreeMap<&'a str, Vec<&'a str>>) -> Vec<Vec<&'a str>> {
    let mut found: BTreeSet<Vec<&str>> = BTreeSet::new();
    let mut done: BTreeSet<&str> = BTreeSet::new();
    for &root in edges.keys() {
        if done.contains(root) {
            continue;
        }
        // The path from the root, each node with the index of its next
        // edge to follow.
        let mut path: Vec<(&str, usize)> = vec![(root, 0)];
        while let Some((node, next)) = path.last_mut() {
            let callees = edges.get(*node).map(Vec::as_slice).unwrap_or_default();
            let Some(&callee) = callees.get(*next) else {
                done.insert(*node);
                path.pop();
                continue;
            };
            *next += 1;
            if let Some(at) = path.iter().position(|(on, _)| *on == callee) {
                let mut cycle: Vec<&str> = path[at..].iter().map(|(on, _)| *on).collect();
                let first = (0..cycle.len())
                    .min_by_key(|&index| cycle[index])
                    .unwrap_or_default();
                cycle.rotate_left(first);
                found.insert(cycle);
            } else if !done.contains(callee) {
                path.push((callee, 0));
            }
        }
    }
    found.into_iter().collect()
}

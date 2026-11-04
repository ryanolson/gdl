use std::collections::{BTreeMap, BTreeSet};

use crate::Role;
use crate::hash::Fnv;

use super::{NodeInfo, hash_type};

/// Hash the unary nodes and every node's `calls` after the properties:
/// each unary node in name order, with its role, request, response and
/// `idempotent`, then each (caller, service) pair in order. Neither the
/// order of the declarations nor the order of a `calls` list means
/// anything. The marker differs from the properties' marker, so neither
/// part can hash as the other. A graph with neither writes nothing, and
/// keeps the hash it had before unary nodes existed.
pub(super) fn hash(h: &mut Fnv, nodes: &[NodeInfo]) {
    let services: BTreeMap<&str, &NodeInfo> = nodes
        .iter()
        .filter(|node| node.role == Role::Unary)
        .map(|node| (node.name.as_str(), node))
        .collect();
    let calls: BTreeSet<(&str, &str)> = nodes
        .iter()
        .flat_map(|node| {
            node.calls
                .iter()
                .map(|service| (node.name.as_str(), service.as_str()))
        })
        .collect();
    if services.is_empty() && calls.is_empty() {
        return;
    }
    h.u64(u64::MAX - 1);
    h.u64(services.len() as u64);
    for (name, node) in services {
        h.str(name);
        h.str(node.role.name());
        match &node.unary {
            // `check_manifest` refuses a unary node without its types; the
            // hash only has to tell it apart.
            None => h.u64(0),
            Some(unary) => {
                h.u64(1);
                hash_type(h, &unary.req);
                hash_type(h, &unary.resp);
                h.u64(u64::from(unary.idempotent));
            }
        }
    }
    h.u64(calls.len() as u64);
    for (caller, service) in calls {
        h.str(caller);
        h.str(service);
    }
}

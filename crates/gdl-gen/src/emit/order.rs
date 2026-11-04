//! The order a segment builds the nodes it hosts in.

use gdl_model::{Manifest, NodeInfo, Role};

/// The nodes of `manifest` in `hosted` that take a binding, in the order a
/// segment builds them: the unary nodes first, each after the hosted
/// services it calls, in declaration order otherwise; then the others, in
/// declaration order. `calls` has no cycle, so each pass finds a service
/// whose hosted callees are all placed.
pub(super) fn build_order<'m>(manifest: &'m Manifest, hosted: &[String]) -> Vec<&'m NodeInfo> {
    let bound: Vec<&NodeInfo> = manifest
        .nodes
        .iter()
        .filter(|node| node.binding.is_some() && hosted.contains(&node.name))
        .collect();
    let (mut services, others): (Vec<_>, Vec<_>) =
        bound.into_iter().partition(|node| node.role == Role::Unary);
    let mut order: Vec<&NodeInfo> = Vec::new();
    while !services.is_empty() {
        let next = services
            .iter()
            .position(|node| {
                node.calls
                    .iter()
                    .all(|callee| !services.iter().any(|waiting| &waiting.name == callee))
            })
            .unwrap_or(0);
        order.push(services.remove(next));
    }
    order.extend(others);
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A segment builds its unary nodes first, each after the services it
    /// calls, whatever order the graph declares them in; then the others, in
    /// declaration order. A node the segment does not host, or that takes no
    /// binding, is not built.
    #[test]
    fn a_segment_builds_each_service_after_those_it_calls_then_its_callers() {
        let node = |name: &str, role, calls: &[&str]| NodeInfo {
            name: name.into(),
            role,
            binding: (role != Role::Source).then(|| name.to_lowercase()),
            calls: calls.iter().map(|c| c.to_string()).collect(),
            unary: None,
        };
        let manifest = Manifest {
            name: "ordered".into(),
            schema: 1,
            nodes: vec![
                node("S", Role::Source, &[]),
                node("Router", Role::Operator, &["Overlap", "Tokenize"]),
                node("Overlap", Role::Unary, &["Score", "Tokenize"]),
                node("Score", Role::Unary, &["Tokenize"]),
                node("Elsewhere", Role::Unary, &[]),
                node("Tokenize", Role::Unary, &[]),
                node("D", Role::Sink, &["Tokenize"]),
            ],
            hops: Vec::new(),
            properties: Vec::new(),
        };
        let hosted: Vec<String> = ["S", "Router", "Overlap", "Score", "Tokenize", "D"]
            .iter()
            .map(|n| n.to_string())
            .collect();
        let order: Vec<&str> = build_order(&manifest, &hosted)
            .iter()
            .map(|node| node.name.as_str())
            .collect();
        assert_eq!(order, ["Tokenize", "Score", "Overlap", "Router", "D"]);
    }
}

use std::collections::{BTreeMap, BTreeSet};

use crate::Role;
use crate::manifest::{HopInfo, Manifest};

use super::{ClassBoundary, PlacementError};

/// The hops that cross from one class into another, checking each against
/// the boundary rule, and the classes they cross between for a cycle.
///
/// The rule keeps every stage of a boundary hop but its inner end on the
/// outer end's instance: that instance chooses the pool, sends the one
/// `gdl.fwd` into it, and minted the anchor the inner end's output goes to,
/// so the only instance the other class must reach is the one that sent to
/// it. A backward stage in another segment of the outer end's class would
/// be provisioned first, and the inner end would have to reach its
/// instance, which never sent to it; a forward path that leaves the outer
/// end's segment and returns can return to another instance than the one
/// that chose.
///
/// A stage that is in no one segment, or whose segment has no class, is not
/// judged, so that it is reported once, as unplaced or unclassed.
pub(super) fn bound(
    manifest: &Manifest,
    segment_of: &BTreeMap<String, String>,
    class_of: &BTreeMap<String, String>,
    errors: &mut Vec<PlacementError>,
) -> Vec<ClassBoundary> {
    let class = |node: &str| {
        segment_of
            .get(node)
            .and_then(|segment| class_of.get(segment))
            .map(String::as_str)
    };
    let mut boundaries = Vec::new();
    let mut edges: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    // The boundary hop the current hop is nested under, and the classes it
    // crosses from and into. Each root's chain is walked once, outermost
    // hop first.
    let mut under: Option<(&HopInfo, &str, &str)> = None;
    for (index, hop) in manifest.hops.iter().enumerate() {
        if index == 0 || manifest.hops[index - 1].inner != hop.outer {
            under = None;
        }
        // The outer end of a nested hop is the inner end of the hop that
        // owns it, which was judged with that hop.
        let stages = || {
            hop.forward
                .iter()
                .chain(std::iter::once(&hop.inner))
                .chain(&hop.backward)
        };
        if let Some((boundary, from, into)) = under {
            for stage in stages() {
                if let Some(other) = class(stage).filter(|c| *c != into) {
                    errors.push(PlacementError::NestedOutsideClass {
                        outer: boundary.outer.clone(),
                        inner: boundary.inner.clone(),
                        from: from.to_string(),
                        into: into.to_string(),
                        stage: stage.clone(),
                        segment: segment_of[stage].clone(),
                        class: other.to_string(),
                    });
                }
            }
            continue;
        }
        let Some(from) = class(&hop.outer) else {
            continue;
        };
        let inner = class(&hop.inner);
        let into = match inner.filter(|c| *c != from) {
            Some(into) => into,
            None => {
                let Some((stage, into)) = stages()
                    .filter_map(|stage| Some((stage, class(stage)?)))
                    .find(|(_, c)| *c != from)
                else {
                    continue;
                };
                if inner.is_some() {
                    errors.push(PlacementError::InnerEndInOuterClass {
                        outer: hop.outer.clone(),
                        inner: hop.inner.clone(),
                        from: from.to_string(),
                        into: into.to_string(),
                        stage: stage.clone(),
                    });
                }
                into
            }
        };
        let outer_segment = &segment_of[&hop.outer];
        for (role, stages) in [
            (Role::Forward, &hop.forward),
            (Role::Backward, &hop.backward),
        ] {
            for stage in stages {
                if segment_of.get(stage).is_some_and(|s| s != outer_segment) {
                    errors.push(PlacementError::StageOffOuterSegment {
                        outer: hop.outer.clone(),
                        inner: hop.inner.clone(),
                        from: from.to_string(),
                        into: into.to_string(),
                        stage: stage.clone(),
                        role,
                        segment: segment_of[stage].clone(),
                        outer_segment: outer_segment.clone(),
                    });
                }
            }
        }
        if inner == Some(into) {
            edges.entry(from).or_default().insert(into);
            boundaries.push(ClassBoundary {
                hop: index,
                from: from.to_string(),
                into: into.to_string(),
            });
            under = Some((hop, from, into));
        }
    }
    for classes in cycles(&edges) {
        errors.push(PlacementError::ClassCycle { classes });
    }
    boundaries
}

/// The classes on each cycle of `edges`, one entry per set of classes that
/// reach each other, in name order. Tarjan's strongly connected
/// components: each class and each edge is visited once, so the check grows
/// with the boundaries, where keeping what each class reaches would grow
/// with their square on a chain of classes.
fn cycles(edges: &BTreeMap<&str, BTreeSet<&str>>) -> Vec<Vec<String>> {
    let names: Vec<&str> = edges
        .iter()
        .flat_map(|(from, into)| std::iter::once(*from).chain(into.iter().copied()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let at = |name: &str| names.binary_search(&name).expect("a class of an edge");
    let mut next: Vec<Vec<usize>> = vec![Vec::new(); names.len()];
    for (from, into) in edges {
        next[at(from)].extend(into.iter().map(|name| at(name)));
    }

    const UNSEEN: usize = usize::MAX;
    let mut order = vec![UNSEEN; names.len()];
    let mut low = vec![0; names.len()];
    let mut open = vec![false; names.len()];
    let mut stack = Vec::new();
    let mut seen = 0;
    let mut cycles = Vec::new();
    for root in 0..names.len() {
        if order[root] != UNSEEN {
            continue;
        }
        // Each frame is a class and how many of its edges it has followed:
        // an explicit stack, so that a long chain of classes cannot
        // overflow the thread's.
        let mut frames = vec![(root, 0)];
        order[root] = seen;
        low[root] = seen;
        seen += 1;
        stack.push(root);
        open[root] = true;
        while let Some(frame) = frames.last_mut() {
            let (class, followed) = *frame;
            if let Some(&to) = next[class].get(followed) {
                frame.1 += 1;
                if order[to] == UNSEEN {
                    order[to] = seen;
                    low[to] = seen;
                    seen += 1;
                    stack.push(to);
                    open[to] = true;
                    frames.push((to, 0));
                } else if open[to] {
                    low[class] = low[class].min(order[to]);
                }
                continue;
            }
            frames.pop();
            if let Some(&(parent, _)) = frames.last() {
                low[parent] = low[parent].min(low[class]);
            }
            if low[class] == order[class] {
                let mut component = Vec::new();
                while let Some(member) = stack.pop() {
                    open[member] = false;
                    component.push(member);
                    if member == class {
                        break;
                    }
                }
                if component.len() > 1 {
                    component.sort_unstable();
                    cycles.push(component);
                }
            }
        }
    }
    // Indices follow name order, so sorting them sorts by name.
    cycles.sort_unstable();
    cycles
        .into_iter()
        .map(|component| {
            component
                .into_iter()
                .map(|i| names[i].to_string())
                .collect()
        })
        .collect()
}

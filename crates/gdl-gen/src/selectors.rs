//! Selectors from crates of their own: a spec's `[selector.<name>]`
//! tables, checked, resolved against what each crate offers, and the
//! selectors each segment registers. A segment that sends through a
//! crossing whose `[routing]` router names a bound selector builds it from
//! its settings and registers it; no other segment depends on the crate.
//! A segment names the selector's type in its code, so the binary links
//! it: a registration that the crate made for itself, at link time, would
//! not be kept by a linker that sees nothing else of the crate used.

use std::collections::BTreeSet;

use crate::Offers;
use crate::policies::{misfit, offered, read_offer, type_problem};
use crate::spec::{Bind, Spec};

/// gdl's own selectors, which a placement names with no binding.
const GDLS: [&str; 2] = ["affinity", "least_loaded"];

/// The bound selectors segment `segment` registers under `layout`, each
/// once, by name: those of the crossings it sends.
pub(crate) fn registered<'s>(
    spec: &'s Spec,
    layout: &gdl_model::Layout,
    segment: &str,
) -> Vec<(&'s str, &'s Bind)> {
    let names: BTreeSet<&str> = layout
        .sent_selectors(segment)
        .map(|(_, _, name)| name)
        .collect();
    spec.selectors
        .iter()
        .filter(|(name, _)| names.contains(name.as_str()))
        .map(|(name, bind)| (name.as_str(), bind))
        .collect()
}

/// What is wrong with the spec's `[selector.<name>]` tables that needs
/// none of their crates read: a name a placement cannot use, gdl's own, or
/// one a node, an entry adapter, a `[config]` key, or a bound pool policy
/// already has; a type that is not in the table's crate; and a table no
/// layout's `[routing]` names.
pub(crate) fn problems(
    spec: &Spec,
    manifest: &gdl_model::Manifest,
    layouts: &crate::validate::Layouts,
) -> Vec<String> {
    let mut problems = Vec::new();
    for (name, bind) in &spec.selectors {
        let at = format!("[selector.{name}]");
        if let Err(why) = gdl_model::check_selector_name(name) {
            problems.push(format!("{at}: {why}"));
        }
        if GDLS.contains(&name.as_str()) {
            problems.push(format!(
                "{at}: `{name}` is gdl's own selector, which needs no binding"
            ));
        }
        // The segment builds the selector as it builds a node, an entry
        // adapter, or a pool policy, with its metrics under the name: one
        // name must not mean two of them.
        let adapter = spec
            .service
            .as_ref()
            .is_some_and(|service| service.nodes().iter().any(|n| n.name == *name));
        if manifest.node(name).is_some()
            || adapter
            || spec.config.contains_key(name)
            || spec.pool_policies.contains_key(name)
        {
            problems.push(format!(
                "{at}: the segment builds a node, entry adapter, or pool policy `{name}`; name the selector apart from it"
            ));
        }
        problems.extend(type_problem(bind).map(|problem| format!("{at}: {problem}")));
        // Judged only where every layout fits its graph: one that does not
        // routes no crossing, and is refused for what it is.
        let named = layouts.values().any(|layout| {
            layout.as_ref().is_none_or(|layout| {
                (0..layout.hops().len()).any(|hop| {
                    layout.hops()[hop].forward_runs.iter().skip(1).any(|run| {
                        *layout.request_router(hop, run.start)
                            == gdl_model::RequestRouter::Selector(name.clone())
                    })
                })
            })
        });
        if !named {
            problems.push(format!(
                "{at}: no `[routing]` router names `{name}`, so no segment registers it"
            ));
        }
    }
    problems
}

/// What is wrong with each `[selector.<name>]` table whose crate the
/// reader can reach: a crate it cannot read, a type that is not a selector
/// the crate offers, and a selector whose `Head` does not fit a request
/// that crosses through it. The heads are checked only where `resolution`
/// gives the graph's messages their identity: a graph file's. Also the
/// property kinds each selector the reader found needs, by name, as its
/// `NEEDS` names them.
pub(crate) fn resolve(
    spec: &Spec,
    target: &std::path::Path,
    from: Offers<'_>,
    manifest: &gdl_model::Manifest,
    resolution: Option<&crate::Resolution>,
) -> (Vec<String>, std::collections::BTreeMap<String, Vec<String>>) {
    let mut problems = Vec::new();
    let mut needs = std::collections::BTreeMap::new();
    for (name, bind) in &spec.selectors {
        let at = format!("[selector.{name}]");
        let offer = match read_offer(spec, &at, bind, target, from) {
            Ok(Some(offer)) => offer,
            Ok(None) => continue,
            Err(unread) => {
                problems.extend(unread);
                continue;
            }
        };
        match offered(
            &offer,
            &offer.selectors,
            &offer.skipped_selectors,
            bind,
            "selector",
        ) {
            Err(problem) => problems.push(format!("{at}: {problem}")),
            Ok(selector) => {
                needs.insert(name.clone(), selector.needs.clone());
                problems.extend(
                    resolution
                        .into_iter()
                        .flat_map(|resolution| {
                            misfits(spec, manifest, resolution, name, &selector.head)
                        })
                        .map(|problem| format!("{at}: {problem}")),
                )
            }
        }
    }
    (problems, needs)
}

/// Why selector `name`, which reads `head`, does not fit a request that
/// crosses through it, in any layout of `spec`: the checks a segment that
/// sends through it makes when it starts. The request is the edge that
/// enters the crossing's first stage. Each problem is given once, however
/// many layouts find it.
fn misfits(
    spec: &Spec,
    manifest: &gdl_model::Manifest,
    resolution: &crate::Resolution,
    name: &str,
    head: &gdl_extract::PolicyHead,
) -> Vec<String> {
    let mut problems = Vec::new();
    // A layout that does not fit its graph is refused for what it is.
    for laid in spec.layouts() {
        let Ok(layout) = gdl_model::layout(manifest, laid.placement) else {
            continue;
        };
        let crossings = laid
            .placement
            .segments()
            .flat_map(|(segment, _)| layout.sent_selectors(segment).collect::<Vec<_>>());
        for (hop, at, router) in crossings {
            if router != name {
                continue;
            }
            let hop = &manifest.hops[hop];
            let (Some(edge), Some(node)) = (
                at.checked_sub(1)
                    .and_then(|edge| hop.forward_types.get(edge)),
                hop.forward_path().nth(at),
            ) else {
                continue;
            };
            let Some(problem) = misfit(resolution, edge, head) else {
                continue;
            };
            let problem = format!("selector `{name}` of the crossing into node `{node}` {problem}");
            if !problems.contains(&problem) {
                problems.push(problem);
            }
        }
    }
    problems
}

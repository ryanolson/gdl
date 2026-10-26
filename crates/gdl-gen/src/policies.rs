//! Cell policies from crates of their own: a spec's `[cell_policy.<name>]`
//! tables, checked, resolved against what each crate offers, and the
//! policies each segment registers. A segment that chooses cells of a class
//! whose `[cells.<class>] policy` a table binds builds that policy from its
//! settings and registers it; no other segment depends on the crate.

use gdl_extract::Offer;

use crate::Offers;
use crate::spec::{Bind, ListedCrate, Spec};
use crate::validate::type_crate;

/// The bound policies segment `segment` registers under `layout`, each once,
/// by name: those of the classes it chooses cells of.
pub(crate) fn registered<'s>(
    spec: &'s Spec,
    layout: &gdl_model::Layout,
    segment: &str,
) -> Vec<(&'s str, &'s Bind)> {
    let names: std::collections::BTreeSet<&str> = layout
        .chosen_classes(segment)
        .into_iter()
        .filter_map(|class| layout.cell_policy(class))
        .map(|policy| policy.policy.as_str())
        .collect();
    spec.cell_policies
        .iter()
        .filter(|(name, _)| names.contains(name.as_str()))
        .map(|(name, bind)| (name.as_str(), bind))
        .collect()
}

/// What is wrong with the spec's `[cell_policy.<name>]` tables that needs
/// none of their crates read: a name a placement cannot use or a node
/// already has, a type that is not in the table's crate, and a table no
/// layout's placement names.
pub(crate) fn problems(
    spec: &Spec,
    manifest: &gdl_model::Manifest,
    layouts: &crate::validate::Layouts,
) -> Vec<String> {
    let mut problems = Vec::new();
    for (name, bind) in &spec.cell_policies {
        let at = format!("[cell_policy.{name}]");
        if let Err(why) = gdl_model::check_cell_policy_name(name) {
            problems.push(format!("{at}: {why}"));
        }
        if ["home_first", "round_robin"].contains(&name.as_str()) {
            problems.push(format!(
                "{at}: `{name}` is gdl's own policy, which needs no binding"
            ));
        }
        // The segment builds the policy as it builds a node or an entry
        // adapter, with the graph's config values and its metrics under the
        // name: one name must not mean both.
        let adapter = spec
            .service
            .as_ref()
            .is_some_and(|service| service.nodes().iter().any(|n| n.name == *name));
        if manifest.node(name).is_some() || adapter || spec.config.contains_key(name) {
            problems.push(format!(
                "{at}: the segment builds a node or entry adapter `{name}`; name the policy apart from it"
            ));
        }
        match type_crate(&bind.ty) {
            _ if bind.ty.trim().is_empty() => problems.push(format!("{at}: `type` is empty")),
            None => problems.push(format!("{at}: type `{}` is not a Rust type path", bind.ty)),
            Some(krate) if krate != bind.lib => problems.push(format!(
                "{at}: type `{}` is not in crate `{}`",
                bind.ty, bind.lib
            )),
            Some(_) => {}
        }
        // Judged only where every layout fits its graph: one that does not
        // names no class's policy, and is refused for what it is.
        let named = layouts.values().any(|layout| {
            layout.as_ref().is_none_or(|layout| {
                layout
                    .boundaries()
                    .iter()
                    .filter_map(|boundary| layout.cell_policy(&boundary.into))
                    .any(|policy| policy.policy == *name)
            })
        });
        if !named {
            problems.push(format!(
                "{at}: no `[cells.<class>] policy` names `{name}`, so no segment registers it"
            ));
        }
    }
    problems
}

/// What is wrong with each `[cell_policy.<name>]` table whose crate the
/// reader can reach: a crate it cannot read, and a type that is not a cell
/// policy the crate offers. Offers come from `from`, as a node crate's do.
pub(crate) fn resolve(spec: &Spec, target: &std::path::Path, from: Offers<'_>) -> Vec<String> {
    let mut problems = Vec::new();
    for (name, bind) in &spec.cell_policies {
        let at = format!("[cell_policy.{name}]");
        // A crate whose manifest could not be read was reported when the
        // spec was loaded.
        if spec
            .problems
            .iter()
            .any(|p| p.starts_with(&format!("{at}: ")))
        {
            continue;
        }
        let listed = ListedCrate {
            package: bind.package.clone(),
            lib: bind.lib.clone(),
            source: bind.source.clone(),
        };
        match crate::offers(std::slice::from_ref(&listed), target, from) {
            Ok(offers) => {
                if let Some(problem) = offers.first().and_then(|offer| missing(offer, bind)) {
                    problems.push(format!("{at}: {problem}"));
                }
            }
            Err(error) => problems.extend(
                error
                    .0
                    .iter()
                    .map(|problem| format!("{at}: crate `{}`: {problem}", bind.package)),
            ),
        }
    }
    problems
}

/// Why `offer` holds no cell policy for `bind`'s type, if it does not. The
/// reader offers a policy only where the crate root reaches it by its
/// name, so the type must be written as that path: the crate, then the
/// name.
fn missing(offer: &Offer, bind: &Bind) -> Option<String> {
    let unraw = |path: &str| path.trim().trim_start_matches("::").replace("r#", "");
    let written = unraw(&bind.ty);
    let at_root = |name: &str| format!("{}::{name}", unraw(&bind.lib));
    let name = written.rsplit("::").next().unwrap_or_default().to_string();
    if offer
        .policies
        .iter()
        .any(|policy| at_root(&policy.name) == written)
    {
        return None;
    }
    if offer.policies.iter().any(|policy| policy.name == name) {
        return Some(format!(
            "crate `{}` offers cell policy `{name}` at `{}`; write that path",
            offer.package,
            at_root(&name)
        ));
    }
    if let Some(skipped) = offer.skipped_policies.iter().find(|s| s.name == name) {
        return Some(format!(
            "crate `{}` leaves `{name}` out: {}",
            offer.package, skipped.problem
        ));
    }
    let offered: Vec<String> = offer
        .policies
        .iter()
        .map(|policy| format!("`{}`", policy.name))
        .collect();
    Some(format!(
        "crate `{}` implements no cell policy `{name}`; {}",
        offer.package,
        match offered.as_slice() {
            [] => "it implements none".to_string(),
            _ => format!("it implements {}", offered.join(", ")),
        }
    ))
}

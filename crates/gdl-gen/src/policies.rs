//! Pool policies from crates of their own: a spec's `[pool_policy.<name>]`
//! tables, checked, resolved against what each crate offers, and the
//! policies each segment registers. A segment that chooses pools of a class
//! whose `[pools.<class>] policy` a table binds builds that policy from its
//! settings and registers it; no other segment depends on the crate.

use gdl_extract::{Offer, Origin, PolicyEntry, PolicyHead};

use crate::Offers;
use crate::spec::{Bind, ListedCrate, Spec};
use crate::validate::type_crate;

/// The bound policies segment `segment` registers under `layout`, each once,
/// by name: those of the classes it chooses pools of.
pub(crate) fn registered<'s>(
    spec: &'s Spec,
    layout: &gdl_model::Layout,
    segment: &str,
) -> Vec<(&'s str, &'s Bind)> {
    let names: std::collections::BTreeSet<&str> = layout
        .chosen_classes(segment)
        .into_iter()
        .filter_map(|class| layout.pool_policy(class))
        .map(|policy| policy.policy.as_str())
        .collect();
    spec.pool_policies
        .iter()
        .filter(|(name, _)| names.contains(name.as_str()))
        .map(|(name, bind)| (name.as_str(), bind))
        .collect()
}

/// What is wrong with the spec's `[pool_policy.<name>]` tables that needs
/// none of their crates read: a name a placement cannot use or a node
/// already has, a type that is not in the table's crate, and a table no
/// layout's placement names.
pub(crate) fn problems(
    spec: &Spec,
    manifest: &gdl_model::Manifest,
    layouts: &crate::validate::Layouts,
) -> Vec<String> {
    let mut problems = Vec::new();
    for (name, bind) in &spec.pool_policies {
        let at = format!("[pool_policy.{name}]");
        if let Err(why) = gdl_model::check_pool_policy_name(name) {
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
                    .filter_map(|boundary| layout.pool_policy(&boundary.into))
                    .any(|policy| policy.policy == *name)
            })
        });
        if !named {
            problems.push(format!(
                "{at}: no `[pools.<class>] policy` names `{name}`, so no segment registers it"
            ));
        }
    }
    problems
}

/// What is wrong with each `[pool_policy.<name>]` table whose crate the
/// reader can reach: a crate it cannot read, a type that is not a pool
/// policy the crate offers, and a policy whose `Head` does not fit a
/// request that crosses into a class that names it. Offers come from
/// `from`, as a node crate's do. The heads are checked only where
/// `resolution` gives the graph's messages their identity: a graph file's.
pub(crate) fn resolve(
    spec: &Spec,
    target: &std::path::Path,
    from: Offers<'_>,
    manifest: &gdl_model::Manifest,
    resolution: Option<&crate::Resolution>,
) -> Vec<String> {
    let mut problems = Vec::new();
    for (name, bind) in &spec.pool_policies {
        let at = format!("[pool_policy.{name}]");
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
                let Some(offer) = offers.first() else {
                    continue;
                };
                match offered(offer, bind) {
                    Err(problem) => problems.push(format!("{at}: {problem}")),
                    Ok(policy) => problems.extend(
                        resolution
                            .into_iter()
                            .flat_map(|resolution| {
                                misfits(spec, manifest, resolution, name, &policy.head)
                            })
                            .map(|problem| format!("{at}: {problem}")),
                    ),
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

/// The pool policy `offer` holds for `bind`'s type, or why it holds none.
/// The reader offers a policy only where the crate root reaches it by its
/// name, so the type must be written as that path: the crate, then the
/// name.
fn offered<'o>(offer: &'o Offer, bind: &Bind) -> Result<&'o PolicyEntry, String> {
    let unraw = |path: &str| path.trim().trim_start_matches("::").replace("r#", "");
    let written = unraw(&bind.ty);
    let at_root = |name: &str| format!("{}::{name}", unraw(&bind.lib));
    let name = written.rsplit("::").next().unwrap_or_default().to_string();
    if let Some(policy) = offer
        .policies
        .iter()
        .find(|policy| at_root(&policy.name) == written)
    {
        return Ok(policy);
    }
    if offer.policies.iter().any(|policy| policy.name == name) {
        return Err(format!(
            "crate `{}` offers pool policy `{name}` at `{}`; write that path",
            offer.package,
            at_root(&name)
        ));
    }
    if let Some(skipped) = offer.skipped_policies.iter().find(|s| s.name == name) {
        return Err(format!(
            "crate `{}` leaves `{name}` out: {}",
            offer.package, skipped.problem
        ));
    }
    let offered: Vec<String> = offer
        .policies
        .iter()
        .map(|policy| format!("`{}`", policy.name))
        .collect();
    Err(format!(
        "crate `{}` implements no pool policy `{name}`; {}",
        offer.package,
        match offered.as_slice() {
            [] => "it implements none".to_string(),
            _ => format!("it implements {}", offered.join(", ")),
        }
    ))
}

/// Why policy `name`, which reads `head`, does not fit a request that
/// crosses into a class that names it, in any layout of `spec`: the
/// checks a chooser makes when it starts. The crossing's request is the
/// last forward edge of its hop, since a hop crosses classes only at its
/// inner end. Each problem is given once, however many layouts find it.
fn misfits(
    spec: &Spec,
    manifest: &gdl_model::Manifest,
    resolution: &crate::Resolution,
    name: &str,
    head: &PolicyHead,
) -> Vec<String> {
    let mut problems = Vec::new();
    // A layout that does not fit its graph is refused for what it is.
    for layout in spec
        .layouts()
        .filter_map(|laid| gdl_model::layout(manifest, laid.placement).ok())
    {
        for boundary in layout.boundaries() {
            if layout
                .pool_policy(&boundary.into)
                .is_none_or(|policy| policy.policy != name)
            {
                continue;
            }
            let Some(edge) = manifest.hops[boundary.hop].forward_types.last() else {
                continue;
            };
            let Some(problem) = misfit(resolution, edge, head) else {
                continue;
            };
            let problem = format!(
                "pool policy `{name}` of class `{}` {problem}",
                boundary.into
            );
            if !problems.contains(&problem) {
                problems.push(problem);
            }
        }
    }
    problems
}

/// Why `head` cannot be read from a request of `edge`, if it cannot: a
/// message or a projection's `Of` that is not the edge's message, by
/// identity, `()` where a message crosses or the reverse, and a
/// projection, which reads fields by name, on an edge encoded as
/// `msgpack_array`. `Encoded` reads any head.
fn misfit(
    resolution: &crate::Resolution,
    edge: &gdl_model::TypeInfo,
    head: &PolicyHead,
) -> Option<String> {
    // `None` for `()`; an edge whose message the resolution does not hold
    // is not judged.
    let carried: Option<&Origin> = match edge.name.as_str() {
        "()" => None,
        path => Some(
            &resolution
                .messages
                .values()
                .find(|message| gdl_syntax::spelled(&message.path) == path)?
                .origin,
        ),
    };
    let (read, by_name): (Option<&Origin>, bool) = match head {
        PolicyHead::Encoded => return None,
        PolicyHead::Unit => (None, false),
        PolicyHead::Message(origin) => (Some(origin), false),
        PolicyHead::Projected { of, .. } => (Some(of), true),
    };
    let reads = |shown: &str| match head {
        PolicyHead::Projected { projection, .. } => {
            format!("reads `{projection}`, a part of {shown}")
        }
        _ => format!("reads the head as {shown}"),
    };
    if read != carried {
        let (reads_shown, carried_shown) = match (read, carried) {
            (Some(read), Some(carried)) => {
                let [a, b]: [String; 2] = Origin::shown_apart(&[read, carried])
                    .try_into()
                    .expect("one name for each origin");
                (a, b)
            }
            (read, carried) => (
                read.map_or("`()`".into(), Origin::shown),
                carried.map_or("`()`".into(), Origin::shown),
            ),
        };
        return Some(format!(
            "{}, but the request that crosses into it carries {carried_shown}",
            reads(&reads_shown)
        ));
    }
    if by_name && edge.encoding == gdl_model::Encoding::MsgpackArray {
        return Some(format!(
            "{}, by the names of its fields, but the request that crosses into it is encoded as `msgpack_array`, which carries no names",
            reads(&read.map_or("`()`".into(), Origin::shown))
        ));
    }
    None
}

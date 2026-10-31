use crate::{Layout, Manifest, RequestRouter};

/// A reader of a property kind that the graph does not draw: an entry
/// adapter, which runs in the segment that hosts the source, or a pool
/// policy, which runs wherever its class is chosen, or a selector, which
/// runs in the segment that sends the crossings it routes. Graph nodes say what
/// they need in the graph ([`crate::PropertyNeed`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyConsumer {
    /// What needs the kind, as a problem names it: ``entry `messages` ``,
    /// ``pool policy `router` ``.
    pub consumer: String,
    /// The kind it reads.
    pub kind: String,
    /// The class whose pools it reads the kind from.
    pub class: String,
    /// The segment it runs in, which must choose pools of the class to
    /// read them; `None` for a pool policy, which runs only where its
    /// class is chosen.
    pub segment: Option<String>,
}

/// The readers that selector `name`, which needs the kinds `needs`, is in
/// `layout`: for each crossing it routes, and each kind, one that reads the
/// kind from the class of the segment the crossing enters, and runs in the
/// segment that sends it. Each reader once. A placement without classes
/// has no class to read from: each kind is read from class `""` then, which
/// [`property_supply`] refuses, since no segment follows pools.
pub fn selector_consumers(layout: &Layout, name: &str, needs: &[&str]) -> Vec<PropertyConsumer> {
    let mut consumers: Vec<PropertyConsumer> = Vec::new();
    for (hop, plan) in layout.hops().iter().enumerate() {
        for pair in plan.forward_runs.windows(2) {
            let routed = matches!(
                layout.request_router(hop, pair[1].start),
                RequestRouter::Selector(router) if router == name
            );
            if !routed {
                continue;
            }
            let class = layout.class_of(&pair[1].segment).unwrap_or_default();
            for kind in needs {
                let consumer = PropertyConsumer {
                    consumer: format!("selector `{name}`"),
                    kind: (*kind).to_string(),
                    class: class.to_string(),
                    segment: Some(pair[0].segment.clone()),
                };
                if !consumers.contains(&consumer) {
                    consumers.push(consumer);
                }
            }
        }
    }
    consumers
}

/// What is wrong with how `layout` supplies the property kinds its graph's
/// nodes and the `outside` consumers need, and with what its
/// `[pools.<class>]` tables say their pools serve:
///
/// - each kind a consumer needs, or a table provides, is declared in the
///   graph;
/// - each class a consumer reads is a class of the layout that another
///   class sends to, since only such a class has pools that are chosen;
/// - a consumer that runs in one segment runs where its class is chosen;
/// - some entry segment of the class hosts a node that provides the kind,
///   or the class's `[pools.<class>] provides` names it. Only an entry
///   instance reports its properties, so a provider in another segment
///   supplies nothing.
///
/// Each problem names the kind, the consumer, and the class, once.
pub fn property_supply(
    manifest: &Manifest,
    layout: &Layout,
    outside: &[PropertyConsumer],
) -> Vec<String> {
    let mut problems = Vec::new();
    for boundary in layout.boundaries() {
        let Some(policy) = layout.pool_policy(&boundary.into) else {
            continue;
        };
        for kind in &policy.provides {
            let problem = format!(
                "`[pools.{}] provides` names property kind `{kind}`, which the graph does not declare",
                boundary.into
            );
            if manifest.property(kind).is_none() && !problems.contains(&problem) {
                problems.push(problem);
            }
        }
    }
    let graph = manifest.properties.iter().flat_map(|property| {
        property.needs.iter().map(|need| PropertyConsumer {
            consumer: format!("node `{}`", need.node),
            kind: property.kind.clone(),
            class: need.class.clone(),
            segment: layout.segment_of(&need.node).map(str::to_string),
        })
    });
    for consumer in graph.chain(outside.iter().cloned()) {
        if let Some(problem) = unsupplied(manifest, layout, &consumer)
            && !problems.contains(&problem)
        {
            problems.push(problem);
        }
    }
    problems
}

/// Why `consumer` does not find its kind supplied in `layout`, if it does
/// not.
fn unsupplied(manifest: &Manifest, layout: &Layout, consumer: &PropertyConsumer) -> Option<String> {
    let PropertyConsumer {
        consumer,
        kind,
        class,
        segment,
    } = consumer;
    let needs = format!("{consumer} needs property `{kind}` from class `{class}`");
    if class.is_empty() {
        return Some(format!(
            "{consumer} needs property `{kind}`, but the placement has no classes, so its segment follows no pools"
        ));
    }
    let Some(property) = manifest.property(kind) else {
        return Some(format!(
            "{consumer} needs property kind `{kind}`, which the graph does not declare"
        ));
    };
    let entries = layout.entry_segments(class);
    if entries.is_empty() {
        let known = layout.class_segments().contains_key(class.as_str());
        return Some(if known {
            format!(
                "{needs}, but no class sends to class `{class}`, so none of its pools is chosen"
            )
        } else {
            format!("{needs}, but the placement has no class `{class}`")
        });
    }
    if let Some(segment) = segment
        && !layout.chosen_classes(segment).contains(class.as_str())
    {
        return Some(format!(
            "{needs}, but its segment `{segment}` chooses no pool of class `{class}`"
        ));
    }
    let provided = property.providers.iter().any(|node| {
        layout
            .segment_of(node)
            .is_some_and(|segment| entries.contains(segment))
    });
    let declared = layout
        .pool_policy(class)
        .is_some_and(|policy| policy.provides.contains(kind));
    if provided || declared {
        return None;
    }
    let entries: Vec<String> = entries.iter().map(|s| format!("`{s}`")).collect();
    Some(format!(
        "{needs}, but no entry segment of class `{class}` ({}) hosts a node that provides it, and `[pools.{class}] provides` does not name it",
        entries.join(", ")
    ))
}

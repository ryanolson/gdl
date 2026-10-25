use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::Role;
use crate::hash::Fnv;
use crate::manifest::{HopInfo, Manifest, TypeInfo};
use crate::{Problem, Problems, read_toml};

mod boundary;
mod cells;
mod classes;
mod errors;
mod layouts;
mod report;
mod spelled;

use boundary::bound;
pub use cells::CellPolicy;
use classes::classify;
pub use errors::{PlacementError, PlacementErrors, WireReason};
pub use layouts::{Placements, check_layout_name};
pub use report::CrossingReport;
use report::hop_lines;
use spelled::Spelled;

/// Which nodes live in which segment.
///
/// A placement file contains `[segment.<name>]` tables, optional `[routing]`
/// policies, optional `[class.<name>]` tables that group the segments into
/// classes that deploy together, and optional `[cells.<class>]` tables that
/// say how a chooser picks a cell of a class it sends to. Other deployment
/// tables are ignored here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    #[serde(rename = "segment", default)]
    segments: BTreeMap<String, SegmentSpec>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    routing: BTreeMap<String, RequestRouter>,
    // Skipped when empty, so that a placement without classes writes the
    // text, and the hub reads the JSON, that it did before classes existed.
    #[serde(rename = "class", default, skip_serializing_if = "BTreeMap::is_empty")]
    classes: BTreeMap<String, ClassSpec>,
    // Skipped when empty for the same reason; a table of defaults is
    // dropped as it is read.
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "cells::recorded"
    )]
    cells: BTreeMap<String, CellPolicy>,
    #[serde(skip)]
    spelled: Spelled,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentSpec {
    nodes: Vec<String>,
}

// Unknown keys are refused: a cell policy written here by mistake, rather
// than under `[cells.<class>]`, would otherwise be dropped without a word.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassSpec {
    segments: Vec<String>,
}

/// Selection of a replica when a request crosses into another segment.
///
/// In a placement file a policy is a string (`"round_robin"`, `"random"`)
/// and a selector a table (`{ selector = "affinity" }`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestRouter {
    /// Cycle through replicas in roster order: [`RequestRouter::DEFAULT`].
    RoundRobin,
    /// Select a replica independently for each request hand-off.
    Random,
    /// The selector of this name, which the sending segment registers or
    /// gdl provides ([`SELECTORS`]).
    Selector(String),
}

impl RequestRouter {
    /// The router of a hop that `[routing]` does not name: round robin.
    pub const DEFAULT: Self = Self::RoundRobin;
}

impl Default for RequestRouter {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One of gdl's own selectors: a crossing can name it without anything
/// registering it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SelectorInfo {
    /// The name a placement uses.
    pub name: &'static str,
    /// What it does, in one sentence.
    pub summary: &'static str,
}

/// gdl's own selectors, in name order.
pub const SELECTORS: &[SelectorInfo] = &[
    SelectorInfo {
        name: "affinity",
        summary: "Send requests with the same key to the same replica, from every sender: the key is a metadata value when one is set, else the request's head.",
    },
    SelectorInfo {
        name: "least_loaded",
        summary: "Send each request to the replica with the fewest requests this sender has in flight there.",
    },
];

// Read by hand rather than derived: a derived enum reads a selector's
// table as one entry, so a second key is refused without its name.
impl<'de> Deserialize<'de> for RequestRouter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Router;
        impl<'de> serde::de::Visitor<'de> for Router {
            type Value = RequestRouter;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("\"round_robin\", \"random\", or { selector = \"<name>\" }")
            }

            fn visit_str<E: serde::de::Error>(self, policy: &str) -> Result<Self::Value, E> {
                match policy {
                    "round_robin" => Ok(RequestRouter::RoundRobin),
                    "random" => Ok(RequestRouter::Random),
                    other => Err(E::unknown_variant(other, &["round_robin", "random"])),
                }
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Selector {
                    selector: String,
                }
                let table =
                    Selector::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
                Ok(RequestRouter::Selector(table.selector))
            }
        }
        deserializer.deserialize_any(Router)
    }
}

/// The longest selector or class name.
const NAME_LIMIT: usize = 64;

/// Whether `name` has the shape of a selector's or a class's name. Both are
/// settings keys reachable from the environment, where case is lost and
/// `__` separates keys, so a name is lowercase ASCII letters and digits in
/// words joined by single `_`s, starting with a letter. `kind` names what
/// the name is for in the refusal.
fn check_name_shape(kind: &str, name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    let shaped = chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !name.contains("__")
        && !name.ends_with('_');
    if !shaped || name.len() > NAME_LIMIT {
        return Err(format!(
            "{kind} name `{name}` must be lowercase letters and digits in words joined by single `_`s, start with a letter, and be at most {NAME_LIMIT} characters"
        ));
    }
    Ok(())
}

/// Whether `name` can name a selector. A name is also the selector's
/// settings key (`[selector.<name>]`) and its metrics' endpoint label, so
/// it has a settings key's shape, and it is never a built-in policy's name.
pub fn check_selector_name(name: &str) -> Result<(), String> {
    check_name_shape("selector", name)?;
    if matches!(name, "round_robin" | "random") {
        return Err(format!(
            "selector name `{name}` is a built-in policy's: write `\"{name}\"`"
        ));
    }
    Ok(())
}

/// Whether `name` can name a class. A class name is a TOML key in the files
/// that deploy a class, and a value a setting in the environment carries,
/// so it has a settings key's shape. It never names a routing policy, so
/// the policies' names are not reserved.
pub fn check_class_name(name: &str) -> Result<(), String> {
    check_name_shape("class", name)
}

impl Placement {
    /// An empty placement.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a segment holding `nodes`.
    pub fn segment<S: Into<String>>(
        mut self,
        name: impl Into<String>,
        nodes: impl IntoIterator<Item = S>,
    ) -> Self {
        self.segments.insert(
            name.into(),
            SegmentSpec {
                nodes: nodes.into_iter().map(Into::into).collect(),
            },
        );
        self
    }

    /// Add a class: `segments` deploy together. Once a placement has a
    /// class, every segment must be in exactly one; [`layout`] refuses it
    /// otherwise.
    pub fn class<S: Into<String>>(
        mut self,
        name: impl Into<String>,
        segments: impl IntoIterator<Item = S>,
    ) -> Self {
        self.classes.insert(
            name.into(),
            ClassSpec {
                segments: segments.into_iter().map(Into::into).collect(),
            },
        );
        self
    }

    /// Set the request router for the boundary entering `node`. The default
    /// policy is not recorded: a placement that spells it out is the same
    /// placement, with the same hash, as one that leaves it unsaid.
    pub fn route(mut self, node: impl Into<String>, router: RequestRouter) -> Self {
        let node = node.into();
        if router == RequestRouter::default() {
            self.routing.remove(&node);
            self.spelled.routing.insert(node);
        } else {
            self.spelled.routing.remove(&node);
            self.routing.insert(node, router);
        }
        self
    }

    /// Set how a chooser picks a cell of `class`. Defaults are not
    /// recorded, as a default router is not: a value equal to its default
    /// is the same placement as one that leaves it unsaid, and setting the
    /// default policy removes the class's.
    pub fn cell_policy(mut self, class: impl Into<String>, policy: CellPolicy) -> Self {
        let class = class.into();
        if policy == CellPolicy::default() {
            self.cells.remove(&class);
            self.spelled.cells.insert(class);
        } else {
            self.spelled.cells.remove(&class);
            self.cells.insert(class, policy);
        }
        self
    }

    /// Parse segment placement, request routing, classes, and cell policies
    /// from a deployment file. Every problem with the names is reported,
    /// each where it is; a file that does not parse is one problem.
    pub fn from_toml(text: &str) -> Result<Self, Problems> {
        let placement: Self = read_toml(text)?;
        placement.finish(&toml::from_str(text).unwrap_or_default())
    }

    /// The placement as read from `written`, the table that holds its
    /// tables, with its defaults dropped and its names checked. Each
    /// problem's table is relative to `written`.
    fn finish(self, written: &toml::Table) -> Result<Self, Problems> {
        let mut placement = self;
        placement
            .routing
            .retain(|_, router| *router != RequestRouter::default());
        placement.spelled = Spelled::read(written, &placement.routing, &placement.cells);
        let mut problems = Vec::new();
        for (node, router) in &placement.routing {
            if let RequestRouter::Selector(name) = router
                && let Err(why) = check_selector_name(name)
            {
                problems.push(Problem::at(Some("routing".into()), Some(node.clone()), why));
            }
        }
        for name in placement.classes.keys() {
            if let Err(why) = check_class_name(name) {
                problems.push(Problem::at(Some(format!("class.{name}")), None, why));
            }
        }
        for (class, policy) in &placement.cells {
            if let Err(why) = cells::check_policy_name(policy) {
                problems.push(Problem::at(
                    Some(format!("cells.{class}")),
                    Some("policy".into()),
                    why,
                ));
            }
        }
        if problems.is_empty() {
            Ok(placement)
        } else {
            Err(Problems(problems))
        }
    }

    /// Segment names and their nodes, in name order.
    pub fn segments(&self) -> impl Iterator<Item = (&str, &[String])> {
        self.segments
            .iter()
            .map(|(name, spec)| (name.as_str(), spec.nodes.as_slice()))
    }

    /// Class names and their segments, in name order.
    pub fn classes(&self) -> impl Iterator<Item = (&str, &[String])> {
        self.classes
            .iter()
            .map(|(name, spec)| (name.as_str(), spec.segments.as_slice()))
    }

    /// The placement hash changes with node placement, segment names,
    /// request routing policies, classes, and each class's effective cell
    /// policy, a default one included. Node order inside a segment, and
    /// segment order inside a class, do not matter.
    pub fn hash(&self) -> u64 {
        let mut h = Fnv::new();
        for (name, spec) in &self.segments {
            h.str(name);
            let nodes: BTreeSet<&str> = spec.nodes.iter().map(String::as_str).collect();
            h.strs(&nodes.into_iter().collect::<Vec<_>>());
        }
        for (node, router) in &self.routing {
            h.str(node);
            match router {
                RequestRouter::RoundRobin => h.str("round_robin"),
                RequestRouter::Random => h.str("random"),
                // No selector name has a `:`, so no selector hashes as a
                // policy, and the policies keep the hash they had before.
                RequestRouter::Selector(name) => h.str(&format!("selector:{name}")),
            }
        }
        // A class lists segments as a segment lists nodes. The marker
        // starts the classes where a segment's or a router's name would
        // start, with a value no name's length can have, so no class hashes
        // as either. A placement without classes writes nothing here and
        // keeps the hash it had before classes existed.
        if !self.classes.is_empty() {
            h.u64(u64::MAX);
            h.u64(self.classes.len() as u64);
            for (name, spec) in &self.classes {
                h.str(name);
                let segments: BTreeSet<&str> = spec.segments.iter().map(String::as_str).collect();
                h.strs(&segments.into_iter().collect::<Vec<_>>());
            }
        }
        cells::hash(&mut h, self.classes.keys(), &self.cells);
        h.finish()
    }
}

/// A placement checked against a manifest, with every hop split into runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    roots: usize,
    contract_hash: u64,
    placement_hash: u64,
    segment_of: BTreeMap<String, String>,
    class_of: BTreeMap<String, String>,
    boundaries: Vec<ClassBoundary>,
    cell_policies: BTreeMap<String, CellPolicy>,
    hops: Vec<HopLayout>,
    routing: BTreeMap<(usize, usize), RequestRouter>,
}

/// A hop that crosses from one class into another. Under the boundary rule
/// its inner end is its only stage in the other class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassBoundary {
    /// The hop's index in [`Layout::hops`].
    pub hop: usize,
    /// The class of the hop's outer end.
    pub from: String,
    /// The class of the hop's inner end.
    pub into: String,
}

/// How one hop executes under a placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopLayout {
    /// Every stage is in the outer end's segment: the hop is a nested call.
    pub local: bool,
    /// The forward path (outer end, forward chain, inner end) split into
    /// same-segment runs.
    pub forward_runs: Vec<Run>,
    /// The backward path (inner end, backward chain, outer end) split into
    /// same-segment runs. Run 0 holds the inner end; the last holds the
    /// outer end.
    pub backward_runs: Vec<Run>,
    /// Backward runs the outer end opens before dispatch, in the order it
    /// opens them: from the run nearest the outer end to the run nearest
    /// the inner end, so each run's reply anchor already exists.
    pub provisioning: Vec<usize>,
    /// Whether each forward edge carries a streamed request, outer end
    /// first: one entry per forward edge.
    pub request_streams: Vec<bool>,
}

/// A maximal sequence of consecutive stages in one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    /// The segment.
    pub segment: String,
    /// Index of the run's first stage in its path.
    pub start: usize,
    /// The stages, in path order.
    pub stages: Vec<String>,
}

/// A path of a hop: the forward one, from the outer end to the inner end,
/// or the backward one, back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg {
    /// [`HopInfo::forward_path`], whose edges carry
    /// [`HopInfo::forward_types`].
    Forward,
    /// [`HopInfo::backward_path`], whose edges carry
    /// [`HopInfo::backward_types`].
    Backward,
}

/// An edge of a hop whose type must be a wire type under a placement:
/// edge `edge` of path `leg`, from its stage `edge` to its stage
/// `edge + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireEdge {
    /// The path.
    pub leg: Leg,
    /// The edge's index in the path's types.
    pub edge: usize,
    /// Why it must be.
    pub reason: WireReason,
}

impl HopLayout {
    /// The edges whose types must be wire types under this layout: every
    /// edge between two runs, and, when the hop is not local but its
    /// response never leaves one segment, the inner end's first edge,
    /// whose items reach the outer end through an anchor. The placement's
    /// check and the generated segments' crossing checks both read this,
    /// so that they cannot disagree.
    pub fn wire_edges(&self) -> Vec<WireEdge> {
        let mut edges = Vec::new();
        for (leg, runs) in [
            (Leg::Forward, &self.forward_runs),
            (Leg::Backward, &self.backward_runs),
        ] {
            for pair in runs.windows(2) {
                edges.push(WireEdge {
                    leg,
                    edge: pair[1].start - 1,
                    reason: WireReason::Crossing,
                });
            }
        }
        if !self.local && self.backward_runs.len() == 1 {
            // The response never crosses, but the inner end runs in a task
            // the forward path reached from another segment, so its output
            // still returns to the outer end through an anchor.
            edges.push(WireEdge {
                leg: Leg::Backward,
                edge: 0,
                reason: WireReason::AnchorCarried,
            });
        }
        edges
    }

    /// Segment crossings one request makes on this hop.
    pub fn crossings(&self) -> usize {
        if self.local {
            return 0;
        }
        (self.forward_runs.len() - 1) + (self.backward_runs.len() - 1)
    }

    /// Anchors one request mints on this hop: one per backward run after the
    /// first, and at least one whenever the hop is not local (the forward
    /// path left the outer end's segment, so the response returns through an
    /// anchor even if every backward stage is local).
    ///
    /// A streamed request adds one anchor at each forward crossing it
    /// passes: its receiver's input anchor.
    pub fn anchors(&self) -> usize {
        if self.local {
            0
        } else {
            (self.backward_runs.len() - 1).max(1) + self.request_anchors()
        }
    }

    /// Input anchors one request mints on this hop: one per forward
    /// crossing whose edge carries a streamed request.
    pub fn request_anchors(&self) -> usize {
        self.forward_runs
            .iter()
            .skip(1)
            .filter(|run| self.request_streams.get(run.start - 1) == Some(&true))
            .count()
    }
}

impl Layout {
    /// The contract hash of the manifest this layout was built from.
    pub fn contract_hash(&self) -> u64 {
        self.contract_hash
    }

    /// The hash of the placement this layout was built from.
    pub fn placement_hash(&self) -> u64 {
        self.placement_hash
    }

    /// The segment a node is placed in.
    pub fn segment_of(&self, node: &str) -> Option<&str> {
        self.segment_of.get(node).map(String::as_str)
    }

    /// The class a segment is in: `None` when the placement has no classes.
    pub fn class_of(&self, segment: &str) -> Option<&str> {
        self.class_of.get(segment).map(String::as_str)
    }

    /// Each class and its segments: empty when the placement has no
    /// classes.
    pub(crate) fn class_segments(&self) -> BTreeMap<&str, BTreeSet<&str>> {
        let mut classes: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for (segment, class) in &self.class_of {
            classes.entry(class).or_default().insert(segment);
        }
        classes
    }

    /// The segments `segment` sends to: the next run of each forward path
    /// it holds a run of, and the backward runs it opens as an outer end.
    /// Replies ride stream anchors back and are not sends. A forward path
    /// that leaves a segment and comes back makes two segments send to each
    /// other, so this relation can have cycles.
    pub fn sends_to(&self, segment: &str) -> BTreeSet<String> {
        let mut targets = BTreeSet::new();
        for hop in &self.hops {
            for pair in hop.forward_runs.windows(2) {
                if pair[0].segment == segment {
                    targets.insert(pair[1].segment.clone());
                }
            }
            if hop
                .forward_runs
                .first()
                .is_some_and(|run| run.segment == segment)
            {
                for &run in &hop.provisioning {
                    if let Some(run) = hop.backward_runs.get(run) {
                        targets.insert(run.segment.clone());
                    }
                }
            }
        }
        targets.remove(segment);
        targets
    }

    /// Whether work that `from` holds can reach `to` through one or more
    /// sends. A segment reaches itself only when a path leaves it and
    /// returns. A stopped instance waits for the sources that reach its
    /// segment, since only their requests can still arrive.
    pub fn reaches(&self, from: &str, to: &str) -> bool {
        let mut seen = BTreeSet::new();
        let mut next: Vec<String> = self.sends_to(from).into_iter().collect();
        while let Some(at) = next.pop() {
            if at == to {
                return true;
            }
            if seen.insert(at.clone()) {
                next.extend(self.sends_to(&at));
            }
        }
        false
    }

    /// The hops, outermost first.
    pub fn hops(&self) -> &[HopLayout] {
        &self.hops
    }

    /// Request router for the hand-off at a hop's forward position.
    pub fn request_router(&self, hop: usize, position: usize) -> &RequestRouter {
        static DEFAULT: RequestRouter = RequestRouter::RoundRobin;
        self.routing.get(&(hop, position)).unwrap_or(&DEFAULT)
    }

    /// How a chooser picks a cell of `class`, defaults applied: `None`
    /// unless a class boundary enters `class`.
    pub fn cell_policy(&self, class: &str) -> Option<&CellPolicy> {
        self.cell_policies.get(class)
    }

    /// The hops that cross from one class into another, in hop order:
    /// empty when the placement has no classes.
    pub fn boundaries(&self) -> &[ClassBoundary] {
        &self.boundaries
    }

    /// The entry segments of `class`: those holding the inner end of a hop
    /// that crosses into it, which is all a chooser in another class
    /// reaches. Empty for a class no hop enters.
    pub fn entry_segments(&self, class: &str) -> BTreeSet<&str> {
        self.boundaries
            .iter()
            .filter(|boundary| boundary.into == class)
            .filter_map(|boundary| self.hops[boundary.hop].backward_runs.first())
            .map(|inner| inner.segment.as_str())
            .collect()
    }

    /// Crossings and anchors for one request through each root and, when
    /// the placement has classes, the WAN round trips each class boundary
    /// adds when the request spills to a cell in another region.
    pub fn report(&self) -> CrossingReport {
        CrossingReport {
            roots: self.roots,
            hops: hop_lines(&self.hops, &self.boundaries),
            classed: !self.class_of.is_empty(),
        }
    }
}

/// Check `placement` against `manifest` and split every hop into runs.
///
/// When the placement has classes, every hop that crosses from one class
/// into another must cross only at its inner end: every other stage of the
/// hop is in its outer end's segment, and every hop nested under the inner
/// end stays in the inner end's class. The class boundaries must not make
/// a cycle.
pub fn layout(manifest: &Manifest, placement: &Placement) -> Result<Layout, PlacementErrors> {
    // A class groups segments and decides nowhere a node runs, so a class
    // or boundary problem stops none of the other checks: it is reported
    // with whatever they find.
    let mut class_errors = Vec::new();
    let class_of = classify(placement, &mut class_errors);
    cells::judge(placement, &mut class_errors);
    let mut errors = check_manifest(manifest);
    if !errors.is_empty() {
        errors.append(&mut class_errors);
        return Err(PlacementErrors(errors));
    }
    // A node in no segment, or in several, is judged nowhere, but the
    // boundary rule still judges every hop of the nodes that are placed.
    let segment_of = assign(manifest, placement, &mut errors);
    let boundaries = if placement.classes.is_empty() {
        Vec::new()
    } else {
        bound(manifest, &segment_of, &class_of, &mut class_errors)
    };
    let undecided = cells::Undecided::new(manifest, &segment_of, &class_of);
    let cell_policies = cells::entered(placement, &boundaries, &undecided, &mut class_errors);
    if !errors.is_empty() {
        errors.append(&mut class_errors);
        return Err(PlacementErrors(errors));
    }
    match place(manifest, placement, segment_of) {
        Ok(layout) if class_errors.is_empty() => Ok(Layout {
            class_of,
            boundaries,
            cell_policies,
            ..layout
        }),
        Ok(_) => Err(PlacementErrors(class_errors)),
        Err(PlacementErrors(mut errors)) => {
            errors.append(&mut class_errors);
            Err(PlacementErrors(errors))
        }
    }
}

/// The segment of each node that is in exactly one, checking that every
/// node of a sound manifest is.
fn assign(
    manifest: &Manifest,
    placement: &Placement,
    errors: &mut Vec<PlacementError>,
) -> BTreeMap<String, String> {
    let mut where_placed: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (segment, nodes) in placement.segments() {
        if nodes.is_empty() {
            errors.push(PlacementError::EmptySegment {
                segment: segment.to_string(),
            });
        }
        for node in nodes {
            if manifest.node(node).is_none() {
                errors.push(PlacementError::UnknownNode {
                    segment: segment.to_string(),
                    node: node.clone(),
                });
                continue;
            }
            where_placed.entry(node).or_default().push(segment);
        }
    }
    let mut segment_of = BTreeMap::new();
    for node in &manifest.nodes {
        match where_placed.get(node.name.as_str()).map(Vec::as_slice) {
            None | Some([]) => errors.push(PlacementError::Unplaced {
                node: node.name.clone(),
            }),
            Some([segment]) => {
                segment_of.insert(node.name.clone(), segment.to_string());
            }
            Some(segments) => errors.push(PlacementError::PlacedTwice {
                node: node.name.clone(),
                segments: segments.iter().map(ToString::to_string).collect(),
            }),
        }
    }
    segment_of
}

/// Every check that needs the nodes' segments but not their classes, and
/// the layout without classes.
fn place(
    manifest: &Manifest,
    placement: &Placement,
    segment_of: BTreeMap<String, String>,
) -> Result<Layout, PlacementErrors> {
    let mut errors = Vec::new();
    let hops: Vec<HopLayout> = manifest
        .hops
        .iter()
        .map(|hop| hop_layout(hop, &segment_of, &mut errors))
        .collect();
    if !errors.is_empty() {
        return Err(PlacementErrors(errors));
    }
    let mut routing = BTreeMap::new();
    for (node, router) in &placement.routing {
        if let RequestRouter::Selector(name) = router
            && let Err(reason) = check_selector_name(name)
        {
            errors.push(PlacementError::InvalidSelector {
                node: node.clone(),
                reason,
            });
            continue;
        }
        let mut found = false;
        for (index, hop) in hops.iter().enumerate() {
            for run in hop.forward_runs.iter().skip(1) {
                if run.stages.first() == Some(node) {
                    routing.insert((index, run.start), router.clone());
                    found = true;
                }
            }
        }
        if !found {
            errors.push(PlacementError::InvalidRouter(node.clone()));
        }
    }
    // A default router names a boundary as any other router does.
    for node in &placement.spelled.routing {
        let found = hops.iter().any(|hop| {
            hop.forward_runs
                .iter()
                .skip(1)
                .any(|run| run.stages.first() == Some(node))
        });
        if !found {
            errors.push(PlacementError::InvalidRouter(node.clone()));
        }
    }
    if !errors.is_empty() {
        return Err(PlacementErrors(errors));
    }
    Ok(Layout {
        roots: manifest
            .nodes
            .iter()
            .filter(|n| n.role == Role::Source)
            .count(),
        routing,
        contract_hash: manifest.contract_hash(),
        placement_hash: placement.hash(),
        segment_of,
        class_of: BTreeMap::new(),
        boundaries: Vec::new(),
        cell_policies: BTreeMap::new(),
        hops,
    })
}

fn hop_layout(
    hop: &HopInfo,
    segment_of: &BTreeMap<String, String>,
    errors: &mut Vec<PlacementError>,
) -> HopLayout {
    let forward: Vec<&str> = hop.forward_path().collect();
    let backward: Vec<&str> = hop.backward_path().collect();
    let forward_runs = runs(&forward, segment_of);
    let backward_runs = runs(&backward, segment_of);
    let local = forward_runs.len() == 1
        && backward_runs.len() == 1
        && forward_runs[0].segment == backward_runs[0].segment;

    let provisioning = if local || backward_runs.len() < 3 {
        Vec::new()
    } else {
        (1..backward_runs.len() - 1).rev().collect()
    };
    let layout = HopLayout {
        local,
        forward_runs,
        backward_runs,
        provisioning,
        request_streams: hop
            .forward_types
            .iter()
            .map(|t| t.stream.is_some())
            .collect(),
    };
    for wire in layout.wire_edges() {
        let (path, types) = match wire.leg {
            Leg::Forward => (&forward, &hop.forward_types),
            Leg::Backward => (&backward, &hop.backward_types),
        };
        require_wire(
            path[wire.edge],
            path[wire.edge + 1],
            &types[wire.edge],
            wire.reason,
            errors,
        );
    }
    layout
}

fn runs(path: &[&str], segment_of: &BTreeMap<String, String>) -> Vec<Run> {
    let mut runs: Vec<Run> = Vec::new();
    for (index, node) in path.iter().enumerate() {
        let segment = &segment_of[*node];
        match runs.last_mut() {
            Some(run) if &run.segment == segment => run.stages.push(node.to_string()),
            _ => runs.push(Run {
                segment: segment.clone(),
                start: index,
                stages: vec![node.to_string()],
            }),
        }
    }
    runs
}

fn require_wire(
    from: &str,
    to: &str,
    ty: &TypeInfo,
    reason: WireReason,
    errors: &mut Vec<PlacementError>,
) {
    let mut not_wire = |ty: String| {
        errors.push(PlacementError::NotWire {
            from: from.to_string(),
            to: to.to_string(),
            ty,
            reason,
        });
    };
    // A streamed request crosses whole: its head and its items each need a
    // codec, and each part without one is reported.
    if !ty.wire {
        not_wire(ty.name.clone());
    }
    if let Some(item) = ty.stream.as_ref().filter(|item| !item.wire) {
        not_wire(format!("stream {}", item.name));
    }
}

/// Structural checks on a manifest. `graph!` emits consistent manifests; a
/// hand-written or corrupted one is caught here instead of panicking later.
pub(crate) fn check_manifest(manifest: &Manifest) -> Vec<PlacementError> {
    let mut errors = Vec::new();
    let mut bad = |why: String| errors.push(PlacementError::InvalidManifest(why));

    let mut seen = BTreeSet::new();
    for node in &manifest.nodes {
        if !seen.insert(node.name.as_str()) {
            bad(format!("node `{}` is declared twice", node.name));
        }
    }
    if manifest.hops.is_empty() {
        bad("the graph has no hops".to_string());
        return errors;
    }
    let role = |name: &str| manifest.node(name).map(|n| n.role);
    for (index, hop) in manifest.hops.iter().enumerate() {
        if !matches!(role(&hop.outer), Some(Role::Source | Role::Operator)) {
            bad(format!(
                "hop {index}: its outer end must be a Source or Operator"
            ));
        }
        if !matches!(role(&hop.inner), Some(Role::Sink | Role::Operator)) {
            bad(format!(
                "hop {index}: its inner end must be a Sink or Operator"
            ));
        }
        if role(&hop.outer) == Some(Role::Operator)
            && (index == 0 || manifest.hops[index - 1].inner != hop.outer)
        {
            bad(format!("hop {index}: operator must follow its owning hop"));
        }
        if role(&hop.inner) == Some(Role::Operator)
            && manifest
                .hops
                .get(index + 1)
                .is_none_or(|next| next.outer != hop.inner)
        {
            bad(format!("hop {index}: operator must own the next hop"));
        }
        for node in &hop.forward {
            if role(node) != Some(Role::Forward) {
                bad(format!("hop {index}: `{node}` must be a Forward"));
            }
        }
        for node in &hop.backward {
            if role(node) != Some(Role::Backward) {
                bad(format!("hop {index}: `{node}` must be a Backward"));
            }
        }
        if hop.forward_types.len() != hop.forward.len() + 1 {
            bad(format!(
                "hop {index}: needs one forward type per forward edge"
            ));
        }
        if hop.backward_types.len() != hop.backward.len() + 1 {
            bad(format!(
                "hop {index}: needs one backward type per backward edge"
            ));
        }
        for (nodes, types) in [
            (&hop.forward, &hop.forward_types),
            (&hop.backward, &hop.backward_types),
        ] {
            for (position, name) in nodes.iter().enumerate() {
                if manifest.node(name).is_some_and(|n| n.is_identity())
                    && types.get(position) != types.get(position + 1)
                {
                    bad(format!(
                        "identity node `{name}` must preserve its message type"
                    ));
                }
            }
        }
        if manifest.node(&hop.inner).is_some_and(|n| n.is_identity())
            && let Some(nested) = manifest.hops.get(index + 1)
            && (hop.forward_types.last() != nested.forward_types.first()
                || hop.backward_types.first() != nested.backward_types.last())
        {
            bad(format!(
                "identity operator `{}` must preserve its request and response types",
                hop.inner
            ));
        }
        if let Some(ty) = hop.backward_types.iter().find(|t| t.stream.is_some()) {
            bad(format!(
                "hop {index}: response type `{ty}` is streamed; only requests can be"
            ));
        }
    }
    // Each node fills exactly one place: a source is a root hop's outer
    // end, an operator is one hop's inner end and the next hop's outer end,
    // and a sink can terminate several roots. Other nodes appear once.
    // Counting catches an operator that
    // owns two hops, or nests under itself.
    let mut places: BTreeMap<&str, usize> = BTreeMap::new();
    for hop in &manifest.hops {
        for node in std::iter::once(&hop.outer)
            .chain(std::iter::once(&hop.inner))
            .chain(&hop.forward)
            .chain(&hop.backward)
        {
            *places.entry(node.as_str()).or_default() += 1;
        }
    }
    for node in &manifest.nodes {
        let expected = if node.role == Role::Operator { 2 } else { 1 };
        let found = places.get(node.name.as_str()).copied().unwrap_or(0);
        if found > expected && node.role != Role::Sink {
            bad(format!(
                "node `{}` fills {found} places in the topology; a {:?} fills {expected}",
                node.name, node.role
            ));
        }
    }
    for node in &manifest.nodes {
        let in_topology = manifest.hops.iter().any(|hop| {
            hop.outer == node.name
                || hop.inner == node.name
                || hop.forward.contains(&node.name)
                || hop.backward.contains(&node.name)
        });
        if !in_topology {
            bad(format!("node `{}` is not in the topology", node.name));
        }
    }
    errors
}

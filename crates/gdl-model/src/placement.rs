use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::Role;
use crate::hash::Fnv;
use crate::manifest::{HopInfo, Manifest, TypeInfo};
use crate::{Problem, Problems, read_toml};

mod boundary;
mod calls;
mod classes;
mod codecs;
mod errors;
mod layouts;
mod pools;
mod report;
mod spelled;

use boundary::bound;
use calls::Services;
use classes::classify;
pub use codecs::{CodecEntry, EdgeCodec, EdgeCompression};
pub use errors::{PlacementError, PlacementErrors, WireReason};
pub use layouts::{Placements, check_layout_name};
pub use pools::PoolPolicy;
pub use report::CrossingReport;
use report::hop_lines;
use spelled::Spelled;

/// Which nodes live in which segment.
///
/// A placement file contains `[segment.<name>]` tables, optional `[routing]`
/// policies, optional `[class.<name>]` tables that group the segments into
/// classes that deploy together, optional `[pools.<class>]` tables that
/// say how a chooser picks a pool of a class it sends to, and an optional
/// `[codec]` table that names an edge's codec. Other deployment tables are
/// ignored here.
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
        deserialize_with = "pools::recorded"
    )]
    pools: BTreeMap<String, PoolPolicy>,
    // Skipped when empty, so that a placement without codecs writes the
    // text, and the hub reads the JSON, that it did before codecs existed.
    #[serde(rename = "codec", default, skip_serializing_if = "BTreeMap::is_empty")]
    codecs: BTreeMap<String, CodecEntry>,
    #[serde(skip)]
    spelled: Spelled,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentSpec {
    nodes: Vec<String>,
}

// Unknown keys are refused: a pool policy written here by mistake, rather
// than under `[pools.<class>]`, would otherwise be dropped without a word.
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
pub(crate) fn check_name_shape(kind: &str, name: &str) -> Result<(), String> {
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

/// Whether `name` can name a pool policy in `[pools.<class>] policy`: a
/// name of the same shape as a selector's, so that a registered policy is
/// named as a selector is.
pub fn check_pool_policy_name(name: &str) -> Result<(), String> {
    check_name_shape("policy", name)
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

    /// Set how a chooser picks a pool of `class`. Defaults are not
    /// recorded, as a default router is not: a value equal to its default
    /// is the same placement as one that leaves it unsaid, and setting the
    /// default policy removes the class's.
    pub fn pool_policy(mut self, class: impl Into<String>, policy: PoolPolicy) -> Self {
        let class = class.into();
        if policy == PoolPolicy::default() {
            self.pools.remove(&class);
            self.spelled.pools.insert(class);
        } else {
            self.spelled.pools.remove(&class);
            self.pools.insert(class, policy);
        }
        self
    }

    /// Name the codec of the edge `<from> -> <to>`, for the messages on it
    /// that pin none. The layout check refuses an edge the graph does not
    /// have, or one that is not encoded under the placement.
    pub fn codec(mut self, edge: &str, entry: CodecEntry) -> Self {
        let key = codecs::edge_key(edge).unwrap_or_else(|_| edge.to_string());
        self.codecs.insert(key, entry);
        self
    }

    /// Parse segment placement, request routing, classes, and pool policies
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
        placement.spelled = Spelled::read(written, &placement.routing, &placement.pools);
        let mut problems = Vec::new();
        placement.codecs = codecs::keyed(std::mem::take(&mut placement.codecs), &mut problems);
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
        for (class, policy) in &placement.pools {
            if let Err(why) = pools::check_policy_name(policy) {
                problems.push(Problem::at(
                    Some(format!("pools.{class}")),
                    Some("policy".into()),
                    why,
                ));
            }
            if let Err(why) = pools::check_provides(policy) {
                problems.push(Problem::at(
                    Some(format!("pools.{class}")),
                    Some("provides".into()),
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
    /// request routing policies, classes, each class's effective pool
    /// policy, a default one included, and the property kinds each class's
    /// pools serve from their settings (`[pools.<class>] provides`), and
    /// each `[codec]` entry without its floor. Node order inside a segment,
    /// and segment order inside a class, do not matter.
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
        pools::hash(&mut h, self.classes.keys(), &self.pools);
        codecs::hash(&mut h, &self.codecs);
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
    services: BTreeMap<String, BTreeSet<String>>,
    class_of: BTreeMap<String, String>,
    boundaries: Vec<ClassBoundary>,
    pool_policies: BTreeMap<String, PoolPolicy>,
    hops: Vec<HopLayout>,
    routing: BTreeMap<(usize, usize), RequestRouter>,
    call_routing: BTreeMap<String, RequestRouter>,
    /// The edges a `[codec]` entry compresses, by hop, path, and the
    /// position each enters.
    compression: BTreeMap<(usize, Leg, usize), EdgeCompression>,
    /// Each segment's calls to a copy of a unary node in another segment.
    remote_calls: calls::Remote,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    /// What a `[codec]` entry compresses on the edge of hop `hop` that
    /// enters position `pos` of its `leg`: `None` for an edge without one.
    pub fn compression(&self, hop: usize, leg: Leg, pos: usize) -> Option<&EdgeCompression> {
        self.compression.get(&(hop, leg, pos))
    }

    /// The segments at either end of an edge that a `[codec]` entry
    /// compresses: each sends or receives a compressed value.
    pub fn compressing_segments(&self) -> BTreeSet<&str> {
        let mut segments = BTreeSet::new();
        for &(hop, leg, pos) in self.compression.keys() {
            let hop = &self.hops[hop];
            let runs = match leg {
                Leg::Forward => &hop.forward_runs,
                Leg::Backward => &hop.backward_runs,
            };
            let segment_at = |at: usize| {
                runs.iter()
                    .find(|run| (run.start..run.start + run.stages.len()).contains(&at))
                    .map(|run| run.segment.as_str())
            };
            segments.extend(segment_at(pos - 1));
            segments.extend(segment_at(pos));
        }
        segments
    }

    /// The segment a node of the topology is placed in. A unary node may be
    /// in several, so it has none here: see [`Layout::service_segments`].
    pub fn segment_of(&self, node: &str) -> Option<&str> {
        self.segment_of.get(node).map(String::as_str)
    }

    /// The segments that list unary node `node`, in name order: empty for
    /// any other node.
    pub fn service_segments(&self, node: &str) -> Vec<&str> {
        self.services
            .get(node)
            .map(|segments| segments.iter().map(String::as_str).collect())
            .unwrap_or_default()
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
    /// it holds a run of, the backward runs it opens as an outer end, and
    /// the segment of each service it calls in another segment
    /// ([`Layout::calls_from`]).
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
        targets.extend(self.calls_from(segment).map(|(_, to)| to.to_string()));
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

    /// The services that nodes of `segment` call in another segment, in name
    /// order, each with the one segment that hosts the copy they call. A
    /// call to the copy in `segment` itself is not among them.
    pub fn calls_from<'a>(
        &'a self,
        segment: &str,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.remote_calls
            .get(segment)
            .into_iter()
            .flatten()
            .map(|(service, to)| (service.as_str(), to.as_str()))
    }

    /// Whether a node in another segment calls the copy of unary node
    /// `service` in `segment`.
    pub fn called_in(&self, service: &str, segment: &str) -> bool {
        self.remote_calls
            .values()
            .any(|calls| calls.get(service).is_some_and(|to| to == segment))
    }

    /// How a caller picks an instance of unary node `service` in another
    /// segment: its `[routing]` entry, or round robin.
    pub fn call_router(&self, service: &str) -> &RequestRouter {
        static DEFAULT: RequestRouter = RequestRouter::RoundRobin;
        self.call_routing.get(service).unwrap_or(&DEFAULT)
    }

    /// The selectors `segment` calls through: each unary node its nodes
    /// call in another segment whose router names a selector, as (service,
    /// name), in name order. A call to the copy in `segment` itself is
    /// routed by nothing, so its selector is not here. The segment must
    /// register each name, or have gdl's own of that name.
    pub fn call_selectors<'a>(
        &'a self,
        segment: &str,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.calls_from(segment)
            .filter_map(|(service, _)| match self.call_router(service) {
                RequestRouter::Selector(name) => Some((service, name.as_str())),
                _ => None,
            })
    }

    /// Every call from one segment to a copy in another, as (the caller's
    /// segment, the service, the service's segment).
    pub(crate) fn remote_calls(&self) -> impl Iterator<Item = (&str, &str, &str)> {
        self.remote_calls.iter().flat_map(|(from, calls)| {
            calls
                .iter()
                .map(move |(service, to)| (from.as_str(), service.as_str(), to.as_str()))
        })
    }

    /// How a chooser picks a pool of `class`, defaults applied: `None`
    /// unless a class boundary enters `class`.
    pub fn pool_policy(&self, class: &str) -> Option<&PoolPolicy> {
        self.pool_policies.get(class)
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

    /// The selectors `segment` sends through: each crossing whose first
    /// stage is in another segment, whose sending run is in `segment`, and
    /// whose router names a selector, as (hop, position, name). The edge
    /// the crossing carries is the hop's forward edge `position - 1`. The
    /// segment must register each name, or have gdl's own of that name.
    pub fn sent_selectors<'a>(
        &'a self,
        segment: &'a str,
    ) -> impl Iterator<Item = (usize, usize, &'a str)> + 'a {
        self.hops.iter().enumerate().flat_map(move |(hop, plan)| {
            plan.forward_runs.windows(2).filter_map(move |pair| {
                if pair[0].segment != segment {
                    return None;
                }
                match self.request_router(hop, pair[1].start) {
                    RequestRouter::Selector(name) => Some((hop, pair[1].start, name.as_str())),
                    _ => None,
                }
            })
        })
    }

    /// The classes `segment` chooses pools of: those a hop crosses into
    /// whose outer end `segment` holds. Each needs its pool policy where
    /// `segment` runs. Empty for a segment that is not a chooser, and for a
    /// placement without classes.
    pub fn chosen_classes(&self, segment: &str) -> BTreeSet<&str> {
        self.boundaries
            .iter()
            .filter(|boundary| {
                self.hops[boundary.hop]
                    .forward_runs
                    .first()
                    .is_some_and(|outer| outer.segment == segment)
            })
            .map(|boundary| boundary.into.as_str())
            .collect()
    }

    /// Crossings and anchors for one request through each root and, when
    /// the placement has classes, the WAN round trips each class boundary
    /// adds when the request spills to a pool in another region.
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
    pools::judge(placement, &mut class_errors);
    let mut errors = check_manifest(manifest);
    if !errors.is_empty() {
        errors.append(&mut class_errors);
        return Err(PlacementErrors(errors));
    }
    // A node in no segment, or in several, is judged nowhere, but the
    // boundary rule still judges every hop of the nodes that are placed.
    let (segment_of, services) = assign(manifest, placement, &mut errors);
    calls::judge(manifest, &segment_of, &services, &class_of, &mut errors);
    let boundaries = if placement.classes.is_empty() {
        Vec::new()
    } else {
        bound(manifest, &segment_of, &class_of, &mut class_errors)
    };
    let undecided = pools::Undecided::new(manifest, &segment_of, &class_of);
    let pool_policies = pools::entered(placement, &boundaries, &undecided, &mut class_errors);
    if !errors.is_empty() {
        errors.append(&mut class_errors);
        return Err(PlacementErrors(errors));
    }
    match place(manifest, placement, segment_of, services) {
        Ok(layout) if class_errors.is_empty() => Ok(Layout {
            class_of,
            boundaries,
            pool_policies,
            ..layout
        }),
        Ok(_) => Err(PlacementErrors(class_errors)),
        Err(PlacementErrors(mut errors)) => {
            errors.append(&mut class_errors);
            Err(PlacementErrors(errors))
        }
    }
}

/// The segment of each node of the topology that is in exactly one, and
/// the segments of each unary node, checking that every node of a sound
/// manifest is in one, and that only a unary node is in several.
fn assign(
    manifest: &Manifest,
    placement: &Placement,
    errors: &mut Vec<PlacementError>,
) -> (BTreeMap<String, String>, Services) {
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
    let (mut segment_of, mut services) = (BTreeMap::new(), Services::new());
    for node in &manifest.nodes {
        match where_placed.get(node.name.as_str()).map(Vec::as_slice) {
            None | Some([]) => errors.push(PlacementError::Unplaced {
                node: node.name.clone(),
            }),
            Some(segments) if node.role == Role::Unary => {
                let segments = segments.iter().map(ToString::to_string).collect();
                services.insert(node.name.clone(), segments);
            }
            Some([segment]) => {
                segment_of.insert(node.name.clone(), segment.to_string());
            }
            Some(segments) => errors.push(PlacementError::PlacedTwice {
                node: node.name.clone(),
                segments: segments.iter().map(ToString::to_string).collect(),
            }),
        }
    }
    (segment_of, services)
}

/// Every check that needs the nodes' segments but not their classes, and
/// the layout without classes.
fn place(
    manifest: &Manifest,
    placement: &Placement,
    segment_of: BTreeMap<String, String>,
    services: Services,
) -> Result<Layout, PlacementErrors> {
    let mut errors = Vec::new();
    let hops: Vec<HopLayout> = manifest
        .hops
        .iter()
        .enumerate()
        .map(|(index, hop)| hop_layout(index, hop, &segment_of, &mut errors))
        .collect();
    if !errors.is_empty() {
        return Err(PlacementErrors(errors));
    }
    let (mut routing, mut call_routing) = (BTreeMap::new(), BTreeMap::new());
    let unary = |node: &str| manifest.node(node).is_some_and(|n| n.role == Role::Unary);
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
        if unary(node) {
            call_routing.insert(node.clone(), router.clone());
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
        let found = unary(node)
            || hops.iter().any(|hop| {
                hop.forward_runs
                    .iter()
                    .skip(1)
                    .any(|run| run.stages.first() == Some(node))
            });
        if !found {
            errors.push(PlacementError::InvalidRouter(node.clone()));
        }
    }
    let compression = codecs::resolve(&placement.codecs, &manifest.hops, &hops, &mut errors);
    if !errors.is_empty() {
        return Err(PlacementErrors(errors));
    }
    Ok(Layout {
        compression,
        roots: manifest
            .nodes
            .iter()
            .filter(|n| n.role == Role::Source)
            .count(),
        routing,
        call_routing,
        remote_calls: calls::remote(manifest, &segment_of, &services),
        contract_hash: manifest.contract_hash(),
        placement_hash: placement.hash(),
        segment_of,
        services,
        class_of: BTreeMap::new(),
        boundaries: Vec::new(),
        pool_policies: BTreeMap::new(),
        hops,
    })
}

fn hop_layout(
    index: usize,
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
    around_frames(index, hop, &layout, errors);
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

/// The task that runs a stage of a hop, for an around node, whose two
/// halves must run in one task: its value is kept in that task's future and
/// never crosses. See `docs/src/concepts/placement.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Frame {
    /// The outer end's task: the forward walk up to the first hand-off, and
    /// the backward run the response returns to.
    Outer,
    /// The task a hand-off starts at the inner end: the last forward run,
    /// the inner end, and the first backward run.
    Inner,
    /// A task that runs only one direction: a middle forward run, a backward
    /// run the outer end opens, or the inner end's task when the forward
    /// path never left the outer end's segment.
    Other,
}

impl Frame {
    fn name(self) -> &'static str {
        match self {
            Self::Outer => "the outer end's task",
            Self::Inner => "the inner end's task",
            Self::Other => "a task of its own",
        }
    }
}

fn forward_frame(layout: &HopLayout, pos: usize) -> Frame {
    let runs = &layout.forward_runs;
    if pos < runs[0].start + runs[0].stages.len() {
        Frame::Outer
    } else if runs.len() > 1 && pos >= runs[runs.len() - 1].start {
        Frame::Inner
    } else {
        Frame::Other
    }
}

fn backward_frame(layout: &HopLayout, pos: usize) -> Frame {
    let runs = &layout.backward_runs;
    // With one backward run, the outer end applies every backward node.
    if runs.len() == 1 || pos >= runs[runs.len() - 1].start {
        Frame::Outer
    } else if pos < runs[1].start && layout.forward_runs.len() > 1 {
        Frame::Inner
    } else {
        Frame::Other
    }
}

/// Refuses each around node whose two halves run in two tasks.
fn around_frames(
    index: usize,
    hop: &HopInfo,
    layout: &HopLayout,
    errors: &mut Vec<PlacementError>,
) {
    if layout.local {
        return;
    }
    for (at, node) in hop.forward.iter().enumerate() {
        let Some(back) = hop.backward.iter().position(|b| b == node) else {
            continue;
        };
        let (ahead, behind) = (
            forward_frame(layout, at + 1),
            backward_frame(layout, back + 1),
        );
        if ahead != behind || ahead == Frame::Other {
            errors.push(PlacementError::AroundSplit {
                node: node.clone(),
                hop: index,
                forward: ahead.name(),
                backward: behind.name(),
            });
        }
    }
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
    let mut errors = calls::check_graph(manifest);
    // A unary node on a hop is refused once, by name; the rules below for
    // the role at each place pass over it.
    let unary = |name: &str| manifest.node(name).is_some_and(|n| n.role == Role::Unary);
    let mut on_hops = BTreeSet::new();
    for hop in &manifest.hops {
        for node in hop
            .forward_path()
            .chain(hop.backward.iter().map(String::as_str))
        {
            if unary(node) && on_hops.insert(node) {
                errors.push(PlacementError::UnaryInTopology {
                    node: node.to_string(),
                });
            }
        }
    }
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
        if !matches!(
            role(&hop.outer),
            Some(Role::Source | Role::Operator | Role::Unary)
        ) {
            bad(format!(
                "hop {index}: its outer end must be a Source or Operator"
            ));
        }
        if !matches!(
            role(&hop.inner),
            Some(Role::Sink | Role::Operator | Role::Unary)
        ) {
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
            if !matches!(role(node), Some(Role::Forward | Role::Around | Role::Unary)) {
                bad(format!("hop {index}: `{node}` must be a Forward"));
            }
        }
        for node in &hop.backward {
            if !matches!(
                role(node),
                Some(Role::Backward | Role::Around | Role::Unary)
            ) {
                bad(format!("hop {index}: `{node}` must be a Backward"));
            }
        }
        // An around node's halves pair as brackets do: the backward chain
        // meets them in the reverse of the forward chain's order.
        let arounds = |nodes: &[String]| -> Vec<String> {
            nodes
                .iter()
                .filter(|n| role(n) == Some(Role::Around))
                .cloned()
                .collect()
        };
        let (ahead, mut behind) = (arounds(&hop.forward), arounds(&hop.backward));
        behind.reverse();
        if ahead != behind {
            let mut unpaired: Vec<&String> = ahead
                .iter()
                .filter(|n| !behind.contains(n))
                .chain(behind.iter().filter(|n| !ahead.contains(n)))
                .collect();
            unpaired.dedup();
            match unpaired.first() {
                Some(node) => bad(format!(
                    "hop {index}: around node `{node}` must be in both its forward and its backward chain"
                )),
                None => bad(format!(
                    "hop {index}: around nodes {ahead:?} must nest, their backward halves in the reverse of their forward order"
                )),
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
    for node in manifest.nodes.iter().filter(|n| n.role != Role::Unary) {
        let expected = if matches!(node.role, Role::Operator | Role::Around) {
            2
        } else {
            1
        };
        let found = places.get(node.name.as_str()).copied().unwrap_or(0);
        if found > expected && node.role != Role::Sink {
            bad(format!(
                "node `{}` fills {found} places in the topology; a {:?} fills {expected}",
                node.name, node.role
            ));
        }
    }
    for node in manifest.nodes.iter().filter(|n| n.role != Role::Unary) {
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

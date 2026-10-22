use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::Role;
use crate::hash::Fnv;
use crate::manifest::{HopInfo, Manifest, TypeInfo};

/// Which nodes live in which segment.
///
/// A placement file contains `[segment.<name>]` tables, optional `[routing]`
/// policies, and optional `[class.<name>]` tables that group the segments
/// into classes that deploy together. Other deployment tables are ignored
/// here.
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct SegmentSpec {
    nodes: Vec<String>,
}

// Unknown keys are refused: a cell policy written here by mistake would
// otherwise be dropped without a word.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassSpec {
    segments: Vec<String>,
}

/// Selection of a replica when a request crosses into another segment.
///
/// In a placement file a policy is a string (`"round_robin"`, `"random"`)
/// and a selector a table (`{ selector = "affinity" }`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestRouter {
    /// Cycle through replicas in roster order.
    #[default]
    RoundRobin,
    /// Select a replica independently for each request hand-off.
    Random,
    /// The selector of this name, which the sending segment registers or
    /// gdl provides ([`SELECTORS`]).
    Selector(String),
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
fn check_class_name(name: &str) -> Result<(), String> {
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
        } else {
            self.routing.insert(node, router);
        }
        self
    }

    /// Parse segment placement, request routing, and classes from a
    /// deployment file.
    pub fn from_toml(text: &str) -> Result<Self, Error> {
        let mut placement: Self = toml::from_str(text).map_err(|e| Error::new(e.to_string()))?;
        placement
            .routing
            .retain(|_, router| *router != RequestRouter::default());
        for (node, router) in &placement.routing {
            if let RequestRouter::Selector(name) = router {
                check_selector_name(name)
                    .map_err(|why| Error::new(format!("[routing] {node}: {why}")))?;
            }
        }
        for name in placement.classes.keys() {
            check_class_name(name).map_err(Error::new)?;
        }
        Ok(placement)
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
    /// request routing policies, and classes. Node order inside a segment,
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

impl HopLayout {
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

    /// The hops that cross from one class into another, in hop order:
    /// empty when the placement has no classes.
    pub fn boundaries(&self) -> &[ClassBoundary] {
        &self.boundaries
    }

    /// Crossings and anchors for one request through each root and, when
    /// the placement has classes, the WAN round trips each class boundary
    /// adds when the request spills to a cell in another region.
    pub fn report(&self) -> CrossingReport {
        let mut hops: Vec<HopCount> = self
            .hops
            .iter()
            .map(|hop| HopCount {
                crossings: hop.crossings(),
                anchors: hop.anchors(),
                wan_round_trips: 0,
                boundary: None,
            })
            .collect();
        for boundary in &self.boundaries {
            let count = &mut hops[boundary.hop];
            count.wan_round_trips = spill_round_trips(&self.hops[boundary.hop]);
            count.boundary = Some((boundary.from.clone(), boundary.into.clone()));
        }
        CrossingReport {
            roots: self.roots,
            hops,
            classed: !self.class_of.is_empty(),
        }
    }
}

/// The WAN round trips one request adds at a class boundary when the cell
/// it is offered to is in another region, between warm peers. The
/// `gdl.fwd` goes out and the inner end's output opens the outer end's
/// ticket back, with no attach round trip. A streamed request's items wait
/// for the acknowledgement that brings back the inner end's input anchor,
/// so it takes a second. The boundary rule makes the edge into the inner
/// end the hop's only crossing out of the outer end's segment. `_hello` and
/// a cold connection's handshake are paid once per pair of instances, not
/// per request, and are not counted.
fn spill_round_trips(hop: &HopLayout) -> usize {
    if hop.request_streams.last() == Some(&true) {
        2
    } else {
        1
    }
}

/// Aggregate segment crossings and anchors for one request through each
/// root and, when the placement has classes, the WAN round trips its class
/// boundaries add to a request that spills to another region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossingReport {
    roots: usize,
    hops: Vec<HopCount>,
    // Without classes the report reads as it did before classes existed,
    // so that the generated `crossings.txt` of every such spec is unchanged.
    classed: bool,
}

impl CrossingReport {
    /// Crossings for one request through each root, over all hops.
    pub fn crossings(&self) -> usize {
        self.hops.iter().map(|hop| hop.crossings).sum()
    }

    /// Anchors for one request through each root, over all hops.
    pub fn anchors(&self) -> usize {
        self.hops.iter().map(|hop| hop.anchors).sum()
    }

    /// WAN round trips for one request through each root, over all hops,
    /// when every class boundary it crosses spills to another region. In
    /// one region a boundary adds none. Zero without classes.
    pub fn wan_round_trips(&self) -> usize {
        self.hops.iter().map(|hop| hop.wan_round_trips).sum()
    }
}

impl fmt::Display for CrossingReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, hop) in self.hops.iter().enumerate() {
            write!(
                f,
                "hop {index}: {} crossings, {} anchors",
                hop.crossings, hop.anchors
            )?;
            if self.classed {
                write!(f, ", {} WAN round trips", hop.wan_round_trips)?;
                if let Some((from, into)) = &hop.boundary {
                    write!(f, " ({from} -> {into})")?;
                }
            }
            writeln!(f)?;
        }
        write!(
            f,
            "{}: {} crossings, {} anchors",
            if self.roots == 1 {
                "per request"
            } else {
                "one request per root"
            },
            self.crossings(),
            self.anchors()
        )?;
        if self.classed {
            write!(f, ", {} WAN round trips", self.wan_round_trips())?;
        }
        Ok(())
    }
}

/// One hop's line of a [`CrossingReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct HopCount {
    crossings: usize,
    anchors: usize,
    wan_round_trips: usize,
    /// The classes a boundary hop crosses from and into.
    boundary: Option<(String, String)>,
}

/// Why a placement was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlacementError {
    /// A router targets a node that does not start a remote request run.
    InvalidRouter(String),
    /// A router names a selector by a name no selector can have.
    InvalidSelector {
        /// The node whose boundary it routes.
        node: String,
        /// Why the name is refused.
        reason: String,
    },
    /// The manifest itself is inconsistent.
    InvalidManifest(String),
    /// A segment lists a node the graph does not have.
    UnknownNode {
        /// The segment.
        segment: String,
        /// The unknown name.
        node: String,
    },
    /// A node is in no segment.
    Unplaced {
        /// The node.
        node: String,
    },
    /// A node is in more than one segment.
    PlacedTwice {
        /// The node.
        node: String,
        /// Every segment that lists it.
        segments: Vec<String>,
    },
    /// A segment lists no nodes.
    EmptySegment {
        /// The segment.
        segment: String,
    },
    /// A class's name is not one a class can have.
    InvalidClassName {
        /// The class.
        class: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A class lists a segment the placement does not have.
    UnknownSegment {
        /// The class.
        class: String,
        /// The unknown name.
        segment: String,
    },
    /// A class lists no segments.
    EmptyClass {
        /// The class.
        class: String,
    },
    /// A class lists one segment more than once.
    SegmentRepeated {
        /// The class.
        class: String,
        /// The segment.
        segment: String,
    },
    /// The placement has classes, and a segment is in none of them.
    Unclassed {
        /// The segment.
        segment: String,
    },
    /// A segment is in more than one class.
    ClassedTwice {
        /// The segment.
        segment: String,
        /// Every class that lists it.
        classes: Vec<String>,
    },
    /// A hop reaches another class at a stage before or after its inner
    /// end, and its inner end is in its outer end's class.
    InnerEndInOuterClass {
        /// The hop's outer end.
        outer: String,
        /// The hop's inner end.
        inner: String,
        /// The class of both ends.
        from: String,
        /// The other class the hop reaches.
        into: String,
        /// The first stage in that class.
        stage: String,
    },
    /// A hop that crosses into another class has a forward or backward
    /// stage outside its outer end's segment.
    StageOffOuterSegment {
        /// The hop's outer end.
        outer: String,
        /// The hop's inner end.
        inner: String,
        /// The outer end's class.
        from: String,
        /// The class the hop crosses into.
        into: String,
        /// The stage.
        stage: String,
        /// Whether the stage is forward or backward.
        role: Role,
        /// The stage's segment.
        segment: String,
        /// The outer end's segment.
        outer_segment: String,
    },
    /// A hop nested under a class boundary's inner end has a stage outside
    /// the inner end's class.
    NestedOutsideClass {
        /// The boundary hop's outer end.
        outer: String,
        /// The boundary hop's inner end, which the stage's hop is under.
        inner: String,
        /// The boundary hop's outer end's class.
        from: String,
        /// The inner end's class.
        into: String,
        /// The stage.
        stage: String,
        /// The stage's segment.
        segment: String,
        /// The stage's class.
        class: String,
    },
    /// The class boundaries make a cycle: work could cross from a class
    /// into one that crosses back into it.
    ClassCycle {
        /// The classes on the cycle, in name order.
        classes: Vec<String>,
    },
    /// An edge that must be encoded carries a type with no codec.
    NotWire {
        /// The edge's upstream node.
        from: String,
        /// The edge's downstream node.
        to: String,
        /// The type on the edge.
        ty: String,
        /// Why the edge must be encoded.
        reason: WireReason,
    },
}

/// Why an edge must be encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireReason {
    /// Its two ends are in different segments.
    Crossing,
    /// Its ends share a segment, but its items reach the outer end through an
    /// anchor because the hop's forward path leaves that segment and comes
    /// back.
    AnchorCarried,
}

impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRouter(node) => {
                write!(f, "request router `{node}` must target a request boundary")
            }
            Self::InvalidSelector { node, reason } => {
                write!(f, "request router `{node}`: {reason}")
            }
            Self::InvalidManifest(why) => write!(f, "invalid manifest: {why}"),
            Self::UnknownNode { segment, node } => {
                write!(f, "segment `{segment}` lists unknown node `{node}`")
            }
            Self::Unplaced { node } => write!(f, "node `{node}` is in no segment"),
            Self::PlacedTwice { node, segments } => {
                write!(f, "node `{node}` is in several segments: {segments:?}")
            }
            Self::EmptySegment { segment } => write!(f, "segment `{segment}` has no nodes"),
            Self::InvalidClassName { class, reason } => write!(f, "class `{class}`: {reason}"),
            Self::UnknownSegment { class, segment } => {
                write!(f, "class `{class}` lists unknown segment `{segment}`")
            }
            Self::EmptyClass { class } => write!(f, "class `{class}` has no segments"),
            Self::SegmentRepeated { class, segment } => {
                write!(f, "class `{class}` lists segment `{segment}` twice")
            }
            Self::Unclassed { segment } => write!(f, "segment `{segment}` is in no class"),
            Self::ClassedTwice { segment, classes } => {
                write!(f, "segment `{segment}` is in several classes: {classes:?}")
            }
            Self::InnerEndInOuterClass {
                outer,
                inner,
                from,
                into,
                stage,
            } => write!(
                f,
                "hop `{outer}` -> `{inner}` reaches class `{into}` at `{stage}`, but its inner end `{inner}` is in `{outer}`'s class `{from}`; a hop may cross into another class only at its inner end"
            ),
            Self::StageOffOuterSegment {
                outer,
                inner,
                from,
                into,
                stage,
                role,
                segment,
                outer_segment,
            } => write!(
                f,
                "hop `{outer}` -> `{inner}` crosses from class `{from}` into `{into}`, but its {} stage `{stage}` is in segment `{segment}`; every stage but the inner end must be in `{outer}`'s segment `{outer_segment}`",
                role.name()
            ),
            Self::NestedOutsideClass {
                outer,
                inner,
                from,
                into,
                stage,
                segment,
                class,
            } => write!(
                f,
                "hop `{outer}` -> `{inner}` crosses from class `{from}` into `{into}`, but `{stage}`, under `{inner}`, is in segment `{segment}` of class `{class}`; every hop under the inner end must stay in `{into}`"
            ),
            Self::ClassCycle { classes } => {
                let classes: Vec<String> = classes.iter().map(|c| format!("`{c}`")).collect();
                write!(
                    f,
                    "the class boundaries make a cycle through {}; work may cross between two classes in one direction only",
                    classes.join(", ")
                )
            }
            Self::NotWire {
                from,
                to,
                ty,
                reason,
            } => {
                let why = match reason {
                    WireReason::Crossing => "crosses a segment boundary",
                    WireReason::AnchorCarried => "is carried by a reply anchor",
                };
                write!(
                    f,
                    "edge `{from}` -> `{to}` {why}, but its type `{ty}` is not serializable"
                )
            }
        }
    }
}

/// Every reason a placement was rejected; validation reports all of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementErrors(pub Vec<PlacementError>);

impl fmt::Display for PlacementErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PlacementErrors {}

impl From<PlacementErrors> for Error {
    fn from(errors: PlacementErrors) -> Self {
        Error::new(errors.to_string())
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
    if !errors.is_empty() {
        errors.append(&mut class_errors);
        return Err(PlacementErrors(errors));
    }
    match place(manifest, placement, segment_of) {
        Ok(layout) if class_errors.is_empty() => Ok(Layout {
            class_of,
            boundaries,
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
        hops,
    })
}

/// Each segment's class, checking that a placement with classes puts every
/// segment in exactly one. A placement without classes has none to check.
fn classify(placement: &Placement, errors: &mut Vec<PlacementError>) -> BTreeMap<String, String> {
    let mut class_of = BTreeMap::new();
    if placement.classes.is_empty() {
        return class_of;
    }
    let mut where_classed: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (class, segments) in placement.classes() {
        // A misnamed class still classifies its segments, so that each of
        // them is not also reported as in no class.
        if let Err(reason) = check_class_name(class) {
            errors.push(PlacementError::InvalidClassName {
                class: class.to_string(),
                reason,
            });
        }
        if segments.is_empty() {
            errors.push(PlacementError::EmptyClass {
                class: class.to_string(),
            });
        }
        let mut listed = BTreeSet::new();
        for segment in segments {
            if !listed.insert(segment.as_str()) {
                errors.push(PlacementError::SegmentRepeated {
                    class: class.to_string(),
                    segment: segment.clone(),
                });
            } else if !placement.segments.contains_key(segment) {
                errors.push(PlacementError::UnknownSegment {
                    class: class.to_string(),
                    segment: segment.clone(),
                });
            } else {
                where_classed.entry(segment).or_default().push(class);
            }
        }
    }
    for (segment, _) in placement.segments() {
        match where_classed.get(segment).map(Vec::as_slice) {
            None | Some([]) => errors.push(PlacementError::Unclassed {
                segment: segment.to_string(),
            }),
            Some([class]) => {
                class_of.insert(segment.to_string(), class.to_string());
            }
            Some(classes) => errors.push(PlacementError::ClassedTwice {
                segment: segment.to_string(),
                classes: classes.iter().map(ToString::to_string).collect(),
            }),
        }
    }
    class_of
}

/// The hops that cross from one class into another, checking each against
/// the boundary rule, and the classes they cross between for a cycle.
///
/// The rule keeps every stage of a boundary hop but its inner end on the
/// outer end's instance: that instance chooses the cell, sends the one
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
fn bound(
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

    require_wire_at_crossings(&forward, &hop.forward_types, segment_of, errors);
    require_wire_at_crossings(&backward, &hop.backward_types, segment_of, errors);
    if !local && backward_runs.len() == 1 {
        // The response never crosses, but the inner end runs in a task the
        // forward path reached from another segment, so its output still
        // returns to the outer end through an anchor.
        require_wire(
            backward[0],
            backward[1],
            &hop.backward_types[0],
            WireReason::AnchorCarried,
            errors,
        );
    }

    let provisioning = if local || backward_runs.len() < 3 {
        Vec::new()
    } else {
        (1..backward_runs.len() - 1).rev().collect()
    };
    HopLayout {
        local,
        forward_runs,
        backward_runs,
        provisioning,
        request_streams: hop
            .forward_types
            .iter()
            .map(|t| t.stream.is_some())
            .collect(),
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

fn require_wire_at_crossings(
    path: &[&str],
    types: &[TypeInfo],
    segment_of: &BTreeMap<String, String>,
    errors: &mut Vec<PlacementError>,
) {
    for (edge, ty) in path.windows(2).zip(types) {
        if segment_of[edge[0]] != segment_of[edge[1]] {
            require_wire(edge[0], edge[1], ty, WireReason::Crossing, errors);
        }
    }
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

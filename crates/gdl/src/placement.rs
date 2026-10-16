use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::decl::Role;
use crate::error::{Error, ErrorKind};
use crate::hash::Fnv;
use crate::manifest::{HopInfo, Manifest, TypeInfo};

/// Which nodes live in which segment.
///
/// In a placement file this is the `[segment.<name>]` tables; other tables in
/// the same file (bindings, the contract path) are ignored here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    #[serde(rename = "segment", default)]
    segments: BTreeMap<String, SegmentSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct SegmentSpec {
    nodes: Vec<String>,
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

    /// Parse the `[segment.*]` tables of a placement file.
    pub fn from_toml(text: &str) -> Result<Self, Error> {
        toml::from_str(text).map_err(|e| Error::new(ErrorKind::Placement, e.to_string()))
    }

    /// Segment names and their nodes, in name order.
    pub fn segments(&self) -> impl Iterator<Item = (&str, &[String])> {
        self.segments
            .iter()
            .map(|(name, spec)| (name.as_str(), spec.nodes.as_slice()))
    }

    /// The placement hash: changes when any node moves, or a segment is
    /// renamed. Node order inside a segment does not matter.
    pub fn hash(&self) -> u64 {
        let mut h = Fnv::new();
        for (name, spec) in &self.segments {
            h.str(name);
            let nodes: BTreeSet<&str> = spec.nodes.iter().map(String::as_str).collect();
            h.strs(&nodes.into_iter().collect::<Vec<_>>());
        }
        h.finish()
    }
}

/// A placement checked against a manifest, with every hop split into runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    contract_hash: u64,
    placement_hash: u64,
    segment_of: BTreeMap<String, String>,
    hops: Vec<HopLayout>,
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
    pub fn anchors(&self) -> usize {
        if self.local {
            0
        } else {
            (self.backward_runs.len() - 1).max(1)
        }
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

    /// The hops, outermost first.
    pub fn hops(&self) -> &[HopLayout] {
        &self.hops
    }

    /// Crossings and anchors per request.
    pub fn report(&self) -> CrossingReport {
        CrossingReport {
            hops: self
                .hops
                .iter()
                .map(|hop| (hop.crossings(), hop.anchors()))
                .collect(),
        }
    }
}

/// What one request costs in segment crossings and anchors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossingReport {
    hops: Vec<(usize, usize)>,
}

impl CrossingReport {
    /// Crossings per request, over all hops.
    pub fn crossings(&self) -> usize {
        self.hops.iter().map(|(c, _)| c).sum()
    }

    /// Anchors per request, over all hops.
    pub fn anchors(&self) -> usize {
        self.hops.iter().map(|(_, a)| a).sum()
    }
}

impl fmt::Display for CrossingReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, (crossings, anchors)) in self.hops.iter().enumerate() {
            writeln!(f, "hop {index}: {crossings} crossings, {anchors} anchors")?;
        }
        write!(
            f,
            "per request: {} crossings, {} anchors",
            self.crossings(),
            self.anchors()
        )
    }
}

/// Why a placement was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlacementError {
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
            Self::InvalidManifest(why) => write!(f, "invalid manifest: {why}"),
            Self::UnknownNode { segment, node } => {
                write!(f, "segment `{segment}` lists unknown node `{node}`")
            }
            Self::Unplaced { node } => write!(f, "node `{node}` is in no segment"),
            Self::PlacedTwice { node, segments } => {
                write!(f, "node `{node}` is in several segments: {segments:?}")
            }
            Self::EmptySegment { segment } => write!(f, "segment `{segment}` has no nodes"),
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
        Error::new(ErrorKind::Placement, errors.to_string())
    }
}

/// Check `placement` against `manifest` and split every hop into runs.
pub fn layout(manifest: &Manifest, placement: &Placement) -> Result<Layout, PlacementErrors> {
    let mut errors = check_manifest(manifest);
    if !errors.is_empty() {
        return Err(PlacementErrors(errors));
    }

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
    if !errors.is_empty() {
        return Err(PlacementErrors(errors));
    }

    let hops: Vec<HopLayout> = manifest
        .hops
        .iter()
        .map(|hop| hop_layout(hop, &segment_of, &mut errors))
        .collect();
    if !errors.is_empty() {
        return Err(PlacementErrors(errors));
    }
    Ok(Layout {
        contract_hash: manifest.contract_hash(),
        placement_hash: placement.hash(),
        segment_of,
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
    if !ty.wire {
        errors.push(PlacementError::NotWire {
            from: from.to_string(),
            to: to.to_string(),
            ty: ty.name.clone(),
            reason,
        });
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
    let last = manifest.hops.len() - 1;
    for (index, hop) in manifest.hops.iter().enumerate() {
        let expected_outer = if index == 0 {
            Role::Source
        } else {
            Role::Operator
        };
        let expected_inner = if index == last {
            Role::Sink
        } else {
            Role::Operator
        };
        if role(&hop.outer) != Some(expected_outer) {
            bad(format!(
                "hop {index}: `{}` must be a {expected_outer:?}",
                hop.outer
            ));
        }
        if role(&hop.inner) != Some(expected_inner) {
            bad(format!(
                "hop {index}: `{}` must be a {expected_inner:?}",
                hop.inner
            ));
        }
        if index > 0 && manifest.hops[index - 1].inner != hop.outer {
            bad(format!(
                "hop {index}: its outer end `{}` is not the previous hop's inner end",
                hop.outer
            ));
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
    }
    // Each node fills exactly one place: the source is the first hop's outer
    // end, an operator is one hop's inner end and the next hop's outer end,
    // and every other node appears once. Counting catches an operator that
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
        if found > expected {
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

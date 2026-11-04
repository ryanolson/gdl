use std::fmt;

use crate::Role;
use crate::{Error, Problem, Problems};

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
    /// A pool policy's name is not one a policy can have.
    InvalidPoolPolicy {
        /// The class the policy is for.
        class: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A `[pools.<class>]` table's `provides` names a kind that cannot be
    /// a property kind, or names one twice.
    InvalidPoolProvides {
        /// The class the table is for.
        class: String,
        /// Why it is refused.
        reason: String,
    },
    /// A placement without classes has a pool policy.
    PoolPolicyWithoutClasses {
        /// The class the policy names.
        class: String,
    },
    /// A pool policy names a class the placement does not have.
    PoolPolicyUnknownClass {
        /// The class the policy names.
        class: String,
    },
    /// A pool policy is for a class that no class boundary enters, so no
    /// chooser picks a pool of it.
    PoolPolicyNotEntered {
        /// The class the policy names.
        class: String,
    },
    /// An around node's two halves run in two tasks, so the value its
    /// forward half keeps cannot reach its backward half.
    AroundSplit {
        /// The around node.
        node: String,
        /// The index of its hop.
        hop: usize,
        /// Where its forward half runs.
        forward: &'static str,
        /// Where its backward half runs.
        backward: &'static str,
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
    /// A node's `calls` names a node the graph does not declare as unary.
    UnknownService {
        /// The calling node.
        node: String,
        /// The name in its `calls`.
        service: String,
    },
    /// A node's `calls` names a node of the topology.
    CallsPipelineNode {
        /// The calling node.
        node: String,
        /// The name in its `calls`.
        service: String,
        /// The named node's role.
        role: Role,
    },
    /// The `calls` clauses make a cycle.
    CallCycle {
        /// The unary nodes on the cycle, from the first in name order, each
        /// calling the next and the last calling the first.
        nodes: Vec<String>,
    },
    /// A unary node provides or needs a property kind.
    UnaryProperty {
        /// The unary node.
        node: String,
        /// The clause as the graph writes it: `provides model`, or
        /// `needs model from sets`.
        clause: String,
    },
    /// A unary node is on a hop of the topology.
    UnaryInTopology {
        /// The unary node.
        node: String,
    },
    /// A caller in one class has no copy of a service it calls in its own
    /// segment, and the one copy it would call is in another class.
    CallAcrossClasses {
        /// The calling node.
        node: String,
        /// The caller's segment.
        segment: String,
        /// The unary node it calls.
        service: String,
        /// The caller's class.
        from: String,
        /// The class of the service's segment.
        into: String,
    },
    /// A caller has no copy of a service it calls in its own segment, and
    /// copies sit in more than one other segment.
    CallAmbiguous {
        /// The calling node.
        node: String,
        /// The caller's segment.
        segment: String,
        /// The unary node it calls.
        service: String,
        /// Every segment that lists the service.
        segments: Vec<String>,
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
    /// A node calls a unary node whose only copy is in another segment: the
    /// edge carries the call's request and its response.
    Call,
}

impl PlacementError {
    /// The problem, with the table and the key of the placement to change;
    /// its reader adds the file. A problem of the graph and the placement
    /// together names the table whose tables it is about (`[segment]`,
    /// `[class]`), and a problem of the graph alone names none.
    pub fn problem(&self) -> Problem {
        let at = |table: String, key: Option<&str>| (Some(table), key.map(str::to_string));
        let (table, key) = match self {
            Self::InvalidRouter(node) | Self::InvalidSelector { node, .. } => {
                at("routing".into(), Some(node))
            }
            // A message type that cannot cross is the graph's to change.
            Self::InvalidManifest(_)
            | Self::NotWire { .. }
            | Self::UnknownService { .. }
            | Self::CallsPipelineNode { .. }
            | Self::CallCycle { .. }
            | Self::UnaryInTopology { .. }
            | Self::UnaryProperty { .. } => (None, None),
            // The caller's segment takes a copy of the service.
            Self::CallAcrossClasses { segment, .. } | Self::CallAmbiguous { segment, .. } => {
                at(format!("segment.{segment}"), Some("nodes"))
            }
            Self::UnknownNode { segment, .. } | Self::EmptySegment { segment } => {
                at(format!("segment.{segment}"), Some("nodes"))
            }
            Self::Unplaced { .. } | Self::PlacedTwice { .. } | Self::AroundSplit { .. } => {
                at("segment".into(), None)
            }
            Self::InvalidClassName { class, .. } => at(format!("class.{class}"), None),
            Self::UnknownSegment { class, .. }
            | Self::EmptyClass { class }
            | Self::SegmentRepeated { class, .. } => at(format!("class.{class}"), Some("segments")),
            Self::Unclassed { .. } | Self::ClassedTwice { .. } | Self::ClassCycle { .. } => {
                at("class".into(), None)
            }
            // The stage that breaks the boundary rule moves to another
            // segment, or its segment to another class.
            Self::InnerEndInOuterClass { into, .. } => {
                at(format!("class.{into}"), Some("segments"))
            }
            Self::StageOffOuterSegment { segment, .. } => {
                at(format!("segment.{segment}"), Some("nodes"))
            }
            Self::NestedOutsideClass { class, .. } => {
                at(format!("class.{class}"), Some("segments"))
            }
            Self::InvalidPoolPolicy { class, .. } => at(format!("pools.{class}"), Some("policy")),
            Self::InvalidPoolProvides { class, .. } => {
                at(format!("pools.{class}"), Some("provides"))
            }
            Self::PoolPolicyWithoutClasses { class }
            | Self::PoolPolicyUnknownClass { class }
            | Self::PoolPolicyNotEntered { class } => at(format!("pools.{class}"), None),
        };
        Problem::at(table, key, self.to_string())
    }
}

impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRouter(node) => write!(
                f,
                "request router `{node}` must target a request boundary or a unary node"
            ),
            Self::InvalidSelector { node, reason } => {
                write!(f, "request router `{node}`: {reason}")
            }
            Self::InvalidManifest(why) => write!(f, "invalid manifest: {why}"),
            Self::UnknownNode { segment, node } => {
                write!(f, "segment `{segment}` lists unknown node `{node}`")
            }
            Self::Unplaced { node } => write!(f, "node `{node}` is in no segment"),
            Self::PlacedTwice { node, segments } => write!(
                f,
                "node `{node}` is in several segments: {segments:?}; only a unary node may be"
            ),
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
            Self::InvalidPoolPolicy { class, reason } => {
                write!(f, "pool policy `[pools.{class}]`: {reason}")
            }
            Self::InvalidPoolProvides { class, reason } => {
                write!(f, "`[pools.{class}] provides`: {reason}")
            }
            Self::PoolPolicyWithoutClasses { class } => write!(
                f,
                "pool policy `[pools.{class}]`: the placement has no classes"
            ),
            Self::PoolPolicyUnknownClass { class } => write!(
                f,
                "pool policy `[pools.{class}]`: the placement has no class `{class}`"
            ),
            Self::PoolPolicyNotEntered { class } => write!(
                f,
                "pool policy `[pools.{class}]`: no class boundary enters class `{class}`; a pool policy picks a pool of a class that another class sends to"
            ),
            Self::AroundSplit {
                node,
                hop,
                forward,
                backward,
            } => write!(
                f,
                "around node `{node}` (hop {hop}): its forward half runs in {forward} and its backward half in {backward}; place both halves where one task runs them, with the hop's outer end or with its inner end"
            ),
            Self::NotWire {
                from,
                to,
                ty,
                reason,
            } => {
                let why = match reason {
                    WireReason::Crossing => "crosses a segment boundary",
                    WireReason::AnchorCarried => "is carried by a reply anchor",
                    WireReason::Call => {
                        return write!(
                            f,
                            "`{from}` calls `{to}` in another segment, but `{ty}` is not serializable; a remote call encodes its request and its response"
                        );
                    }
                };
                write!(
                    f,
                    "edge `{from}` -> `{to}` {why}, but its type `{ty}` is not serializable"
                )
            }
            Self::UnknownService { node, service } => write!(
                f,
                "node `{node}` has `calls {service}`, but the graph declares no unary node `{service}`: declare it with `unary {service}: <Req> -> <Resp>;`"
            ),
            Self::CallsPipelineNode {
                node,
                service,
                role,
            } => write!(
                f,
                "node `{node}` has `calls {service}`, but `{service}` is a {} in the topology; `calls` names only unary nodes",
                role.name()
            ),
            Self::CallCycle { nodes } => {
                let mut text = String::new();
                for (index, node) in nodes.iter().enumerate() {
                    let next = &nodes[(index + 1) % nodes.len()];
                    if index > 0 {
                        text.push_str(", and ");
                    }
                    text.push_str(&format!("`{node}` calls `{next}`"));
                }
                write!(
                    f,
                    "the `calls` clauses make a cycle: {text}; a unary node may call another only when no chain of calls leads back to it"
                )
            }
            Self::UnaryProperty { node, clause } => write!(
                f,
                "unary node `{node}` has `{clause}`; a unary node cannot provide or need a property"
            ),
            Self::UnaryInTopology { node } => write!(
                f,
                "unary node `{node}` is in the topology; a unary node is on no hop, and a node reaches it with `calls {node}`"
            ),
            Self::CallAcrossClasses {
                node,
                segment,
                service,
                from,
                into,
            } => write!(
                f,
                "node `{node}` in segment `{segment}` of class `{from}` has `calls {service}`, but the only copy of `{service}` is in class `{into}`; a call stays in its class: list `{service}` in a segment of class `{from}`"
            ),
            Self::CallAmbiguous {
                node,
                segment,
                service,
                segments,
            } => write!(
                f,
                "node `{node}` in segment `{segment}` has `calls {service}`, but `{segment}` lists no copy of `{service}`, and copies sit in several segments: {segments:?}; list `{service}` in `{segment}`, or in only one other segment"
            ),
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

impl From<PlacementErrors> for Problems {
    fn from(errors: PlacementErrors) -> Self {
        Problems(errors.0.iter().map(PlacementError::problem).collect())
    }
}

impl From<PlacementErrors> for Error {
    fn from(errors: PlacementErrors) -> Self {
        Error::new(errors.to_string())
    }
}

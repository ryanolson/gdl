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
    /// A cell policy's name is not one a policy can have.
    InvalidCellPolicy {
        /// The class the policy is for.
        class: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A placement without classes has a cell policy.
    CellPolicyWithoutClasses {
        /// The class the policy names.
        class: String,
    },
    /// A cell policy names a class the placement does not have.
    CellPolicyUnknownClass {
        /// The class the policy names.
        class: String,
    },
    /// A cell policy is for a class that no class boundary enters, so no
    /// chooser picks a cell of it.
    CellPolicyNotEntered {
        /// The class the policy names.
        class: String,
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
            Self::InvalidManifest(_) | Self::NotWire { .. } => (None, None),
            Self::UnknownNode { segment, .. } | Self::EmptySegment { segment } => {
                at(format!("segment.{segment}"), Some("nodes"))
            }
            Self::Unplaced { .. } | Self::PlacedTwice { .. } => at("segment".into(), None),
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
            Self::InvalidCellPolicy { class, .. } => at(format!("cells.{class}"), Some("policy")),
            Self::CellPolicyWithoutClasses { class }
            | Self::CellPolicyUnknownClass { class }
            | Self::CellPolicyNotEntered { class } => at(format!("cells.{class}"), None),
        };
        Problem::at(table, key, self.to_string())
    }
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
            Self::InvalidCellPolicy { class, reason } => {
                write!(f, "cell policy `[cells.{class}]`: {reason}")
            }
            Self::CellPolicyWithoutClasses { class } => write!(
                f,
                "cell policy `[cells.{class}]`: the placement has no classes"
            ),
            Self::CellPolicyUnknownClass { class } => write!(
                f,
                "cell policy `[cells.{class}]`: the placement has no class `{class}`"
            ),
            Self::CellPolicyNotEntered { class } => write!(
                f,
                "cell policy `[cells.{class}]`: no class boundary enters class `{class}`; a cell policy picks a cell of a class that another class sends to"
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

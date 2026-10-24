use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};

use crate::hash::Fnv;
use crate::manifest::Manifest;

use super::{ClassBoundary, Placement, PlacementError, check_name_shape};

/// How a chooser picks a cell of a class it sends to: a placement's
/// `[cells.<class>]` table. Its one key is optional in the table, and a
/// value equal to its default is not recorded. Unknown keys are refused,
/// `spill_rtt_ms` and `max_near` among them: they chose near cells, which
/// a chooser does not follow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CellPolicy {
    /// The policy's name, [`CellPolicy::DEFAULT_POLICY`] by default. Named
    /// like a selector, and not checked against a list, since a custom
    /// policy can be registered later.
    #[serde(skip_serializing_if = "is_default_policy")]
    pub policy: String,
}

impl CellPolicy {
    /// `policy` when the table does not set it: `round_robin`.
    pub const DEFAULT_POLICY: &'static str = "round_robin";
}

impl Default for CellPolicy {
    fn default() -> Self {
        Self {
            policy: Self::DEFAULT_POLICY.into(),
        }
    }
}

fn is_default_policy(policy: &str) -> bool {
    policy == CellPolicy::DEFAULT_POLICY
}

/// The `[cells]` tables as read, without those that hold only defaults:
/// a placement that spells a default out is the placement that leaves it
/// unsaid, from TOML and from the hub's JSON alike.
pub(super) fn recorded<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, CellPolicy>, D::Error> {
    let mut cells = BTreeMap::<String, CellPolicy>::deserialize(deserializer)?;
    cells.retain(|_, policy| *policy != CellPolicy::default());
    Ok(cells)
}

/// Why `policy` cannot name a cell policy, if it cannot.
pub(super) fn check_policy_name(policy: &CellPolicy) -> Result<(), String> {
    check_name_shape("policy", &policy.policy)
}

/// Hash the cell policies after the classes: each class's effective
/// policy, the default included, so that a change of default changes the
/// hash rather than what a placement means, and each table that names no
/// class, which layout refuses. The marker starts them where a class's, a
/// segment's, or a router's name would start, with a value no name's
/// length can have and the classes' marker is not, so no cell policy
/// hashes as any of them. A placement with neither classes nor tables
/// writes nothing and keeps the hash it had before cell policies existed.
pub(super) fn hash<'a>(
    h: &mut Fnv,
    classes: impl Iterator<Item = &'a String> + Clone,
    cells: &'a BTreeMap<String, CellPolicy>,
) {
    let count = hashed(classes.clone(), cells).count();
    if count == 0 {
        return;
    }
    h.u64(u64::MAX - 1);
    h.u64(count as u64);
    for class in hashed(classes, cells) {
        h.str(class);
        h.str(
            cells
                .get(class)
                .map_or(CellPolicy::DEFAULT_POLICY, |policy| &policy.policy),
        );
    }
}

/// The classes, and the classes the tables recorded with a value name,
/// once each in name order: both are in name order, so they merge without
/// a set, and hashing a class costs what hashing a segment's node does. A
/// table spelled with the default only is not among them: its class, if
/// the placement has it, hashes the default as every class does, and if
/// not, layout refuses it.
fn hashed<'a>(
    classes: impl Iterator<Item = &'a String>,
    cells: &'a BTreeMap<String, CellPolicy>,
) -> impl Iterator<Item = &'a str> {
    let mut classes = classes.peekable();
    let mut tables = cells.keys().peekable();
    std::iter::from_fn(move || {
        let next = match (classes.peek(), tables.peek()) {
            (None, None) => None,
            (Some(class), Some(table)) if class == table => {
                tables.next();
                classes.next()
            }
            (Some(class), Some(table)) if class < table => classes.next(),
            (_, Some(_)) => tables.next(),
            (Some(_), None) => classes.next(),
        };
        next.map(String::as_str)
    })
}

/// Every class a `[cells]` table names, with a value or with defaults only.
fn named(placement: &Placement) -> BTreeSet<&String> {
    placement
        .cells
        .keys()
        .chain(&placement.spelled.cells)
        .collect()
}

/// The checks that need only the placement: each table's policy name, and
/// that it names a class the placement has.
pub(super) fn judge(placement: &Placement, errors: &mut Vec<PlacementError>) {
    for (class, policy) in &placement.cells {
        if let Err(reason) = check_policy_name(policy) {
            errors.push(PlacementError::InvalidCellPolicy {
                class: class.clone(),
                reason,
            });
        }
    }
    for class in named(placement) {
        if placement.classes.is_empty() {
            errors.push(PlacementError::CellPolicyWithoutClasses {
                class: class.clone(),
            });
        } else if !placement.classes.contains_key(class) {
            errors.push(PlacementError::CellPolicyUnknownClass {
                class: class.clone(),
            });
        }
    }
}

/// The effective policy of each class a boundary enters, checking that
/// each table names such a class. A class that a hop with an unplaced or
/// unclassed end could still enter is not judged, so that the problem is
/// reported once, as what it is; every other class is.
pub(super) fn entered(
    placement: &Placement,
    boundaries: &[ClassBoundary],
    undecided: &Undecided<'_>,
    errors: &mut Vec<PlacementError>,
) -> BTreeMap<String, CellPolicy> {
    let into: BTreeSet<&str> = boundaries.iter().map(|b| b.into.as_str()).collect();
    for class in named(placement) {
        if placement.classes.contains_key(class)
            && !into.contains(class.as_str())
            && !undecided.may_enter(class)
        {
            errors.push(PlacementError::CellPolicyNotEntered {
                class: class.clone(),
            });
        }
    }
    into.into_iter()
        .map(|class| {
            let policy = placement.cells.get(class).cloned().unwrap_or_default();
            (class.to_string(), policy)
        })
        .collect()
}

/// The classes that hops with an unplaced or unclassed end could enter,
/// were those ends placed. A hop enters a class when its inner end is in
/// it and its outer end is not: a hop with only its outer end unresolved
/// could enter its inner end's class, and one with its inner end
/// unresolved could enter any class but its outer end's, unless both ends
/// are in one segment, which has one class. Counted once per hop, so that
/// each class is answered without walking the hops again.
pub(super) struct Undecided<'a> {
    /// The inner ends' classes of hops whose outer end is unresolved.
    into: BTreeSet<&'a str>,
    /// Hops whose inner end is unresolved.
    open: usize,
    /// Of those, how many have their outer end in each class.
    open_from: BTreeMap<&'a str, usize>,
}

impl<'a> Undecided<'a> {
    pub(super) fn new(
        manifest: &'a Manifest,
        segment_of: &'a BTreeMap<String, String>,
        class_of: &'a BTreeMap<String, String>,
    ) -> Self {
        let class = |node: &str| {
            segment_of
                .get(node)
                .and_then(|segment| class_of.get(segment))
                .map(String::as_str)
        };
        let mut undecided = Self {
            into: BTreeSet::new(),
            open: 0,
            open_from: BTreeMap::new(),
        };
        for hop in &manifest.hops {
            // Ends in one segment share whatever class it is given, so the
            // hop enters none.
            let segment = |node: &str| segment_of.get(node);
            if segment(&hop.outer).is_some() && segment(&hop.outer) == segment(&hop.inner) {
                continue;
            }
            match (class(&hop.outer), class(&hop.inner)) {
                (_, None) => {
                    undecided.open += 1;
                    if let Some(from) = class(&hop.outer) {
                        *undecided.open_from.entry(from).or_default() += 1;
                    }
                }
                (None, Some(into)) => {
                    undecided.into.insert(into);
                }
                (Some(_), Some(_)) => {}
            }
        }
        undecided
    }

    /// Whether a hop with an unresolved end could enter `class`.
    fn may_enter(&self, class: &str) -> bool {
        self.into.contains(class) || self.open > self.open_from.get(class).copied().unwrap_or(0)
    }
}

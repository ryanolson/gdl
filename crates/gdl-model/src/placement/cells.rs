use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};

use crate::hash::Fnv;

use super::{ClassBoundary, Placement, PlacementError, check_name_shape};

/// How a chooser picks a cell of a class it sends to: a placement's
/// `[cells.<class>]` table. Each value is optional in the table, and a
/// value equal to its default is not recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CellPolicy {
    /// The policy's name, `home_first` by default. Named like a selector,
    /// and not checked against a list, since a custom policy can be
    /// registered later.
    #[serde(skip_serializing_if = "is_default_policy")]
    pub policy: String,
    /// Cells in other regions within this round trip, in milliseconds,
    /// are near; 80 by default.
    #[serde(skip_serializing_if = "is_default_spill_rtt_ms")]
    pub spill_rtt_ms: u64,
    /// At most this many near cells are offered; 16 by default. 0 offers
    /// home cells only.
    #[serde(skip_serializing_if = "is_default_max_near")]
    pub max_near: u32,
}

const DEFAULT_POLICY: &str = "home_first";
const DEFAULT_SPILL_RTT_MS: u64 = 80;
const DEFAULT_MAX_NEAR: u32 = 16;

impl Default for CellPolicy {
    fn default() -> Self {
        Self {
            policy: DEFAULT_POLICY.into(),
            spill_rtt_ms: DEFAULT_SPILL_RTT_MS,
            max_near: DEFAULT_MAX_NEAR,
        }
    }
}

fn is_default_policy(policy: &str) -> bool {
    policy == DEFAULT_POLICY
}

fn is_default_spill_rtt_ms(value: &u64) -> bool {
    *value == DEFAULT_SPILL_RTT_MS
}

fn is_default_max_near(value: &u32) -> bool {
    *value == DEFAULT_MAX_NEAR
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

/// Hash the cell policies after the classes. The marker starts them where
/// a class's, a segment's, or a router's name would start, with a value no
/// name's length can have and the classes' marker is not, so no cell
/// policy hashes as any of them; every entry then has the same fields. The
/// effective values are hashed, which is unambiguous because a table of
/// defaults is never recorded. A placement without cell policies writes
/// nothing and keeps the hash it had before they existed.
pub(super) fn hash(h: &mut Fnv, cells: &BTreeMap<String, CellPolicy>) {
    if cells.is_empty() {
        return;
    }
    h.u64(u64::MAX - 1);
    h.u64(cells.len() as u64);
    for (class, policy) in cells {
        h.str(class);
        h.str(&policy.policy);
        h.u64(policy.spill_rtt_ms);
        h.u64(u64::from(policy.max_near));
    }
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
/// each table names such a class. `complete` says whether every node is
/// placed and every segment classed: otherwise a boundary through an
/// unplaced node is missing, and no class is judged not entered, so that
/// the problem is reported once, as what it is.
pub(super) fn entered(
    placement: &Placement,
    boundaries: &[ClassBoundary],
    complete: bool,
    errors: &mut Vec<PlacementError>,
) -> BTreeMap<String, CellPolicy> {
    let into: BTreeSet<&str> = boundaries.iter().map(|b| b.into.as_str()).collect();
    if complete {
        for class in placement.cells.keys() {
            if placement.classes.contains_key(class) && !into.contains(class.as_str()) {
                errors.push(PlacementError::CellPolicyNotEntered {
                    class: class.clone(),
                });
            }
        }
    }
    into.into_iter()
        .map(|class| {
            let policy = placement.cells.get(class).cloned().unwrap_or_default();
            (class.to_string(), policy)
        })
        .collect()
}

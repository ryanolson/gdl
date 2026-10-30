//! Where a cluster's pods run, and what they ask the scheduler for: a node
//! selector, tolerations, and CPU and memory requests and limits. A
//! cluster's `resources` give every pod it runs its requests and limits, or
//! none: a default with all four, each segment's own (`hub` for the hubs),
//! and a pool's own over its cluster's, merged field by field. Every value
//! is checked as Kubernetes would check it, so that a malformed one is
//! refused by `check`, not by the API server at the apply.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{FleetError, FleetTable};

/// The pods of a pool's hub, under `resources.segment`: no segment can be
/// named `hub`.
pub const HUB: &str = "hub";

/// A toleration of a node's taint, as a pod's `tolerations` lists it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Toleration {
    /// The taint's key; empty, with `Exists`, tolerates every taint.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
    /// `Equal` or `Exists`; empty is `Equal`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operator: String,
    /// The taint's value, for `Equal`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    /// `NoSchedule`, `PreferNoSchedule`, or `NoExecute`; empty matches
    /// every effect.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub effect: String,
    /// How long a pod stays bound once a `NoExecute` taint is added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

/// A cluster's `resources`: a default for every pod, and each segment's
/// own, by the segment's name or [`HUB`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterResources {
    /// Every pod's requests and limits, unless its segment's own say
    /// otherwise: all four are required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Resources>,
    /// Each segment's own, over the default.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub segment: BTreeMap<String, Resources>,
}

/// A pool's `resources`: each segment's own, by the segment's name or
/// [`HUB`], over its cluster's.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolResources {
    /// Each segment's own, over its cluster's.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub segment: BTreeMap<String, Resources>,
}

/// A container's requests and limits, each of CPU and memory as a
/// Kubernetes quantity (`250m`, `1`, `512Mi`). Any may be unsaid where a
/// merge fills it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    /// What the scheduler reserves for the container.
    #[serde(default, skip_serializing_if = "Quantities::is_empty")]
    pub requests: Quantities,
    /// The most the container may use.
    #[serde(default, skip_serializing_if = "Quantities::is_empty")]
    pub limits: Quantities,
}

/// A CPU and a memory quantity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Quantities {
    /// CPU, in cores: `250m`, `1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Memory, in bytes: `512Mi`, `1G`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

impl Quantities {
    fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none()
    }

    /// `self`, each unsaid field taken from `under`.
    fn over(&self, under: &Quantities) -> Quantities {
        Quantities {
            cpu: self.cpu.clone().or_else(|| under.cpu.clone()),
            memory: self.memory.clone().or_else(|| under.memory.clone()),
        }
    }
}

impl Resources {
    /// Requests and limits of `cpu` and `memory`, each `(request, limit)`.
    pub fn new(cpu: (&str, &str), memory: (&str, &str)) -> Self {
        Self {
            requests: Quantities {
                cpu: Some(cpu.0.into()),
                memory: Some(memory.0.into()),
            },
            limits: Quantities {
                cpu: Some(cpu.1.into()),
                memory: Some(memory.1.into()),
            },
        }
    }

    /// `self`, each unsaid field taken from `under`.
    pub(super) fn over(&self, under: &Resources) -> Resources {
        Resources {
            requests: self.requests.over(&under.requests),
            limits: self.limits.over(&under.limits),
        }
    }

    /// Each field with its name and value, as `requests.cpu`.
    fn fields(&self) -> [(&'static str, &'static str, Option<&String>); 4] {
        [
            ("requests", "cpu", self.requests.cpu.as_ref()),
            ("requests", "memory", self.requests.memory.as_ref()),
            ("limits", "cpu", self.limits.cpu.as_ref()),
            ("limits", "memory", self.limits.memory.as_ref()),
        ]
    }
}

/// The most significant digits a quantity may have: more than any value
/// Kubernetes keeps, and few enough that every one fits the arithmetic.
const MAX_DIGITS: usize = 30;

/// A Kubernetes quantity's value, in billionths of its unit: exact, so
/// that `1` equals `1000m` and `1Gi` is more than `1G`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Quantity(u128);

impl Quantity {
    /// Parse `text` as Kubernetes reads a quantity: a non-negative decimal
    /// number, then a binary suffix (`Ki` to `Ei`), a decimal one (`n`,
    /// `u`, `m`, `k`, `M` to `E`), an exponent (`e3`), or none.
    pub fn parse(text: &str) -> Result<Self, String> {
        let digits = text
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(text.len());
        let (number, suffix) = text.split_at(digits);
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if whole.is_empty() && fraction.is_empty() || fraction.contains('.') {
            return Err("not a number".into());
        }
        let (power_of_ten, power_of_two): (i32, u32) = match suffix {
            "" => (0, 0),
            "n" => (-9, 0),
            "u" => (-6, 0),
            "m" => (-3, 0),
            "k" => (3, 0),
            "M" => (6, 0),
            "G" => (9, 0),
            "T" => (12, 0),
            "P" => (15, 0),
            "E" => (18, 0),
            "Ki" => (0, 10),
            "Mi" => (0, 20),
            "Gi" => (0, 30),
            "Ti" => (0, 40),
            "Pi" => (0, 50),
            "Ei" => (0, 60),
            exponent => match exponent
                .strip_prefix(['e', 'E'])
                .and_then(|e| e.parse::<i32>().ok())
            {
                Some(e) if (-9..=18).contains(&e) => (e, 0),
                _ => return Err(format!("`{suffix}` is not a unit")),
            },
        };
        // Leading zeros of the number and trailing zeros of its fraction
        // say nothing, and a long run of either must not reach a power: so
        // `0.000…0` of any length is 0, and every power below is checked.
        let fraction = fraction.trim_end_matches('0');
        let digits = format!("{whole}{fraction}");
        let digits = digits.trim_start_matches('0');
        if digits.len() > MAX_DIGITS {
            return Err(format!("more than {MAX_DIGITS} significant digits"));
        }
        let mantissa: u128 = if digits.is_empty() {
            0
        } else {
            digits.parse().map_err(|_| "not a number".to_string())?
        };
        // The value in billionths: mantissa x 10^(9 + power - fraction digits).
        let fraction_digits = i32::try_from(fraction.len()).map_err(|_| "too long")?;
        let scale = 9 + power_of_ten - fraction_digits;
        let power = 10u128.checked_pow(scale.unsigned_abs());
        let mut value = mantissa;
        if mantissa != 0 {
            if scale >= 0 {
                value = power
                    .and_then(|power| value.checked_mul(power))
                    .ok_or("too large")?;
            } else {
                match power {
                    Some(divisor) if value.is_multiple_of(divisor) => value /= divisor,
                    _ => return Err("finer than a billionth of its unit".into()),
                }
            }
        }
        value = value
            .checked_mul(1u128 << power_of_two)
            .ok_or("too large")?;
        Ok(Self(value))
    }
}

/// What is wrong with a cluster's selector, tolerations, and resources, at
/// its table.
pub(super) fn check_cluster(
    name: &str,
    node_selector: &BTreeMap<String, String>,
    tolerations: &[Toleration],
    resources: Option<&ClusterResources>,
    errors: &mut Vec<FleetError>,
) {
    let problem = |key: String, reason: String| FleetError::InvalidPlacement {
        table: FleetTable::Cluster(name.into()),
        key,
        reason,
    };
    for (key, value) in node_selector {
        let at = format!("node_selector.{key}");
        if !super::cluster::annotation_key(key) {
            errors.push(problem(at.clone(), format!("`{key}` is not a label key: an optional DNS subdomain and `/`, then at most 63 letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit")));
        }
        if ["kubernetes.io/os", "kubernetes.io/arch"].contains(&key.as_str()) {
            errors.push(problem(
                at.clone(),
                format!("`{key}` is set from the cluster's `platform`"),
            ));
        }
        if !label_value(value) {
            errors.push(problem(at, format!("`{value}` is not a label value: at most 63 letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit")));
        }
    }
    for (index, toleration) in tolerations.iter().enumerate() {
        for reason in toleration_problems(toleration) {
            errors.push(problem(format!("tolerations[{index}]"), reason));
        }
    }
    let Some(resources) = resources else {
        return;
    };
    let Some(default) = &resources.default else {
        errors.push(problem(
            "resources".into(),
            "has no `default`: every pod of the cluster takes its requests and limits from it, so it gives all four".into(),
        ));
        return;
    };
    errors.extend(
        default_problems(default)
            .into_iter()
            .map(|reason| problem("resources.default".into(), reason)),
    );
    for (segment, own) in &resources.segment {
        errors.extend(
            quantity_problems(own)
                .into_iter()
                .chain(limit_problems(&own.over(default)))
                .map(|reason| problem(format!("resources.segment.{segment}"), reason)),
        );
    }
}

/// What is wrong with a pool's resources over its cluster's, at its table.
pub(super) fn check_pool(
    name: &str,
    own: &PoolResources,
    cluster: Option<&ClusterResources>,
    errors: &mut Vec<FleetError>,
) {
    let problem = |key: String, reason: String| FleetError::InvalidPlacement {
        table: FleetTable::Pool(name.into()),
        key,
        reason,
    };
    let Some(default) = cluster.and_then(|cluster| cluster.default.as_ref()) else {
        if !own.segment.is_empty() {
            errors.push(problem(
                "resources".into(),
                "its cluster gives no resources: give the cluster `[cluster.<name>.resources.default]`, so that every pod gets its requests and limits".into(),
            ));
        }
        return;
    };
    let cluster = cluster.expect("a default is the cluster's");
    for (segment, resources) in &own.segment {
        let under = match cluster.segment.get(segment) {
            Some(segment) => segment.over(default),
            None => default.clone(),
        };
        errors.extend(
            quantity_problems(resources)
                .into_iter()
                .chain(limit_problems(&resources.over(&under)))
                .map(|reason| problem(format!("resources.segment.{segment}"), reason)),
        );
    }
}

/// Each key of `segments` that is neither one of `members` nor [`HUB`],
/// at `table`.
pub(super) fn check_segments<'a>(
    table: FleetTable,
    segments: impl IntoIterator<Item = &'a String>,
    members: &BTreeSet<&str>,
    errors: &mut Vec<FleetError>,
) {
    for segment in segments {
        if segment != HUB && !members.contains(segment.as_str()) {
            errors.push(FleetError::InvalidPlacement {
                table: table.clone(),
                key: format!("resources.segment.{segment}"),
                reason: format!("`{segment}` is not a segment of the pods here, nor `{HUB}`"),
            });
        }
    }
}

/// What is wrong with a cluster's default: each quantity that does not
/// parse, each of the four it lacks, and each limit below its request.
fn default_problems(default: &Resources) -> Vec<String> {
    let mut problems = quantity_problems(default);
    for (part, kind, value) in default.fields() {
        if value.is_none() {
            problems.push(format!(
                "gives no {kind} {}: the default gives all four, requests and limits of cpu and memory",
                &part[..part.len() - 1]
            ));
        }
    }
    problems.extend(limit_problems(default));
    problems
}

/// Each limit of `resources`, a merge as a pod gets it, below its request.
fn limit_problems(resources: &Resources) -> Vec<String> {
    let mut problems = Vec::new();
    for (kind, request, limit) in [
        ("cpu", &resources.requests.cpu, &resources.limits.cpu),
        (
            "memory",
            &resources.requests.memory,
            &resources.limits.memory,
        ),
    ] {
        if let (Some(request), Some(limit)) = (request, limit)
            && let (Ok(r), Ok(l)) = (Quantity::parse(request), Quantity::parse(limit))
            && l < r
        {
            problems.push(format!(
                "its {kind} limit `{limit}` is below its request `{request}`"
            ));
        }
    }
    problems
}

/// Each quantity of `resources` that does not parse.
fn quantity_problems(resources: &Resources) -> Vec<String> {
    resources
        .fields()
        .into_iter()
        .filter_map(|(part, kind, value)| {
            let value = value?;
            Quantity::parse(value)
                .err()
                .map(|why| format!("{part}.{kind} `{value}` is not a quantity: {why}"))
        })
        .collect()
}

/// What is wrong with `toleration`, as Kubernetes judges one.
fn toleration_problems(toleration: &Toleration) -> Vec<String> {
    let mut problems = Vec::new();
    let exists = match toleration.operator.as_str() {
        "" | "Equal" => false,
        "Exists" => true,
        other => {
            problems.push(format!(
                "operator `{other}` is neither `Equal` nor `Exists`"
            ));
            false
        }
    };
    if !["", "NoSchedule", "PreferNoSchedule", "NoExecute"].contains(&toleration.effect.as_str()) {
        problems.push(format!(
            "effect `{}` is not `NoSchedule`, `PreferNoSchedule`, or `NoExecute`",
            toleration.effect
        ));
    }
    if exists && !toleration.value.is_empty() {
        problems.push("operator `Exists` takes no value".into());
    }
    if toleration.key.is_empty() && !exists {
        problems.push("an empty key tolerates every taint only with operator `Exists`".into());
    }
    if !toleration.key.is_empty() && !super::cluster::annotation_key(&toleration.key) {
        problems.push(format!("key `{}` is not a label key", toleration.key));
    }
    if !toleration.value.is_empty() && !label_value(&toleration.value) {
        problems.push(format!("value `{}` is not a label value", toleration.value));
    }
    if toleration.toleration_seconds.is_some() && toleration.effect != "NoExecute" {
        problems.push("toleration_seconds applies to effect `NoExecute` only".into());
    }
    problems
}

/// Whether `value` is a label's value: at most 63 letters, digits, `-`,
/// `_`, and `.`, starting and ending with a letter or digit, or empty.
fn label_value(value: &str) -> bool {
    value.is_empty()
        || value.len() <= 63
            && value.starts_with(|c: char| c.is_ascii_alphanumeric())
            && value.ends_with(|c: char| c.is_ascii_alphanumeric())
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

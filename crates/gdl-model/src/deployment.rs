use serde::{Deserialize, Serialize};

use crate::{Error, Manifest, Placement, layout};

/// The port the hub's public listener takes inside a deployment. Every
/// segment, and the hub's own health check, reach the hub through it; it is
/// named once so they cannot disagree.
pub const HUB_PUBLIC_PORT: u16 = 1337;

/// The port the hub's control listener takes inside a deployment.
pub const HUB_CONTROL_PORT: u16 = 8337;

/// The deadline a request gets when it crosses segments without one, and so
/// by default how long a stopped segment may drain. The runtime takes its
/// default from here, and the generated Compose file waits this long plus a
/// margin before Docker kills a stopped container.
pub const DEFAULT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(600);

/// The longest a segment name may be: a hub names a segment's readiness
/// services `gdl-loaded-<hex>` and `gdl-serving-<hex>`, two characters
/// per byte of the name, within velo-hub's 128.
pub const SEGMENT_NAME_LIMIT: usize = 58;

/// Why `name` cannot name a segment, if it cannot: the one rule the
/// generator and the composer both apply. A segment is named in settings
/// keys, where a key set in the environment arrives lowercased, and in a
/// hub's service names, which have a narrow alphabet and a bound.
pub fn segment_name_problem(name: &str) -> Option<String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Some(format!(
            "segment `{name}`: a segment name is lowercase letters, digits, `-`, and `_`"
        ));
    }
    if name.chars().any(|c| c.is_ascii_uppercase()) {
        return Some(format!(
            "segment `{name}`: a segment name is lowercase, so that `GDL_INSTANCES__{}` names it",
            name.to_ascii_uppercase()
        ));
    }
    if name.len() > SEGMENT_NAME_LIMIT {
        return Some(format!(
            "segment `{name}`: a segment name is at most {SEGMENT_NAME_LIMIT} characters, so that its hub services can be named"
        ));
    }
    None
}

/// The graph and placement a deployment's discovery hub accepts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentSpec {
    /// Typed graph contract.
    pub graph: Manifest,
    /// Nodes assigned to each independently deployed process.
    pub placement: Placement,
}

impl DeploymentSpec {
    /// Validate the placement before accepting registrations.
    pub fn new(graph: Manifest, placement: Placement) -> Result<Self, Error> {
        layout(&graph, &placement).map_err(|e| Error::new(e.to_string()))?;
        Ok(Self { graph, placement })
    }

    /// Describe the contract compiled into one segment.
    pub fn registration(&self, segment: &str) -> Result<SegmentRegistration, Error> {
        if !self.placement.segments().any(|(name, _)| name == segment) {
            return Err(Error::new(format!("unknown segment `{segment}`")));
        }
        let layout = layout(&self.graph, &self.placement).map_err(|e| Error::new(e.to_string()))?;
        Ok(SegmentRegistration {
            segment: segment.into(),
            contract: layout.contract_hash(),
            placement: layout.placement_hash(),
        })
    }

    /// Refuse a binary built for a different graph or placement.
    pub fn validate_registration(&self, registration: &SegmentRegistration) -> Result<(), Error> {
        if self.registration(&registration.segment)? != *registration {
            return Err(Error::new(format!(
                "segment `{}` has a different graph contract or placement",
                registration.segment
            )));
        }
        Ok(())
    }
}

/// The compiled identity presented during hub registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRegistration {
    /// Segment name in the placement.
    pub segment: String,
    /// Hash of the typed graph contract.
    pub contract: u64,
    /// Hash of node assignments, routing policies, and classes.
    pub placement: u64,
}

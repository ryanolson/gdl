use serde::{Deserialize, Serialize};

use crate::{Error, Manifest, Placement, layout};

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
    /// Hash of node assignments and routing policies.
    pub placement: u64,
}

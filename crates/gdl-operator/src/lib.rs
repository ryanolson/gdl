//! The gdl Kubernetes operator. A `GdlScaler` scales one segment of a
//! pool: an HPA, or a person, writes its `/scale`, and on a scale-down the
//! operator chooses the instance to remove by its load, takes it out of
//! its entry Service, and steers the ReplicaSet to delete exactly that pod
//! with `controller.kubernetes.io/pod-deletion-cost`. The instance retires
//! and drains on its pod's SIGTERM, as it does under any stop.
//!
//! The operator talks to the Kubernetes API and to the pool hub's HTTP
//! routes alone, and never links velo. See `docs/src/guides/operator.md`.

mod crd;
mod manifests;
mod victim;

pub use crd::{
    DEFAULT_PREFIX, DELETION_COST, GROUP, GdlScaler, GdlScalerSpec, GdlScalerStatus, Last, Outcome,
    ROUTABLE, Retire, Retiring, RetiringPhase, SCALER_MANAGER, Suspend, TargetRef, VERSION,
    parse_duration,
};
pub use manifests::{NAME, crds, namespaced, stream};
pub use victim::{FEWEST_ADMITTED, NEWEST};

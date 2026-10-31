//! A gdl pool's Kubernetes objects, rendered from the pool alone
//! ([`Pool`]): its ConfigMap, its hub's Deployment and Service, and each
//! segment's Deployment, entry Service, and `GdlScaler`. `cargo gdl k8s`
//! and the gdl operator both render with it, so the two write the same
//! objects. It never links velo. See `docs/src/guides/kubernetes.md`.

mod labels;
mod pool;
mod render;
mod settings;

pub use labels::{LABEL_LIMIT, LABEL_PREFIX, checksum, fnv, label_value};
pub use pool::{
    Dynamodb, Env, Hub, Parent, Pool, Port, Presence, Protocol, Resources, Secret, Segment,
    Toleration,
};
pub use render::{
    BINARY, HUB_METRICS, POD_IP, PUBLIC, ROUTABLE, SEGMENT_METRICS, SYSTEM, Scaling, children,
    hub_address, object_name, place, pod_hub, resources, state_claim,
};
pub use settings::{Settings, drain_bound, stop_grace};

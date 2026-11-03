//! A gdl pool's Kubernetes objects, rendered from the pool alone
//! ([`Pool`]): its ConfigMap, its hub's Deployment and Service, and each
//! segment's Deployment, entry Service, and `GdlScaler`, and its DGD when
//! dynamo's operator runs some of its segments. `cargo gdl k8s`
//! and the gdl operator both render with it, so the two write the same
//! objects. It never links velo. See `docs/src/guides/kubernetes.md`.

mod dgd;
mod labels;
mod monitor;
mod pool;
mod render;
mod settings;

pub use dgd::{COMPONENT_LABEL, DGD_API_VERSION, DGD_KIND, DGD_LABEL, segment_components};
pub use labels::{LABEL_LIMIT, LABEL_PREFIX, checksum, fnv, label_value};
pub use monitor::{pod_monitor, pool_monitor};
pub use pool::{
    Dynamodb, Env, Hub, Parent, Pool, Port, Presence, Protocol, Resources, Secret, Segment,
    Toleration,
};
pub use render::{
    BINARY, HUB_METRICS, POD_IP, PUBLIC, ROUTABLE, SEGMENT_METRICS, SYSTEM, Scaling, children,
    hub_address, object_name, place, pod_hub, pool_object, resources, rollover, state_claim,
};
pub use settings::{Settings, drain_bound, stop_grace};

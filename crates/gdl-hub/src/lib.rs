//! Discovery hub with admission restricted to one GDL deployment.
//!
//! [`GraphAdmission`] admits only the segments of one deployment's graph
//! and placement. A cell's hub runs [`CellReadiness`] beside it: the
//! `gdl-cell` extension, which judges from the cell's usable instances and
//! its quorum ([`Quorum`]) whether the cell can take work, answers on the
//! cell health endpoint (`GET /v1/ext/gdl-cell/health`), publishes the
//! cell's record through a [`gdl_cells::CellDirectory`], and serves the
//! cell's watch (`GET /v1/ext/gdl-cell/watch`): the cell's state and epoch
//! and its entry instances, as [`CellWatchEvent`]s, which a hub that
//! follows the cell reads with a [`CellWatcher`]. The hub of a cell whose
//! class sends to other classes is such a follower: it finds their home
//! cells in the directory, follows each over its watch, and relays what it
//! follows to its own instances (`GET /v1/ext/gdl-cell/follows`, as
//! [`FollowEvent`]s, which a [`CellWatcher`] reads too).
mod cell;
mod fanout;
mod follow;
mod quorum;
mod setup;
mod watch;

pub use cell::{CellError, CellHealth, CellReadiness, CellTiming, CellWatch, SegmentHealth};
pub use follow::FollowsWatch;
pub use quorum::Quorum;
pub use setup::{cell_from_env, hub_builder};
pub use watch::{CellWatchError, CellWatchEvent, CellWatcher, FollowEvent};

use gdl_model::{DeploymentSpec, SegmentRegistration};
use velo_hub::{Committed, Extension, RegistrationView, Rejection, Txn};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use velo_hub::{
    axum::{Json, Router, routing::get},
    store::Incarnation,
    velo::{InstanceId, PeerInfo},
};

/// Admission and membership for one graph deployment.
pub struct GraphAdmission {
    spec: DeploymentSpec,
    members: Mutex<HashMap<InstanceId, (Incarnation, Member)>>,
}
#[derive(Clone, serde::Serialize)]
struct Member {
    registration: SegmentRegistration,
    peer: PeerInfo,
}

impl GraphAdmission {
    /// Construct admission for a validated graph and placement.
    pub fn new(spec: DeploymentSpec) -> Self {
        Self {
            spec,
            members: Mutex::default(),
        }
    }

    /// The segment and the peer of the member `id`, as `/members` shows
    /// them.
    pub(crate) fn member(&self, id: InstanceId) -> Option<(String, PeerInfo)> {
        let members = self.members.lock().expect("membership lock");
        let (_, member) = members.get(&id)?;
        Some((member.registration.segment.clone(), member.peer.clone()))
    }
}

impl Extension for GraphAdmission {
    const NAME: &'static str = "gdl";
    type Registration = SegmentRegistration;
    type Staged = SegmentRegistration;

    fn admit(
        &self,
        _: &RegistrationView<'_>,
        payload: Option<&SegmentRegistration>,
    ) -> Result<(), Rejection> {
        let payload = payload
            .ok_or_else(|| Rejection::bad_request("GDL segment registration is required"))?;
        self.spec
            .validate_registration(payload)
            .map_err(|e| Rejection::bad_request(e.to_string()))
    }
    async fn prepare(
        &self,
        _: &Txn<'_>,
        payload: Option<SegmentRegistration>,
    ) -> Result<SegmentRegistration, Rejection> {
        payload.ok_or_else(|| Rejection::bad_request("GDL segment registration is required"))
    }
    fn commit(&self, registration: SegmentRegistration, committed: &Committed<'_>) {
        self.members.lock().expect("membership lock").insert(
            committed.owner,
            (
                committed.incarnation,
                Member {
                    registration,
                    peer: committed.peer.clone(),
                },
            ),
        );
    }
    fn on_removed(&self, removal: velo_hub::store::Removal) {
        let mut members = self.members.lock().expect("membership lock");
        if members
            .get(&removal.id)
            .is_some_and(|(incarnation, _)| *incarnation <= removal.incarnation)
        {
            members.remove(&removal.id);
        }
    }
    fn public_routes(self: Arc<Self>) -> Router {
        Router::new().route(
            "/members",
            get(move || {
                let this = Arc::clone(&self);
                async move {
                    Json(
                        this.members
                            .lock()
                            .expect("membership lock")
                            .values()
                            .map(|(_, member)| member.clone())
                            .collect::<Vec<_>>(),
                    )
                }
            }),
        )
    }
    fn descriptor(&self) -> serde_json::Value {
        serde_json::to_value(&self.spec).expect("deployment is serializable")
    }
}

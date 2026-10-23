//! Discovery hub with admission restricted to one GDL deployment.
mod quorum;

pub use quorum::Quorum;

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

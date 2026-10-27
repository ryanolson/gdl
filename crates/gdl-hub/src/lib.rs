//! Discovery hub with admission restricted to one GDL deployment.
//!
//! [`GraphAdmission`] admits only the segments of one deployment's graph
//! and placement. A cell's hub runs [`CellReadiness`] beside it: the
//! `gdl-cell` extension, which judges from the cell's usable instances and
//! its quorum ([`Quorum`]) whether the cell can take work, answers on the
//! cell health endpoint (`GET /v1/ext/gdl-cell/health`), publishes the
//! cell's record through a [`gdl_cells::CellDirectory`], and serves the
//! cell's watch (`GET /v1/ext/gdl-cell/watch`): the cell's state and epoch
//! and its entry instances, as [`gdl_watch::CellWatchEvent`]s, which a hub
//! that follows the cell reads with a [`gdl_watch::CellWatcher`]. Its entry
//! instances report their admission to it
//! (`POST /v1/ext/gdl-cell/instances/{id}/load` on the control listener,
//! a [`gdl_watch::LoadReport`]), from which it makes the cell's load
//! summary, which its record and its watch carry. It drains the cell when
//! asked (`POST /v1/ext/gdl-cell/drain` on the control listener): the cell
//! is then `draining` whatever its quorum. The hub of a cell whose
//! class sends to other classes is such a follower: it finds its children,
//! the cells of those classes whose record names its cell as their parent,
//! in the directory, follows each over its watch, and relays what it
//! follows to its own instances (`GET /v1/ext/gdl-cell/follows`, as
//! [`gdl_watch::FollowEvent`]s, which a [`gdl_watch::CellWatcher`] reads
//! too), and a watch of one follow of it alone, ordered as the relay is
//! (`GET /v1/ext/gdl-cell/follows/{class}/{cell}`). Each of the three
//! watch routes serves at most its cap of watches at once
//! ([`gdl_model::WatchCaps`], from `[cell.watch_caps]`), and refuses one beyond it with 503 before it builds
//! anything for it. A followed cell whose
//! watch sends an event of a kind the hub does not know is withheld: the
//! relay shows it `unavailable`, its instances hidden, and not offered,
//! until the hub has read it again from a new watch of it, which replaces
//! the first. A follow whose cell takes no work, or whose link is lost, is
//! not offered to its choosers either, and closes once each has
//! acknowledged that it holds no offer to it
//! (`POST /v1/ext/gdl-cell/instances/{id}/unoffered` on the control
//! listener, a [`gdl_watch::Unoffered`]), but for a chooser that has had no
//! watch of the relay open for `[cell] lost_chooser_ms`, which a chooser's
//! watch names it in (`?instance=<id>`, [`gdl_watch::RelayWatcher`]); a
//! withheld follow does not close while its link reads it again. The watches' events and their client are
//! `gdl-watch`'s, so that a segment instance reads the relay without
//! linking the hub.
mod cell;
mod fanout;
mod follow;
mod quorum;
mod report;
mod setup;

pub use cell::{
    CellError, CellHealth, CellReadiness, CellTiming, CellWatch, RefusedWatches, RequiredHealth,
    SegmentHealth,
};
pub use follow::FollowsWatch;
pub use quorum::Quorum;
pub use report::{HubDirectory, VerifiedReport};
pub use setup::{cell_from_env, cell_from_vars, hub_builder};

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

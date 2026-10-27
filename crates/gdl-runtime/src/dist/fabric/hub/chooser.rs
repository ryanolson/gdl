//! What a chooser tells its own hub, through velo-hub's extension client
//! with the instance's own credential: that it runs a follower of the
//! hub's relay, so that the hub waits for it before it closes a follow,
//! that it holds no offer to a followed cell the hub marked not offered,
//! and that it avoids a followed cell's retired entry instance. Each is
//! sent from the chooser's follower, never from a request's path.

use std::time::Duration;

use gdl_watch::{Avoids, Unoffered};
use velo::InstanceId;
use velo_hub::client::{ClientError, ExtClient};

use super::HubSession;

/// How long the hub may take to answer before the follower counts what it
/// sent as not taken and sends it again later.
const ANSWER: Duration = Duration::from_secs(5);

/// Tells one chooser instance's hub what its follower does. Cheap to
/// clone.
#[derive(Debug, Clone)]
pub(crate) struct ChooserHub {
    ext: ExtClient,
    instance: InstanceId,
}

impl HubSession {
    /// What tells the hub what the follower of `instance`, the instance
    /// this session registered, does.
    pub(crate) fn chooser_hub(&self, instance: InstanceId) -> ChooserHub {
        ChooserHub {
            ext: self.client.ext(gdl_watch::EXTENSION),
            instance,
        }
    }
}

impl ChooserHub {
    /// The instance this tells the hub about.
    pub(crate) fn instance(&self) -> InstanceId {
        self.instance
    }

    /// Say that this instance runs a follower of the hub's relay: whether
    /// the hub took it, or why not.
    pub(crate) async fn declare(&self) -> Result<(), String> {
        self.post(&gdl_watch::chooser_path(self.instance), &())
            .await
    }

    /// Tell the hub `unoffered`: whether it took it, or why not.
    pub(crate) async fn acknowledge(&self, unoffered: &Unoffered) -> Result<(), String> {
        self.post(&gdl_watch::unoffered_path(self.instance), unoffered)
            .await
    }

    /// Tell the hub `avoids`: whether it took it, or why not.
    pub(crate) async fn avoid(&self, avoids: &Avoids) -> Result<(), String> {
        self.post(&gdl_watch::avoids_path(self.instance), avoids)
            .await
    }

    async fn post(&self, path: &str, body: &impl serde::Serialize) -> Result<(), String> {
        let sent = self.ext.control_post::<_, ()>(path, body);
        match tokio::time::timeout(ANSWER, sent).await {
            Ok(answer) => answer.map_err(|error: ClientError| error.to_string()),
            Err(_) => Err("the hub did not answer in time".to_string()),
        }
    }
}

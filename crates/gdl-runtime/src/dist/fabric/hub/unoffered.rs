//! A chooser's acknowledgements to its own hub that it holds no offer to
//! a followed cell its hub marked not offered, through velo-hub's
//! extension client with the instance's own credential. They are sent
//! from the chooser's follower, never from a request's path.

use std::time::Duration;

use gdl_watch::Unoffered;
use velo::InstanceId;
use velo_hub::client::{ClientError, ExtClient};

use super::HubSession;

/// How long the hub may take to answer an acknowledgement before the
/// follower counts it as not taken and sends it again later.
const ANSWER: Duration = Duration::from_secs(5);

/// Sends one instance's acknowledgements to its hub. Cheap to clone.
#[derive(Debug, Clone)]
pub(crate) struct Acknowledger {
    ext: ExtClient,
    path: String,
}

impl HubSession {
    /// What sends the acknowledgements of `instance`, the instance this
    /// session registered.
    pub(crate) fn acknowledger(&self, instance: InstanceId) -> Acknowledger {
        Acknowledger {
            ext: self.client.ext(gdl_watch::EXTENSION),
            path: gdl_watch::unoffered_path(instance),
        }
    }
}

impl Acknowledger {
    /// Tell the hub `unoffered`: whether it took it, or why not.
    pub(crate) async fn acknowledge(&self, unoffered: &Unoffered) -> Result<(), String> {
        let sent = self.ext.control_post::<_, ()>(&self.path, unoffered);
        match tokio::time::timeout(ANSWER, sent).await {
            Ok(answer) => answer.map_err(|error: ClientError| error.to_string()),
            Err(_) => Err("the hub did not answer in time".to_string()),
        }
    }
}

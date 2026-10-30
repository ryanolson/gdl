//! Whether each pool the follower follows has a link up, for the hub's
//! metrics: read under the relay's lock once per scrape, never on a
//! watch's path.

use super::Relay;

/// One pool the follower follows, and whether its link is up.
pub(crate) struct Link {
    pub(crate) class: String,
    pub(crate) pool: String,
    /// The pool's region, from its follow or its record; empty for a pool
    /// the hub has neither read nor followed.
    pub(crate) region: String,
    /// The pool's current follow has installed its watch's snapshot, and
    /// its link has not ended. A withheld pool's link is up: it reads the
    /// pool again.
    pub(crate) up: bool,
}

impl Relay {
    /// Each pool the follower has decided to follow, in order of class
    /// and pool, and whether its link is up. A link that ended leaves its
    /// pool down until a later follow of it is installed.
    pub(crate) fn links(&self) -> Vec<Link> {
        let following = self.following.borrow().clone();
        let shared = self.lock();
        following
            .into_keys()
            .map(|(class, pool)| {
                let installed = shared
                    .follow(&class, &pool)
                    .and_then(|follow| follow.installed.as_ref());
                let region = installed
                    .map(|installed| installed.region())
                    .or_else(|| shared.listings.region(&class, &pool))
                    .unwrap_or_default()
                    .to_string();
                Link {
                    up: installed.is_some(),
                    class,
                    pool,
                    region,
                }
            })
            .collect()
    }
}

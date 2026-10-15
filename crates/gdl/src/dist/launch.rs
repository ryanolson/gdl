//! How the instances of a deployment find each other before they deploy,
//! and wait until each is serving.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use velo::{PeerInfo, Velo};

use super::fabric::peer;
use super::runtime::Roster;
use crate::error::{Error, ErrorKind, Result};

impl Roster {
    /// Meet the other segments through a shared directory: publish `node`
    /// as an instance of `segment` in `dir`, then wait until every segment
    /// in `expect` has published at least one instance, and return every
    /// instance found.
    ///
    /// An instance id is fresh on every start, so peers cannot be listed
    /// ahead of time; a directory is the smallest exchange that needs no
    /// service. Files are written whole and renamed into place, so a reader
    /// never sees a partial one. Instances that publish after this returns
    /// are not in the roster, and nothing routes to them.
    ///
    /// The directory belongs to one deployment. Files are never removed, so
    /// a directory reused across runs names instances that are gone, and
    /// requests routed to them fail.
    pub async fn rendezvous<'a>(
        dir: &Path,
        segment: &str,
        node: &Velo,
        expect: impl IntoIterator<Item = &'a str>,
        wait: Duration,
    ) -> Result<Roster> {
        let expect: Vec<&str> = expect.into_iter().collect();
        if segment.contains('.') || expect.iter().any(|s| s.contains('.')) {
            return Err(Error::new(
                ErrorKind::Placement,
                "a segment name used for rendezvous has a `.`",
            ));
        }
        publish(dir, segment, &peer(node))?;
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let roster = read_roster(dir)?;
            let missing: Vec<&str> = expect.iter().copied().filter(|s| !roster.has(s)).collect();
            if missing.is_empty() {
                return Ok(roster);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorKind::Transport,
                    format!("no instance of {missing:?} appeared in {}", dir.display()),
                ));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Publish `node` as serving in `dir`, then wait until every instance in
    /// this roster has. Call it after the segment has started: a request
    /// routed to an instance whose handlers do not exist yet fails, so no
    /// instance may send before every instance it can route to is serving.
    pub async fn ready(
        &self,
        dir: &Path,
        segment: &str,
        node: &Velo,
        wait: Duration,
    ) -> Result<()> {
        publish(dir, segment, &peer(node))?;
        let want: BTreeSet<String> = self
            .peers()
            .map(|peer| peer.instance_id().to_string())
            .collect();
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let serving: BTreeSet<String> = read_roster(dir)?
                .peers()
                .map(|peer| peer.instance_id().to_string())
                .collect();
            let missing: Vec<&String> = want.difference(&serving).collect();
            if missing.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorKind::Transport,
                    format!(
                        "instances {missing:?} did not become ready in {}",
                        dir.display()
                    ),
                ));
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

const POLL: Duration = Duration::from_millis(50);

fn publish(dir: &Path, segment: &str, me: &PeerInfo) -> Result<()> {
    std::fs::create_dir_all(dir).map_err(io)?;
    let name = format!("{segment}.{}", me.instance_id());
    let bytes = rmp_serde::to_vec(me).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))?;
    let partial = dir.join(format!(".{name}.partial"));
    std::fs::write(&partial, bytes).map_err(io)?;
    std::fs::rename(&partial, dir.join(&name)).map_err(io)
}

fn read_roster(dir: &Path) -> Result<Roster> {
    let mut roster = Roster::new();
    for entry in std::fs::read_dir(dir).map_err(io)? {
        let entry = entry.map_err(io)?;
        if !entry.file_type().map_err(io)?.is_file() {
            continue;
        }
        let file = entry.file_name();
        let Some(file) = file.to_str() else { continue };
        if file.starts_with('.') {
            continue;
        }
        let Some((segment, _instance)) = file.split_once('.') else {
            continue;
        };
        let bytes = std::fs::read(entry.path()).map_err(io)?;
        let peer: PeerInfo = rmp_serde::from_slice(&bytes).map_err(|e| {
            Error::new(
                ErrorKind::Codec,
                format!("roster entry {file} is not a peer: {e}"),
            )
        })?;
        roster.add(segment, peer);
    }
    Ok(roster)
}

fn io(error: std::io::Error) -> Error {
    Error::new(ErrorKind::Transport, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dist::fabric::bind_node;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("gdl-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    async fn node() -> std::sync::Arc<Velo> {
        bind_node(([127, 0, 0, 1], 0).into()).await.expect("node")
    }

    fn ids(peers: &[PeerInfo]) -> Vec<String> {
        peers.iter().map(|p| p.instance_id().to_string()).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segments_meet_through_a_directory() {
        let dir = scratch("rendezvous");
        let (a, b) = (node().await, node().await);
        let wait = Duration::from_secs(5);
        let (ra, rb) = tokio::join!(
            Roster::rendezvous(&dir, "front", &a, ["front", "engine"], wait),
            Roster::rendezvous(&dir, "engine", &b, ["front", "engine"], wait),
        );
        for roster in [ra.expect("a"), rb.expect("b")] {
            assert_eq!(
                ids(roster.instances("front")),
                [a.instance_id().to_string()]
            );
            assert_eq!(
                ids(roster.instances("engine")),
                [b.instance_id().to_string()]
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_missing_segment_ends_the_wait_with_an_error() {
        let dir = scratch("missing");
        let a = node().await;
        let wait = Duration::from_millis(200);
        let result = Roster::rendezvous(&dir, "front", &a, ["front", "engine"], wait).await;
        let Err(error) = result else {
            panic!("engine never appears, yet the rendezvous succeeded");
        };
        assert!(error.to_string().contains("engine"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn readiness_waits_for_every_routable_instance() {
        let dir = scratch("ready");
        let ready = dir.join("ready");
        let (front, first, second) = (node().await, node().await, node().await);
        let wait = Duration::from_secs(5);
        let expect = ["front", "engine"];
        let (roster, _, _) = tokio::join!(
            Roster::rendezvous(&dir, "front", &front, expect, wait),
            Roster::rendezvous(&dir, "engine", &first, expect, wait),
            Roster::rendezvous(&dir, "engine", &second, expect, wait),
        );
        let roster = roster.expect("front");
        assert_eq!(roster.instances("engine").len(), 2);
        // One engine instance is serving; the other is still starting.
        roster
            .ready(&ready, "engine", &first, Duration::from_millis(10))
            .await
            .expect_err("the second engine and the front are not ready yet");
        let short = Duration::from_millis(300);
        let Err(error) = roster.ready(&ready, "front", &front, short).await else {
            panic!("ready while an engine instance is still starting");
        };
        assert!(
            error
                .to_string()
                .contains(&second.instance_id().to_string()),
            "{error}"
        );
        roster
            .ready(&ready, "engine", &second, wait)
            .await
            .expect("every instance is serving");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

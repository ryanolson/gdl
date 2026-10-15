//! What a segment binary needs before it can deploy: a velo node listening
//! on an address, and a roster of every segment's instances.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use velo::{PeerInfo, Velo};

use super::runtime::Roster;
use crate::error::{Error, ErrorKind, Result};

/// A velo node whose TCP transport and stream listener bind `listen`. Port
/// zero picks a free port; the node's [`PeerInfo`] carries the bound one.
pub async fn bind_node(listen: SocketAddr) -> Result<Arc<Velo>> {
    let listener = std::net::TcpListener::bind(listen).map_err(transport)?;
    let tcp = velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(listener)
        .map_err(transport)?
        .build()
        .map_err(transport)?;
    Velo::builder()
        .add_transport(Arc::new(tcp))
        .stream_bind_addr(listen.ip())
        .build()
        .await
        .map_err(transport)
}

fn transport(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Transport, error.to_string())
}

impl Roster {
    /// Meet the other segments through a shared directory: publish this
    /// instance as `<segment>.<instance>` in `dir`, then wait until every
    /// segment in `expect` has published at least one instance, and return
    /// every instance found.
    ///
    /// An instance id is fresh on every start, so peers cannot be listed
    /// ahead of time; a directory is the smallest exchange that needs no
    /// service. Files are written whole and renamed into place, so a reader
    /// never sees a partial one. Instances that start after this returns are
    /// not in the roster.
    pub async fn rendezvous<'a>(
        dir: &Path,
        segment: &str,
        me: &PeerInfo,
        expect: impl IntoIterator<Item = &'a str>,
        wait: Duration,
    ) -> Result<Roster> {
        let expect: Vec<&str> = expect.into_iter().collect();
        if segment.contains('.') || expect.iter().any(|s| s.contains('.')) {
            return Err(placement("a segment name used for rendezvous has a `.`"));
        }
        std::fs::create_dir_all(dir).map_err(io)?;
        let name = format!("{segment}.{}", me.instance_id());
        let bytes =
            rmp_serde::to_vec(me).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))?;
        let partial = dir.join(format!(".{name}.partial"));
        std::fs::write(&partial, bytes).map_err(io)?;
        std::fs::rename(&partial, dir.join(&name)).map_err(io)?;

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
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn read_roster(dir: &Path) -> Result<Roster> {
    let mut roster = Roster::new();
    for entry in std::fs::read_dir(dir).map_err(io)? {
        let entry = entry.map_err(io)?;
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

fn placement(why: &str) -> Error {
    Error::new(ErrorKind::Placement, why)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("gdl-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segments_meet_through_a_directory() {
        let dir = scratch("rendezvous");
        let a = bind_node("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("a");
        let b = bind_node("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("b");
        let wait = Duration::from_secs(5);
        let (pa, pb) = (a.peer_info(), b.peer_info());
        let (ra, rb) = tokio::join!(
            Roster::rendezvous(&dir, "front", &pa, ["front", "engine"], wait),
            Roster::rendezvous(&dir, "engine", &pb, ["front", "engine"], wait),
        );
        for roster in [ra.expect("a"), rb.expect("b")] {
            // Compared with the published values: velo merges a node's
            // addresses afresh on every `peer_info()` call, and the merge's
            // byte order is not stable between calls.
            assert_eq!(roster.instances("front"), std::slice::from_ref(&pa));
            assert_eq!(roster.instances("engine"), std::slice::from_ref(&pb));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_missing_segment_ends_the_wait_with_an_error() {
        let dir = scratch("missing");
        let a = bind_node("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("a");
        let result = Roster::rendezvous(
            &dir,
            "front",
            &a.peer_info(),
            ["front", "engine"],
            Duration::from_millis(200),
        )
        .await;
        let Err(error) = result else {
            panic!("engine never appears, yet the rendezvous succeeded");
        };
        assert!(error.to_string().contains("engine"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

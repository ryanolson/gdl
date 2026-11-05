//! `gdl-hub --resolve <class>/<pool> --write <file>`: where a running
//! pool's hub is, read from the pool directory when a joining job starts,
//! and written as the `[hub]` table a segment takes as an override file.
//! Read-only: it watches the directory and never writes to it.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use figment::value::Value;
use futures::StreamExt;
use gdl_model::DirectoryBackend;
use gdl_pools::{DirDirectory, PoolDirectory, PoolEvent, PoolRecord, PoolState, PoolView};

use crate::PoolError;
use crate::setup::variables;

/// The `[hub]` table of a pool's hub, as its record names the hub's public
/// listener. The record has no control port, so the table names none and
/// a segment keeps its default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHub {
    host: String,
    public_port: u16,
}

impl ResolvedHub {
    /// The hub's host: a name, an IPv4 address, or an IPv6 address without
    /// brackets, as a segment's `[hub] host` takes it.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port of the hub's public listener.
    pub fn public_port(&self) -> u16 {
        self.public_port
    }

    /// The settings file: `[hub]` with `host` and `public_port`, and
    /// nothing else, so that it overrides only where the hub is.
    pub fn settings(&self) -> String {
        // A checked host has only letters, digits, `-`, `.`, and `:`, none
        // of which a TOML string escapes.
        format!(
            "[hub]\nhost = \"{}\"\npublic_port = {}\n",
            self.host, self.public_port
        )
    }

    /// Write [`ResolvedHub::settings`] to `path` atomically: to a file
    /// beside it, then renamed over it, so that a reader that waits for
    /// the file, such as a launcher's probe, never reads a part of it.
    ///
    /// # Errors
    ///
    /// The file could not be written or renamed; nothing is left beside
    /// `path`.
    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        let dir = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        let mut file = tempfile::Builder::new()
            .prefix(".gdl-hub-")
            .tempfile_in(dir)?;
        file.write_all(self.settings().as_bytes())?;
        // The temporary file is made readable by its owner alone; a segment
        // in a container may run as another user, and the file holds no
        // secret.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o644))?;
        }
        file.as_file().sync_all()?;
        file.persist(path).map_err(|error| error.error)?;
        Ok(())
    }

    fn of(record: &PoolRecord) -> Result<Self, PoolError> {
        gdl_model::check_hub_address(&record.hub).map_err(|why| {
            PoolError::new(format!(
                "pool `{}` of class `{}`: its record's hub {why}",
                record.name, record.class
            ))
        })?;
        let (host, port) = record.hub.rsplit_once(':').expect("a checked address");
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        Ok(Self {
            host: host.to_owned(),
            public_port: port.parse().expect("a checked port"),
        })
    }
}

/// The hub of pool `pool` of `class`, once `directory` shows that it is
/// live: its record must change, by its hub's heartbeat, within the
/// record's lease from when this reader first saw it. A record carries no
/// time, since clocks across hosts differ, so a reader that has seen it
/// once cannot tell a live hub's record from one a dead hub left; each
/// reader measures the lease on its own clock, as every follower does
/// ([`PoolView`]). So this waits up to one heartbeat for a live hub, and
/// one lease for a dead one.
///
/// A pool of any other state is joined: a joiner is what makes an
/// `unavailable` pool ready. The answer is the pool's state as of the last
/// event read: no reader sees a drain its directory has not delivered,
/// and a joiner of a pool that has just begun to drain stops with it.
///
/// # Errors
///
/// The pool is unknown, withdrawn, draining, or scaled to 0, or its lease
/// passed without a heartbeat; its record names a hub that is not
/// `host:port`; or the directory's watch failed. Each is one line.
pub async fn resolve_hub<D: PoolDirectory>(
    directory: &D,
    class: &str,
    pool: &str,
) -> Result<ResolvedHub, PoolError> {
    let named = |what: String| PoolError::new(format!("pool `{pool}` of class `{class}`: {what}"));
    let mut view = PoolView::new();
    // The version this reader first saw, when it saw it, and the lease
    // that runs from then.
    let mut first: Option<(gdl_pools::Version, Instant, Duration)> = None;
    let mut synced = false;
    let mut watch = Box::pin(directory.watch(class, None));
    loop {
        let next = match first {
            Some((_, seen, lease)) => {
                let deadline = seen + lease;
                let next = tokio::time::timeout_at(deadline.into(), watch.next()).await;
                // `timeout_at` polls the watch before its timer, so an event
                // already queued when the resolver runs again after the
                // deadline would win: the clock decides.
                match next {
                    Ok(next) if Instant::now() < deadline => next,
                    _ => {
                        return Err(named(format!(
                            "no heartbeat within its lease, {} ms: its hub is gone",
                            lease.as_millis()
                        )));
                    }
                }
            }
            None => watch.next().await,
        };
        let event = match next {
            Some(Ok(event)) => event,
            Some(Err(error)) => return Err(named(format!("the directory: {error}"))),
            None => return Err(named("the directory's watch ended".into())),
        };
        let resnapshot = matches!(event, PoolEvent::Resnapshot);
        synced |= matches!(event, PoolEvent::Synced { .. });
        view.apply(event, Instant::now());
        if resnapshot {
            watch = Box::pin(directory.watch(class, None));
            continue;
        }
        // In the middle of the first snapshot, a pool not yet seen may
        // still come.
        if !synced {
            continue;
        }
        let Some((record, changed)) = view.seen(pool) else {
            return Err(named(if view.withdrawn(pool).is_some() {
                "withdrawn: no hub to join".into()
            } else {
                "not in the directory".into()
            }));
        };
        if record.state == PoolState::Draining {
            return Err(named("draining: it takes no new instance".into()));
        }
        if !record.leased() {
            return Err(named("scaled to 0: no hub to join".into()));
        }
        match first {
            None => first = Some((record.version(), changed, record.lease)),
            Some((version, ..)) if record.version() > version => return ResolvedHub::of(record),
            Some(_) => {}
        }
    }
}

/// The binary's `--resolve` mode, from `args`, the arguments after
/// `--resolve` (`<class>/<pool> --write <file>`), and the directory the
/// `GDL_POOL__*` variables of `vars` name, as a pool's hub reads them:
/// `dir` with `GDL_POOL__DIR`, or `dynamodb` with its tables, region, and
/// endpoint. Any other `GDL_POOL__` variable is ignored, so a joining job
/// can share its pool's variables. Writes the pool's `[hub]` to the file
/// ([`ResolvedHub::write`]) and returns a line that says so.
///
/// # Errors
///
/// Usage, a backend this cannot read (`relay`'s records are in a parent's
/// hub, and `memory` is one process's), or what [`resolve_hub`] or the
/// write refuses, as one line. Nothing is written then.
pub async fn resolve_from_args(
    args: impl IntoIterator<Item = OsString>,
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<String, PoolError> {
    const USAGE: &str = "usage: gdl-hub --resolve <class>/<pool> --write <file>";
    let args: Vec<OsString> = args.into_iter().collect();
    let (target, file) = match args.as_slice() {
        [target, flag, file] if flag == "--write" => (target, PathBuf::from(file)),
        _ => return Err(PoolError::new(USAGE)),
    };
    let target = target.to_string_lossy();
    let Some((class, pool)) = target
        .split_once('/')
        .filter(|(class, pool)| !class.is_empty() && !pool.is_empty() && !pool.contains('/'))
    else {
        return Err(PoolError::new(format!(
            "{target:?} is not <class>/<pool>; {USAGE}"
        )));
    };
    // Checked as a record's names are: the `dir` backend would read a
    // class such as `..` as a part of a path.
    gdl_model::check_class_name(class)
        .and_then(|()| gdl_model::check_pool_name(pool))
        .map_err(PoolError::new)?;
    let hub = match directory_from_vars(vars)? {
        DirectoryBackend::Dir { path } => {
            resolve_hub(&DirDirectory::new(path), class, pool).await?
        }
        #[cfg(feature = "dynamodb")]
        DirectoryBackend::DynamoDb {
            records_table,
            lists_table,
            aws_region,
            endpoint,
        } => {
            let directory = crate::dynamodb::directory(
                &records_table,
                &lists_table,
                &aws_region,
                endpoint.as_deref(),
            )
            .await;
            resolve_hub(&directory, class, pool).await?
        }
        #[cfg(not(feature = "dynamodb"))]
        DirectoryBackend::DynamoDb { .. } => {
            return Err(PoolError::new(
                "GDL_POOL__DIRECTORY=dynamodb: this gdl-hub is built without its `dynamodb` feature, which links the AWS SDK; install it with `cargo install gdl-hub --features dynamodb`",
            ));
        }
        DirectoryBackend::Relay { .. } | DirectoryBackend::Memory => {
            unreachable!("directory_from_vars gives `dir` or `dynamodb`")
        }
    };
    hub.write(&file)
        .map_err(|error| PoolError::new(format!("{}: {error}", file.display())))?;
    Ok(format!(
        "gdl-hub: wrote {}: pool `{pool}` of class `{class}` has its hub at {}",
        file.display(),
        hub_address(&hub)
    ))
}

/// The directory `vars` name for `--resolve`: `dir` or `dynamodb`.
fn directory_from_vars(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<DirectoryBackend, PoolError> {
    let vars: Vec<(OsString, OsString)> = vars.into_iter().collect();
    let mut backend = None;
    let mut dir = None;
    for variable in variables(vars.iter().cloned()) {
        // Only these two keys are decoded: a joining job shares its pool's
        // variables, such as `GDL_POOL__HEARTBEAT_MS`, which are not strings.
        let slot = match variable.key.as_str() {
            "directory" => &mut backend,
            "dir" => &mut dir,
            _ => continue,
        };
        // Each value as the hub reads it, through figment, which decodes a
        // value a generator quoted.
        let Ok(value) = variable.value.parse::<Value>();
        let value = value
            .deserialize::<String>()
            .map_err(|error| PoolError::new(format!("{}: {error}", variable.name)))?;
        *slot = Some((variable.name, value));
    }
    match backend.as_ref().map(|(_, value)| value.as_str()) {
        Some("dir") => match dir {
            Some((_, path)) if !path.is_empty() => Ok(DirectoryBackend::Dir { path: path.into() }),
            _ => Err(PoolError::new(
                "GDL_POOL__DIR: missing; with GDL_POOL__DIRECTORY=dir it is the shared directory",
            )),
        },
        Some("dynamodb") => crate::directory_from_vars(vars),
        _ => {
            let (name, given) = backend.map_or_else(
                || ("GDL_POOL__DIRECTORY".to_owned(), "missing".to_owned()),
                |(name, value)| (name, format!("{value:?}")),
            );
            Err(PoolError::new(format!(
                "{name}: {given}; --resolve reads the directory a pool's hub publishes to, `dir` or `dynamodb` (a `relay` pool's record is held by its parent's hub)"
            )))
        }
    }
}

/// `hub`'s public listener, `host:port`, with an IPv6 host in brackets.
fn hub_address(hub: &ResolvedHub) -> String {
    if hub.host.contains(':') {
        format!("[{}]:{}", hub.host, hub.public_port)
    } else {
        format!("{}:{}", hub.host, hub.public_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(hub: &str) -> PoolRecord {
        PoolRecord {
            name: "model-x-01".into(),
            class: "model_x".into(),
            region: "us-east-1".into(),
            parent: None,
            contract: 1,
            placement: 1,
            state: PoolState::Ready,
            epoch: 1,
            seq: 1,
            hub: hub.into(),
            lease: Duration::from_secs(30),
            load: None,
            presence: gdl_pools::Presence::Serving,
            properties: Vec::new(),
            properties_version: 0,
            revoked: Vec::new(),
            dropped: Vec::new(),
        }
    }

    #[test]
    fn a_records_hub_becomes_the_hub_table() {
        for (address, host, port) in [
            ("10.0.0.7:1337", "10.0.0.7", 1337),
            ("model-x-01-hub.ns:8080", "model-x-01-hub.ns", 8080),
            ("[fd00::7]:1337", "fd00::7", 1337),
        ] {
            let hub = ResolvedHub::of(&record(address)).expect(address);
            assert_eq!((hub.host(), hub.public_port()), (host, port));
            assert_eq!(hub_address(&hub), address);
            assert_eq!(
                hub.settings(),
                format!("[hub]\nhost = \"{host}\"\npublic_port = {port}\n")
            );
        }
        for address in ["", "hub", "hub:0", "hub:port", "fd00::7:1337", "a b:1"] {
            let error = ResolvedHub::of(&record(address)).expect_err(address);
            assert!(error.to_string().contains("model-x-01"), "{error}");
        }
    }

    #[test]
    fn the_directory_is_read_as_a_hub_reads_it() {
        let vars = |pairs: &[(&str, &str)]| -> Vec<(OsString, OsString)> {
            pairs
                .iter()
                .map(|(name, value)| ((*name).into(), (*value).into()))
                .collect()
        };
        let dir = directory_from_vars(vars(&[
            ("GDL_POOL__NAME", "ignored"),
            ("gdl_pool__directory", "dir"),
            ("GDL_POOL__DIR", "/shared/pools"),
        ]))
        .expect("dir");
        assert_eq!(
            dir,
            DirectoryBackend::Dir {
                path: "/shared/pools".into()
            }
        );
        let dynamodb = directory_from_vars(vars(&[
            ("GDL_POOL__DIRECTORY", "dynamodb"),
            ("GDL_POOL__RECORDS_TABLE", "gdl-pools"),
            ("GDL_POOL__LISTS_TABLE", "gdl-pool-lists"),
            ("GDL_POOL__AWS_REGION", "us-east-1"),
        ]))
        .expect("dynamodb");
        assert!(matches!(dynamodb, DirectoryBackend::DynamoDb { .. }));
        // A joining job shares its pool's variables, whose values are not
        // all strings: they are not read.
        for backend in [
            vec![
                ("GDL_POOL__DIRECTORY", "dir"),
                ("GDL_POOL__DIR", "/shared/pools"),
            ],
            vec![
                ("GDL_POOL__DIRECTORY", "dynamodb"),
                ("GDL_POOL__RECORDS_TABLE", "gdl-pools"),
                ("GDL_POOL__LISTS_TABLE", "gdl-pool-lists"),
                ("GDL_POOL__AWS_REGION", "us-east-1"),
            ],
        ] {
            let mut shared = backend.clone();
            shared.extend([
                ("GDL_POOL__HEARTBEAT_MS", "200"),
                ("GDL_POOL__LEASE_MS", "1000"),
                ("GDL_POOL__READY__ONLY", "1"),
            ]);
            assert_eq!(
                directory_from_vars(vars(&shared)).expect("shared variables"),
                directory_from_vars(vars(&backend)).expect("the backend alone"),
            );
        }
        for (given, expected) in [
            (vars(&[]), "GDL_POOL__DIRECTORY: missing"),
            (
                vars(&[("GDL_POOL__DIRECTORY", "relay")]),
                "GDL_POOL__DIRECTORY: \"relay\"",
            ),
            (
                vars(&[("GDL_POOL__DIRECTORY", "memory")]),
                "GDL_POOL__DIRECTORY: \"memory\"",
            ),
            (
                vars(&[("GDL_POOL__DIRECTORY", "dir")]),
                "GDL_POOL__DIR: missing",
            ),
            (
                vars(&[("GDL_POOL__DIRECTORY", "dynamodb")]),
                "GDL_POOL__RECORDS_TABLE: missing",
            ),
        ] {
            let error = directory_from_vars(given).expect_err(expected);
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
}

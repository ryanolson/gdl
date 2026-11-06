//! The etcd that a fleet's deployment runs for its `[directory]
//! backend = "etcd"`: one member, with the compaction and quota the
//! backend needs (`docs/src/concepts/pools.md`, "Running etcd for the
//! directory"). The backend never compacts, so without them etcd keeps
//! every heartbeat's revision until its quota stops every write.

/// etcd v3.5.21, the version srt-slurm installs and `scripts/etcd.sh`
/// tests, pinned by the digest of its multi-architecture index.
pub(super) const IMAGE: &str = "gcr.io/etcd-development/etcd@sha256:fd158fbe55240e252947bbd2e8dddc217997ff43978071fac2bd202b6ad15c03";

/// The binary in [`IMAGE`].
pub(super) const BINARY: &str = "/usr/local/bin/etcd";

/// The client port.
pub(super) const PORT: u16 = 2379;

/// The name of the etcd service, and of its objects: a pool may not take
/// it.
pub(super) const NAME: &str = "etcd";

/// Where the data volume is mounted; the data directory is below it, which
/// etcd makes with the mode it requires.
pub(super) const MOUNT: &str = "/var/lib/etcd";

/// The flags of every member, after the binary, as the backend needs them:
/// compact the revisions older than an hour, and an 8 GiB quota.
pub(super) const FLAGS: [&str; 3] = [
    "--auto-compaction-mode=periodic",
    "--auto-compaction-retention=1h",
    "--quota-backend-bytes=8589934592",
];

/// The arguments of the one member, which listens on every address and
/// gives clients `advertise`, `http://host:port`, as its address.
pub(super) fn args(advertise: &str) -> Vec<String> {
    let mut args: Vec<String> = [
        "--name",
        NAME,
        "--data-dir",
        &format!("{MOUNT}/data"),
        "--listen-client-urls",
        &format!("http://0.0.0.0:{PORT}"),
        "--advertise-client-urls",
        advertise,
    ]
    .map(str::to_string)
    .into();
    args.extend(FLAGS.map(str::to_string));
    args
}

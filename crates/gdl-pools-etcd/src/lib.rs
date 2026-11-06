//! A pool directory in etcd: [`EtcdDirectory`], a
//! [`gdl_pools::PoolDirectory`] whose records live in an etcd cluster that
//! every hub of a site reaches, for sites without AWS: Slurm, on-prem, and
//! other clouds.
//!
//! A hub takes it through `gdl_hub::hub_builder_with`, or, built with
//! `gdl-hub`'s `etcd` feature, from its settings (`[pool] directory =
//! "etcd"`). The directory is behind the crate's `client` feature, off by
//! default: it links the etcd client, whose build compiles etcd's protos,
//! so a build with it needs `protoc`. Without it, the crate holds only the
//! keys' encoding, so a build of the workspace needs no `protoc`. It links
//! no TLS.
//!
//! The conformance suite and the backend's own cases run against a real
//! etcd in a test target outside the default suite: `scripts/etcd.sh`.

#[cfg(feature = "client")]
mod directory;
// Without the `client` feature only its tests read it.
#[cfg_attr(not(feature = "client"), allow(dead_code))]
mod value;
#[cfg(feature = "client")]
mod watch;

#[cfg(feature = "client")]
pub use directory::{EtcdDirectory, EtcdLimits, STORE_ID};
#[cfg(feature = "client")]
pub use watch::EtcdWatch;

#[cfg(feature = "etcd-local")]
pub mod testing;

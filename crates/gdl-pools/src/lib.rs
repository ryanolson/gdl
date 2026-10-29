//! The pool plane's shared types: the [`PoolRecord`] each pool's hub
//! publishes, the [`PoolDirectory`] trait every directory backend
//! implements, and the [`PoolView`] in which every reader applies a
//! watch, keeping the newest record of each pool and enforcing its lease
//! on the reader's own clock.
//!
//! Three backends are here too: [`MemoryDirectory`], one directory shared
//! by every hub in a process, for tests that run several pools in one
//! process; [`DirDirectory`], one file per pool under a directory that
//! every hub on one host shares, for Compose; and [`RelayDirectory`], the
//! records a parent pool's hub holds for the children that report to it,
//! and keeps in a state directory of its own.
//! Behind the `conformance` feature, [`conformance`] is the suite that
//! holds a backend to the trait's rules; a backend crate enables the
//! feature in its dev-dependencies and runs [`conformance::run`] from a
//! test.
//!
//! An operator's commands for a pool whose hub is gone, [`PoolCommand`],
//! write its withdrawal, or a record that says it was scaled to 0, through
//! any directory.
//!
//! A backend crate and the hub both depend on this crate, so it holds no
//! transport: no velo, no velo-hub, no HTTP. The hub's publishing (its
//! quorum and the state it derives) is `gdl-hub`'s; the follow link
//! between hubs and the pool policy that picks a pool live elsewhere.

#[cfg(feature = "conformance")]
pub mod conformance;
mod dir;
mod directory;
mod memory;
mod operator;
mod record;
mod relay;
mod view;

pub use dir::{DirDirectory, DirLimits, DirWatch};
pub use directory::{DirectoryError, PoolDirectory, PoolEvent, ResumeToken};
pub use memory::{MemoryDirectory, MemoryLimits, MemoryWatch};
pub use operator::{CommandWrite, PoolCommand, last_record};
pub use record::{
    LoadSummary, ModelEntry, ModelState, PoolCondition, PoolRecord, PoolState, Presence, Version,
    Withdrawal,
};
pub use relay::RelayDirectory;
pub use view::PoolView;

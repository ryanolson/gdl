//! The cell plane's shared types: the [`CellRecord`] each cell's hub
//! publishes, the [`CellDirectory`] trait every directory backend
//! implements, and the [`CellView`] in which every reader applies a
//! watch, keeping the newest record of each cell and enforcing its lease
//! on the reader's own clock.
//!
//! Three backends are here too: [`MemoryDirectory`], one directory shared
//! by every hub in a process, for tests that run several cells in one
//! process; [`DirDirectory`], one file per cell under a directory that
//! every hub on one host shares, for Compose; and [`RelayDirectory`], the
//! records a parent cell's hub holds for the children that report to it,
//! and keeps in a state directory of its own.
//! Behind the `conformance` feature, [`conformance`] is the suite that
//! holds a backend to the trait's rules; a backend crate enables the
//! feature in its dev-dependencies and runs [`conformance::run`] from a
//! test.
//!
//! An operator's commands for a cell whose hub is gone, [`CellCommand`],
//! write its withdrawal, or a record that says it was scaled to 0, through
//! any directory.
//!
//! A backend crate and the hub both depend on this crate, so it holds no
//! transport: no velo, no velo-hub, no HTTP. The hub's publishing (its
//! quorum and the state it derives) is `gdl-hub`'s; the follow link
//! between hubs and the cell policy that picks a cell live elsewhere.

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
pub use directory::{CellDirectory, CellEvent, DirectoryError, ResumeToken};
pub use memory::{MemoryDirectory, MemoryLimits, MemoryWatch};
pub use operator::{CellCommand, CommandWrite, last_record};
pub use record::{
    CellRecord, CellState, LoadSummary, ModelEntry, ModelState, Presence, Version, Withdrawal,
};
pub use relay::RelayDirectory;
pub use view::CellView;

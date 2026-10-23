//! The cell plane's shared types: the [`CellRecord`] each cell's hub
//! publishes, the [`CellDirectory`] trait every directory backend
//! implements, and the [`CellView`] in which every reader applies a
//! watch, keeping the newest record of each cell and enforcing its lease
//! on the reader's own clock.
//!
//! Two backends are here too: [`MemoryDirectory`], one directory shared
//! by every hub in a process, for tests that run several cells in one
//! process; and [`DirDirectory`], one file per cell under a directory
//! that every hub on one host shares, for Compose. Behind the `conformance` feature, [`conformance`] is the
//! suite every backend must pass; a backend crate enables the feature in
//! its dev-dependencies and runs [`conformance::run`] from a test.
//!
//! A backend crate and the hub both depend on this crate, so it holds no
//! transport: no velo, no velo-hub, no HTTP. The hub's publishing (its
//! quorum and the state it derives), the follow link between hubs, and
//! the cell policy that picks a cell live elsewhere.

#[cfg(feature = "conformance")]
pub mod conformance;
mod dir;
mod directory;
mod memory;
mod record;
mod view;

pub use dir::{DirDirectory, DirLimits, DirWatch};
pub use directory::{CellDirectory, CellEvent, DirectoryError, ResumeToken};
pub use memory::{MemoryDirectory, MemoryLimits, MemoryWatch};
pub use record::{CellRecord, CellState, LoadSummary, Version, Withdrawal};
pub use view::CellView;

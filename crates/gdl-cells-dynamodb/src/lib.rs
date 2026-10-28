//! A cell directory in DynamoDB: [`DynamoDirectory`], a
//! [`gdl_cells::CellDirectory`] whose records live in two tables that
//! global tables replicate to every region a fleet runs in, so that every
//! hub reads every cell's record, whatever its region.
//!
//! A hub takes it through `gdl_hub::hub_builder_with`. The crate takes a
//! [`aws_sdk_dynamodb::Client`] that its caller builds, with the caller's
//! credentials, region, and HTTPS client: it links only the DynamoDB
//! client, and no TLS stack or credential chain of its own.
//!
//! The conformance suite runs against DynamoDB Local in a test target
//! outside the default suite: see `tests/local.rs`.

mod directory;
mod item;
mod watch;

pub use directory::{DynamoDirectory, DynamoLimits, Tables};
pub use watch::DynamoWatch;

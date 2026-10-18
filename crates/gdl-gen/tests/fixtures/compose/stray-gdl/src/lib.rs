//! Another package named `gdl`: a second runtime, which declares a
//! message catalogue of the same name as the real one.

pub use linkme;

/// Its message catalogue.
#[linkme::distributed_slice]
pub static GDL_MESSAGE_CATALOGUE: [&'static str];

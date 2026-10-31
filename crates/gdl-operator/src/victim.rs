//! Victim policies: how a `GdlScaler` chooses the instance to remove.

/// The default policy: the instance with the fewest admitted requests,
/// whose drain is the shortest.
pub const FEWEST_ADMITTED: &str = "fewestAdmitted";

/// The newest pod.
pub const NEWEST: &str = "newest";

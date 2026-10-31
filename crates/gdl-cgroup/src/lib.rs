//! The figures a gdl process serves about itself: prometheus's process
//! collector, and its cgroup v2's CPU use and throttling, CPU quota, and memory
//! ([`CgroupCollector`]). Every segment and every hub registers both with
//! [`register`]. Each is read when the registry is scraped, never on the
//! request path. See `docs/src/operations/observability.md`.

mod cgroup;

pub use cgroup::CgroupCollector;

/// Register the process collector (Linux only) and the [`CgroupCollector`]
/// of the calling process in `registry`. Call it once per registry: a
/// second call is refused as a duplicate.
///
/// The names are the collectors' own (`process_*`, `process_cgroup_*`): a
/// registry made with a prefix serves them under it.
///
/// # Errors
///
/// When `registry` holds either collector's metrics already.
pub fn register(registry: &prometheus::Registry) -> prometheus::Result<()> {
    #[cfg(target_os = "linux")]
    registry.register(Box::new(
        prometheus::process_collector::ProcessCollector::for_self(),
    ))?;
    registry.register(Box::new(CgroupCollector::for_self()))
}

/// The name of every family [`register`] adds, whether or not a scrape can
/// read it. A registry served beside these must not serve one of them too.
pub fn families() -> Vec<String> {
    use prometheus::core::Collector;
    let mut names: Vec<String> = CgroupCollector::at(None)
        .desc()
        .into_iter()
        .map(|desc| desc.fq_name.clone())
        .collect();
    #[cfg(target_os = "linux")]
    names.extend(
        prometheus::process_collector::ProcessCollector::for_self()
            .desc()
            .into_iter()
            .map(|desc| desc.fq_name.clone()),
    );
    names
}

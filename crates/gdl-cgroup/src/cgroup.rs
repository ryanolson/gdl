//! The process's cgroup v2 figures, read from its own cgroup's files each
//! time the registry is scraped: CPU throttling, the CPU quota, and memory.
//!
//! A process limited to a CPU quota reads at most its quota in CPU seconds,
//! however much more it needs; the throttled time is the rest. Nothing here
//! runs on the request path.
//!
//! Every figure is optional. Under cgroup v1, off Linux, or when a file or
//! a key is missing or unreadable, its metric is left out, and the scrape
//! goes on.

use std::path::{Path, PathBuf};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

/// Where cgroup v2 is mounted.
const MOUNT: &str = "/sys/fs/cgroup";

/// The families: name, help, and type.
const PERIODS: (&str, &str) = (
    "process_cgroup_cpu_periods_total",
    "CPU quota periods that have elapsed for the process's cgroup (cpu.stat nr_periods).",
);
const THROTTLED_PERIODS: (&str, &str) = (
    "process_cgroup_cpu_throttled_periods_total",
    "CPU quota periods in which the process's cgroup was throttled (cpu.stat nr_throttled).",
);
const THROTTLED_SECONDS: (&str, &str) = (
    "process_cgroup_cpu_throttled_seconds_total",
    "Seconds the process's cgroup was throttled, waiting for its CPU quota (cpu.stat throttled_usec).",
);
const QUOTA: (&str, &str) = (
    "process_cgroup_cpu_quota_cores",
    "The process's cgroup's CPU quota in cores: quota over period in cpu.max. Absent with no quota.",
);
const PERIOD: (&str, &str) = (
    "process_cgroup_cpu_period_seconds",
    "The process's cgroup's CPU quota period in seconds (cpu.max).",
);
const MEMORY: (&str, &str) = (
    "process_cgroup_memory_bytes",
    "Memory charged to the process's cgroup (memory.current).",
);
const MEMORY_PEAK: (&str, &str) = (
    "process_cgroup_memory_peak_bytes",
    "The most memory ever charged to the process's cgroup (memory.peak).",
);

/// A [`Collector`] of the process's cgroup v2 figures: each scrape reads
/// `cpu.stat`, `cpu.max`, `memory.current`, and `memory.peak` from the
/// process's cgroup, and serves what it could read.
pub struct CgroupCollector {
    /// The cgroup's directory; `None` when the process has no cgroup v2.
    dir: Option<PathBuf>,
    descs: Vec<Desc>,
}

impl CgroupCollector {
    /// The collector of the calling process's cgroup, found through
    /// `/proc/self/cgroup`. A process without one (cgroup v1, not Linux)
    /// gets a collector that serves nothing.
    pub fn for_self() -> Self {
        let dir = std::fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|text| own(&text).map(|path| Path::new(MOUNT).join(path)));
        Self::at(dir)
    }

    /// The collector of the cgroup whose files are in `dir`.
    pub(crate) fn at(dir: Option<PathBuf>) -> Self {
        let descs = [
            PERIODS,
            THROTTLED_PERIODS,
            THROTTLED_SECONDS,
            QUOTA,
            PERIOD,
            MEMORY,
            MEMORY_PEAK,
        ]
        .into_iter()
        .map(|(name, help)| {
            Desc::new(name.into(), help.into(), Vec::new(), Default::default())
                .expect("a constant metric name is valid")
        })
        .collect();
        Self { dir, descs }
    }
}

impl Collector for CgroupCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let Some(dir) = &self.dir else {
            return Vec::new();
        };
        Reading::of(dir).families()
    }
}

/// The cgroup's figures at one scrape, each `None` when unread.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Reading {
    pub(crate) periods: Option<u64>,
    pub(crate) throttled_periods: Option<u64>,
    pub(crate) throttled_seconds: Option<f64>,
    pub(crate) quota_cores: Option<f64>,
    pub(crate) period_seconds: Option<f64>,
    pub(crate) memory_bytes: Option<u64>,
    pub(crate) memory_peak_bytes: Option<u64>,
}

impl Reading {
    /// Read the files of the cgroup in `dir`.
    pub(crate) fn of(dir: &Path) -> Self {
        let read = |file: &str| std::fs::read_to_string(dir.join(file)).ok();
        let stat = read("cpu.stat");
        let stat = |key: &str| stat.as_deref().and_then(|text| stat_value(text, key));
        let (quota_cores, period_seconds) = read("cpu.max").map_or((None, None), |text| max(&text));
        let bytes = |file: &str| read(file).and_then(|text| text.trim().parse().ok());
        Self {
            periods: stat("nr_periods"),
            throttled_periods: stat("nr_throttled"),
            throttled_seconds: stat("throttled_usec").map(|usec| usec as f64 / 1e6),
            quota_cores,
            period_seconds,
            memory_bytes: bytes("memory.current"),
            memory_peak_bytes: bytes("memory.peak"),
        }
    }

    fn families(&self) -> Vec<MetricFamily> {
        let as_f64 = |value: Option<u64>| value.map(|value| value as f64);
        [
            (PERIODS, MetricType::COUNTER, as_f64(self.periods)),
            (
                THROTTLED_PERIODS,
                MetricType::COUNTER,
                as_f64(self.throttled_periods),
            ),
            (
                THROTTLED_SECONDS,
                MetricType::COUNTER,
                self.throttled_seconds,
            ),
            (QUOTA, MetricType::GAUGE, self.quota_cores),
            (PERIOD, MetricType::GAUGE, self.period_seconds),
            (MEMORY, MetricType::GAUGE, as_f64(self.memory_bytes)),
            (
                MEMORY_PEAK,
                MetricType::GAUGE,
                as_f64(self.memory_peak_bytes),
            ),
        ]
        .into_iter()
        .filter_map(|((name, help), kind, value)| Some(family(name, help, kind, value?)))
        .collect()
    }
}

/// The process's cgroup v2 path in `/proc/self/cgroup`, `text`: the `0::`
/// line's, relative to the mount.
pub(crate) fn own(text: &str) -> Option<&str> {
    text.lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(|path| path.trim_start_matches('/'))
}

/// The value of `key` in `cpu.stat`, `text`.
fn stat_value(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then(|| value.trim().parse().ok())?
    })
}

/// The quota in cores and the period in seconds, from `cpu.max`, `text`:
/// `<quota> <period>` in microseconds, `max` for no quota.
fn max(text: &str) -> (Option<f64>, Option<f64>) {
    let mut fields = text.split_whitespace();
    let quota = fields.next();
    let Some(period) = fields
        .next()
        .and_then(|period| period.parse::<u64>().ok())
        .filter(|period| *period > 0)
    else {
        return (None, None);
    };
    let quota = quota
        .and_then(|quota| quota.parse::<u64>().ok())
        .map(|quota| quota as f64 / period as f64);
    (quota, Some(period as f64 / 1e6))
}

fn family(name: &str, help: &str, kind: MetricType, value: f64) -> MetricFamily {
    let mut metric = proto::Metric::default();
    if kind == MetricType::COUNTER {
        let mut counter = proto::Counter::default();
        counter.set_value(value);
        metric.set_counter(counter);
    } else {
        let mut gauge = proto::Gauge::default();
        gauge.set_value(value);
        metric.set_gauge(gauge);
    }
    let mut family = MetricFamily::default();
    family.set_name(name.to_string());
    family.set_help(help.to_string());
    family.set_field_type(kind);
    family.set_metric(vec![metric]);
    family
}

#[cfg(test)]
mod tests;

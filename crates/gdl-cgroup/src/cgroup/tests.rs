use std::path::Path;

use prometheus::core::Collector;

use super::{CgroupCollector, Reading, own};

/// A cgroup directory holding `files`, each a name and its contents.
fn cgroup(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("gdl-cgroup-")
        .tempdir()
        .expect("temp dir");
    for (name, text) in files {
        std::fs::write(dir.path().join(name), text).expect("write fixture");
    }
    dir
}

/// A cgroup v2 `cpu.stat` with the cpu controller on: the throttling keys
/// follow the base ones.
const STAT: &str = "usage_usec 9000000\nuser_usec 6000000\nsystem_usec 3000000\nnr_periods 1200\nnr_throttled 300\nthrottled_usec 4500000\nnr_bursts 0\nburst_usec 0\n";

#[test]
fn reads_every_figure() {
    let dir = cgroup(&[
        ("cpu.stat", STAT),
        ("cpu.max", "100000 100000\n"),
        ("memory.current", "52428800\n"),
        ("memory.peak", "73400320\n"),
    ]);
    assert_eq!(
        Reading::of(dir.path()),
        Reading {
            periods: Some(1200),
            throttled_periods: Some(300),
            throttled_seconds: Some(4.5),
            quota_cores: Some(1.0),
            period_seconds: Some(0.1),
            memory_bytes: Some(52_428_800),
            memory_peak_bytes: Some(73_400_320),
        }
    );
}

#[test]
fn a_quota_is_cores_over_the_period() {
    let dir = cgroup(&[("cpu.max", "50000 100000\n")]);
    let reading = Reading::of(dir.path());
    assert_eq!(reading.quota_cores, Some(0.5));
    assert_eq!(reading.period_seconds, Some(0.1));
}

#[test]
fn max_is_no_quota() {
    let dir = cgroup(&[("cpu.max", "max 100000\n")]);
    let reading = Reading::of(dir.path());
    assert_eq!(reading.quota_cores, None);
    assert_eq!(reading.period_seconds, Some(0.1));
}

#[test]
fn missing_files_leave_their_figures_out() {
    let dir = cgroup(&[("memory.peak", "4096\n")]);
    assert_eq!(
        Reading::of(dir.path()),
        Reading {
            memory_peak_bytes: Some(4096),
            ..Reading::default()
        }
    );
}

/// With the cpu controller off for the cgroup, `cpu.stat` holds only the
/// usage keys.
#[test]
fn a_stat_without_throttling_keys_leaves_them_out() {
    let dir = cgroup(&[(
        "cpu.stat",
        "usage_usec 9000000\nuser_usec 6000000\nsystem_usec 3000000\n",
    )]);
    assert_eq!(Reading::of(dir.path()), Reading::default());
}

#[test]
fn malformed_values_are_left_out() {
    let dir = cgroup(&[
        (
            "cpu.stat",
            "nr_periods many\nnr_throttled\nthrottled_usec -1\n",
        ),
        ("cpu.max", "100000 0\n"),
        ("memory.current", "max\n"),
        ("memory.peak", ""),
    ]);
    assert_eq!(Reading::of(dir.path()), Reading::default());
    let dir = cgroup(&[("cpu.max", "100000\n")]);
    assert_eq!(Reading::of(dir.path()), Reading::default());
}

#[test]
fn a_key_matches_whole() {
    let dir = cgroup(&[("cpu.stat", "nr_periods_extra 7\nnr_periods 8\n")]);
    assert_eq!(Reading::of(dir.path()).periods, Some(8));
}

#[test]
fn the_cgroup_is_the_unified_line() {
    assert_eq!(own("0::/kubepods/pod1/abc\n"), Some("kubepods/pod1/abc"));
    assert_eq!(own("0::/\n"), Some(""));
    // Hybrid: the v1 controllers' lines come first.
    assert_eq!(
        own("12:memory:/user.slice\n1:name=systemd:/user.slice\n0::/user.slice\n"),
        Some("user.slice")
    );
    // Pure v1: no unified line.
    assert_eq!(
        own("4:cpu,cpuacct:/docker/abc\n3:memory:/docker/abc\n"),
        None
    );
    assert_eq!(own(""), None);
}

/// The families as served, by name: type and value.
fn served(collector: &CgroupCollector) -> Vec<(String, prometheus::proto::MetricType, f64)> {
    collector
        .collect()
        .into_iter()
        .map(|family| {
            let metric = &family.get_metric()[0];
            let value = match family.get_field_type() {
                prometheus::proto::MetricType::COUNTER => metric.get_counter().get_value(),
                _ => metric.get_gauge().get_value(),
            };
            (family.name().to_string(), family.get_field_type(), value)
        })
        .collect()
}

#[test]
fn serves_counters_and_gauges_in_base_units() {
    use prometheus::proto::MetricType::{COUNTER, GAUGE};
    let dir = cgroup(&[
        ("cpu.stat", STAT),
        ("cpu.max", "150000 100000\n"),
        ("memory.current", "1024\n"),
        ("memory.peak", "2048\n"),
    ]);
    let collector = CgroupCollector::at(Some(dir.path().to_path_buf()));
    assert_eq!(
        served(&collector),
        [
            ("process_cgroup_cpu_periods_total".into(), COUNTER, 1200.0),
            (
                "process_cgroup_cpu_throttled_periods_total".into(),
                COUNTER,
                300.0
            ),
            (
                "process_cgroup_cpu_throttled_seconds_total".into(),
                COUNTER,
                4.5
            ),
            ("process_cgroup_cpu_quota_cores".into(), GAUGE, 1.5),
            ("process_cgroup_cpu_period_seconds".into(), GAUGE, 0.1),
            ("process_cgroup_memory_bytes".into(), GAUGE, 1024.0),
            ("process_cgroup_memory_peak_bytes".into(), GAUGE, 2048.0),
        ]
    );
}

/// The files are read at each scrape, not once.
#[test]
fn each_scrape_reads_the_files() {
    let dir = cgroup(&[("memory.current", "1\n")]);
    let collector = CgroupCollector::at(Some(dir.path().to_path_buf()));
    assert_eq!(served(&collector)[0].2, 1.0);
    std::fs::write(dir.path().join("memory.current"), "2\n").expect("rewrite");
    assert_eq!(served(&collector)[0].2, 2.0);
}

#[test]
fn no_cgroup_serves_nothing() {
    assert!(CgroupCollector::at(None).collect().is_empty());
    let collector = CgroupCollector::at(Some(Path::new("/nonexistent/gdl-cgroup").into()));
    assert!(collector.collect().is_empty());
}

/// Both collectors register in one registry, and a scrape encodes as text
/// with no family twice. A second registration is refused.
#[test]
fn registers_once() {
    let registry = prometheus::Registry::new();
    crate::register(&registry).expect("registered");
    let families = registry.gather();
    let mut names: Vec<&str> = families.iter().map(|family| family.name()).collect();
    let count = names.len();
    names.dedup();
    assert_eq!(names.len(), count, "a family twice: {names:?}");
    #[cfg(target_os = "linux")]
    assert!(names.contains(&"process_cpu_seconds_total"), "{names:?}");
    prometheus::TextEncoder::new()
        .encode_to_string(&families)
        .expect("encoded");
    assert!(crate::register(&registry).is_err());
}

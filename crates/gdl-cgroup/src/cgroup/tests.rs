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

/// A registry holding only the collector of the cgroup in `dir`.
fn registry(dir: Option<&Path>) -> prometheus::Registry {
    let registry = prometheus::Registry::new();
    registry
        .register(Box::new(CgroupCollector::at(dir.map(Path::to_path_buf))))
        .expect("registered");
    registry
}

/// Each family `registry` serves: name, type, and its one value. Read from
/// the text form, since the parsed form's accessors change with
/// prometheus's features.
fn served(registry: &prometheus::Registry) -> Vec<(String, String, f64)> {
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encoded");
    let mut families = Vec::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        let Some(typed) = line.strip_prefix("# TYPE ") else {
            continue;
        };
        let (name, kind) = typed.split_once(' ').expect("a type");
        let sample = lines.next().expect("a sample");
        let value = sample
            .strip_prefix(&format!("{name} "))
            .unwrap_or_else(|| panic!("{name}'s sample: {sample}"));
        families.push((
            name.to_string(),
            kind.to_string(),
            value.parse().expect("a number"),
        ));
    }
    families
}

#[test]
fn serves_counters_and_gauges_in_base_units() {
    let dir = cgroup(&[
        ("cpu.stat", STAT),
        ("cpu.max", "150000 100000\n"),
        ("memory.current", "1024\n"),
        ("memory.peak", "2048\n"),
    ]);
    let mut families = served(&registry(Some(dir.path())));
    families.sort_by(|a, b| a.0.cmp(&b.0));
    let expected = [
        ("process_cgroup_cpu_period_seconds", "gauge", 0.1),
        ("process_cgroup_cpu_periods_total", "counter", 1200.0),
        ("process_cgroup_cpu_quota_cores", "gauge", 1.5),
        (
            "process_cgroup_cpu_throttled_periods_total",
            "counter",
            300.0,
        ),
        ("process_cgroup_cpu_throttled_seconds_total", "counter", 4.5),
        ("process_cgroup_memory_bytes", "gauge", 1024.0),
        ("process_cgroup_memory_peak_bytes", "gauge", 2048.0),
    ]
    .map(|(name, kind, value)| (name.to_string(), kind.to_string(), value));
    assert_eq!(families, expected);
}

/// The files are read at each scrape, not once.
#[test]
fn each_scrape_reads_the_files() {
    let dir = cgroup(&[("memory.current", "1\n")]);
    let registry = registry(Some(dir.path()));
    assert_eq!(served(&registry)[0].2, 1.0);
    std::fs::write(dir.path().join("memory.current"), "2\n").expect("rewrite");
    assert_eq!(served(&registry)[0].2, 2.0);
}

#[test]
fn no_cgroup_serves_nothing() {
    assert!(served(&registry(None)).is_empty());
    assert!(served(&registry(Some(Path::new("/nonexistent/gdl-cgroup")))).is_empty());
}

/// Both collectors register in one registry, and a scrape encodes as text
/// with no family twice. A second registration is refused.
#[test]
fn registers_once() {
    let registry = prometheus::Registry::new();
    crate::register(&registry).expect("registered");
    let mut names: Vec<String> = served(&registry)
        .into_iter()
        .map(|(name, _, _)| name)
        .collect();
    let count = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), count, "a family twice: {names:?}");
    #[cfg(target_os = "linux")]
    assert!(
        names.iter().any(|name| name == "process_cpu_seconds_total"),
        "{names:?}"
    );
    assert!(crate::register(&registry).is_err());
}

/// A segment serves these names unprefixed beside its registry, so a
/// node's own metric must not take one: with an empty `metrics_prefix`,
/// the family would appear twice in one scrape.
#[test]
fn every_name_is_reserved_from_nodes() {
    let cgroup = CgroupCollector::at(None);
    let mut names: Vec<String> = cgroup
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
    for name in &names {
        assert!(
            gdl_model::RUNTIME_METRICS.contains(&name.as_str()),
            "{name} is not reserved"
        );
    }
}

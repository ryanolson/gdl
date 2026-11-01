//! The pool's load summary, made from its entry instances' reports.
//!
//! Each usable entry instance reports its admission once a period; the
//! hub keeps each one's latest report, and once a period sums the reports
//! it counts: those of instances still usable, and no older than three
//! periods. The instance and the hub keep their periods on their own
//! clocks, so two reports can arrive between two summaries: the later
//! one's admission stands, and the refusals of the earlier one, which no
//! summary counted, are carried into it. Taking a report and making a summary each cost the work of
//! one instance and of the pool's entry instances, once a period, never
//! per request. The reports are kept under the feed's lock, with the entry
//! instances they belong to, so that whether an instance is usable is
//! judged with its report.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gdl_pools::{KvLoad, LoadSummary};
use gdl_watch::LoadReport;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use velo_hub::protocol::RegistrationEpoch;
use velo_hub::velo::InstanceId;

use super::feed::Feed;

/// How many periods a report counts for without another from the same
/// instance.
const KEPT: u32 = 3;

/// Each entry instance's latest report, and the summary last made of
/// them.
pub(super) struct Reports {
    reports: HashMap<InstanceId, Report>,
    /// How long a report counts for.
    kept: Duration,
    /// The summary period, which each summary carries.
    period_ms: u64,
    summary: Option<LoadSummary>,
    /// The number of the last summary made: each is one past the last,
    /// changed or not, so that the pair of the hub's epoch and the number
    /// grows with every summary the watch sends.
    sequence: u64,
}

impl Reports {
    /// The reports of a pool whose summary period is `period`.
    pub(super) fn new(period: Duration) -> Self {
        Self {
            reports: HashMap::new(),
            kept: period * KEPT,
            period_ms: u64::try_from(period.as_millis()).unwrap_or(u64::MAX),
            summary: None,
            sequence: 0,
        }
    }

    /// `instance`, a usable entry instance in its registration `epoch`,
    /// reported `load` at `now`. Its admission replaces the earlier
    /// report's; its refusals are added to the earlier report's when no
    /// summary has counted those yet and both are of one registration, so
    /// that a summary's refusals are every refusal reported in its period,
    /// even when two reports of the instance fall in one period.
    pub(super) fn put(
        &mut self,
        instance: InstanceId,
        epoch: RegistrationEpoch,
        mut load: LoadReport,
        now: Instant,
    ) {
        if let Some(earlier) = self.reports.get(&instance)
            && !earlier.summarized
            && earlier.epoch == epoch
        {
            load.refusals = load.refusals.saturating_add(earlier.load.refusals);
        }
        let report = Report {
            at: now,
            epoch,
            load,
            summarized: false,
        };
        self.reports.insert(instance, report);
    }

    /// The latest report `instance` made in its registration `epoch`, and
    /// how long before `now` it was taken, while a summary at `now` would
    /// still count it: no older than three periods.
    pub(super) fn latest(
        &self,
        instance: &InstanceId,
        epoch: RegistrationEpoch,
        now: Instant,
    ) -> Option<(&LoadReport, Duration)> {
        let report = self.reports.get(instance)?;
        let age = now.saturating_duration_since(report.at);
        (report.epoch == epoch && age <= self.kept).then_some((&report.load, age))
    }

    /// The summary last made.
    pub(super) fn summary(&self) -> Option<LoadSummary> {
        self.summary
    }

    /// Make the summary at `now`, of the reports that `usable` says are of
    /// a usable entry instance in its current registration: the others,
    /// and those older than three periods, are dropped, so that a report
    /// never counts for a later registration of its instance. `None` while
    /// no report is counted. Each summary made is numbered one past the
    /// last and kept, whether or not it changed. The summary, if it
    /// changed, judged without its number.
    pub(super) fn summarize(
        &mut self,
        usable: impl Fn(&InstanceId, RegistrationEpoch) -> bool,
        instances: u32,
        now: Instant,
    ) -> Option<Option<LoadSummary>> {
        let kept = self.kept;
        self.reports.retain(|instance, report| {
            report.summarized = true;
            usable(instance, report.epoch) && now.saturating_duration_since(report.at) <= kept
        });
        let summary = (!self.reports.is_empty()).then(|| {
            let mut summary = LoadSummary {
                instances,
                period_ms: self.period_ms,
                ..LoadSummary::default()
            };
            for Report { load: report, .. } in self.reports.values() {
                summary.admitted = summary.admitted.saturating_add(report.admitted);
                summary.limit = summary.limit.saturating_add(report.limit);
                summary.refusals = summary.refusals.saturating_add(report.refusals);
            }
            summary.kv = KvLoad::summed(self.reports.values().map(|report| &report.load.kv));
            self.sequence = self.sequence.saturating_add(1);
            summary.sequence = self.sequence;
            summary
        });
        let changed =
            summary.map(LoadSummary::unnumbered) != self.summary.map(LoadSummary::unnumbered);
        self.summary = summary;
        changed.then_some(summary)
    }
}

/// One instance's latest report.
struct Report {
    at: Instant,
    /// The registration it was made in.
    epoch: RegistrationEpoch,
    load: LoadReport,
    /// Whether a summary has counted it.
    summarized: bool,
}

/// Make the pool's summary once a `period` until `cancel`, starting one
/// period from now, when the first reports can have arrived: each one
/// goes to the feed's watchers (every period while there is one), and to
/// `loads`, which the record is written from when it changed.
pub(super) async fn run(
    feed: Arc<Feed>,
    period: Duration,
    loads: watch::Sender<Option<LoadSummary>>,
    cancel: CancellationToken,
) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let summarizing = async {
        loop {
            ticks.tick().await;
            let summary = feed.summarize(Instant::now());
            loads.send_if_modified(|load| {
                let changed = *load != summary;
                *load = summary;
                changed
            });
        }
    };
    tokio::select! {
        () = summarizing => {}
        () = cancel.cancelled() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{LoadReport, LoadSummary, Reports};
    use std::time::{Duration, Instant};
    use velo_hub::protocol::RegistrationEpoch;
    use velo_hub::velo::InstanceId;

    /// The one registration every report here is made in.
    static EPOCH: std::sync::LazyLock<RegistrationEpoch> =
        std::sync::LazyLock::new(RegistrationEpoch::new);

    /// What `summarize` said, without the summary's number, which
    /// `kv::each_summary_is_numbered_one_past_the_last` checks.
    fn bare(said: Option<Option<LoadSummary>>) -> Option<Option<LoadSummary>> {
        said.map(|summary| summary.map(LoadSummary::unnumbered))
    }

    fn report(admitted: u64, limit: u64, refusals: u64) -> LoadReport {
        LoadReport {
            dropped: Vec::new(),
            properties: gdl_model::PropertyLists::new(),
            admitted,
            limit,
            refusals,
            ..Default::default()
        }
    }

    /// The summary sums what is counted and says so only when it
    /// changed; a report three periods old still counts, an older one
    /// does not, nor one of an instance no longer usable.
    #[test]
    fn a_summary_counts_fresh_reports_of_usable_instances() {
        let period = Duration::from_secs(1);
        let (a, b) = (InstanceId::new_v4(), InstanceId::new_v4());
        let start = Instant::now();
        let mut reports = Reports::new(period);
        assert_eq!(
            bare(reports.summarize(|_, _| true, 2, start)),
            None,
            "no change"
        );
        reports.put(a, *EPOCH, report(3, 8, 1), start);
        reports.put(b, *EPOCH, report(2, 8, 0), start + period);
        let both = LoadSummary {
            admitted: 5,
            limit: 16,
            instances: 2,
            refusals: 1,
            period_ms: 1000,
            ..Default::default()
        };
        assert_eq!(
            bare(reports.summarize(|_, _| true, 2, start)),
            Some(Some(both))
        );
        assert_eq!(
            bare(reports.summarize(|_, _| true, 2, start + period * 3)),
            None
        );
        assert_eq!(
            bare(reports.summarize(
                |_, _| true,
                2,
                start + period * 3 + Duration::from_millis(1)
            )),
            Some(Some(LoadSummary {
                admitted: 2,
                limit: 8,
                instances: 2,
                refusals: 0,
                period_ms: 1000,
                ..Default::default()
            })),
            "a's report is past three periods"
        );
        assert_eq!(
            bare(reports.summarize(|id, _| *id != b, 1, start + period * 3)),
            Some(None)
        );
        reports.put(b, *EPOCH, report(2, 8, 0), start);
        assert_eq!(
            bare(reports.summarize(|_, _| true, 1, start)),
            Some(Some(LoadSummary {
                admitted: 2,
                limit: 8,
                instances: 1,
                refusals: 0,
                period_ms: 1000,
                ..Default::default()
            })),
            "a dropped report is not kept"
        );
        assert_eq!(reports.summary().map(|summary| summary.admitted), Some(2));
    }

    /// The latest report is shown only for the registration it was made in,
    /// and only while a summary would count it: no older than three
    /// periods.
    #[test]
    fn the_latest_report_is_of_its_registration_and_fresh() {
        let period = Duration::from_secs(1);
        let a = InstanceId::new_v4();
        let start = Instant::now();
        let mut reports = Reports::new(period);
        assert!(reports.latest(&a, *EPOCH, start).is_none(), "no report yet");
        reports.put(a, *EPOCH, report(3, 8, 1), start);
        let latest = |at| {
            reports
                .latest(&a, *EPOCH, at)
                .map(|(load, age)| (load.admitted, load.limit, load.refusals, age))
        };
        assert_eq!(latest(start + period), Some((3, 8, 1, period)));
        assert_eq!(latest(start + period * 3), Some((3, 8, 1, period * 3)));
        assert_eq!(latest(start + period * 3 + Duration::from_millis(1)), None);
        assert!(
            reports
                .latest(&a, RegistrationEpoch::new(), start)
                .is_none(),
            "a later registration was shown an earlier one's report"
        );
    }

    /// The instance and the hub keep their periods on their own clocks, so
    /// two reports of one instance can arrive between two summaries: the
    /// later one's admission stands, and the earlier one's refusals are
    /// not lost, since no summary counted them.
    #[test]
    fn refusals_no_summary_counted_are_carried_into_the_next_report() {
        let period = Duration::from_secs(1);
        let a = InstanceId::new_v4();
        let start = Instant::now();
        let mut reports = Reports::new(period);
        reports.put(a, *EPOCH, report(3, 8, 2), start);
        reports.put(a, *EPOCH, report(1, 8, 1), start);
        assert_eq!(
            bare(reports.summarize(|_, _| true, 1, start)),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 3,
                period_ms: 1000,
                ..Default::default()
            }))
        );
        reports.put(a, *EPOCH, report(1, 8, 0), start);
        assert_eq!(
            bare(reports.summarize(|_, _| true, 1, start)),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 0,
                period_ms: 1000,
                ..Default::default()
            })),
            "refusals a summary counted were carried again"
        );
        reports.put(a, *EPOCH, report(1, 8, 2), start);
        reports.put(a, *EPOCH, report(1, 8, 0), start);
        assert_eq!(
            bare(reports.summarize(|_, _| true, 1, start)),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 2,
                period_ms: 1000,
                ..Default::default()
            })),
            "the summary's period lost the refusals of a report it never counted"
        );
        let later = RegistrationEpoch::new();
        reports.put(a, *EPOCH, report(1, 8, 2), start);
        reports.put(a, later, report(1, 8, 0), start);
        assert_eq!(
            bare(reports.summarize(|_, epoch| epoch == later, 1, start)),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 0,
                period_ms: 1000,
                ..Default::default()
            })),
            "an earlier registration's refusals were carried into a later one's"
        );
    }
}

#[cfg(test)]
mod kv {
    use super::{LoadReport, Reports};
    use gdl_pools::KvLoad;
    use std::time::{Duration, Instant};
    use velo_hub::protocol::RegistrationEpoch;
    use velo_hub::velo::InstanceId;

    fn report(kv: KvLoad) -> LoadReport {
        LoadReport {
            admitted: 1,
            limit: 8,
            kv,
            ..Default::default()
        }
    }

    /// Each summary the hub makes is numbered one past the last, also
    /// when the reports have not changed and when the hub makes more than
    /// one in a period; whether it changed is judged without the number.
    #[test]
    fn each_summary_is_numbered_one_past_the_last() {
        let period = Duration::from_secs(1);
        let epoch = RegistrationEpoch::new();
        let a = InstanceId::new_v4();
        let start = Instant::now();
        let mut reports = Reports::new(period);
        reports.put(a, epoch, report(KvLoad::NONE), start);
        let first = reports.summarize(|_, _| true, 1, start);
        assert_eq!(first.flatten().map(|summary| summary.sequence), Some(1));
        let mut numbers = vec![1];
        for _ in 0..3 {
            reports.put(a, epoch, report(KvLoad::NONE), start);
            assert_eq!(
                reports.summarize(|_, _| true, 1, start),
                None,
                "the same reports are no change"
            );
            numbers.push(reports.summary().expect("a summary").sequence);
        }
        assert_eq!(numbers, [1, 2, 3, 4]);
        // A period with no summary takes no number.
        assert_eq!(reports.summarize(|_, _| false, 0, start), Some(None));
        assert_eq!(reports.summarize(|_, _| false, 0, start), None);
        reports.put(a, epoch, report(KvLoad::NONE), start);
        let next = reports.summarize(|_, _| true, 1, start);
        assert_eq!(next.flatten().map(|summary| summary.sequence), Some(5));
    }

    /// The KV numbers sum over two instances' reports as `admitted` does,
    /// the eviction age is the least, and a report past three periods
    /// takes its numbers with it.
    #[test]
    fn a_summary_sums_two_instances_kv_and_keeps_only_fresh_reports() {
        let period = Duration::from_secs(1);
        let epoch = RegistrationEpoch::new();
        let (a, b) = (InstanceId::new_v4(), InstanceId::new_v4());
        let start = Instant::now();
        let mut reports = Reports::new(period);
        let ka = KvLoad::NONE
            .with_capacity(1_000)
            .with_active(100)
            .with_committed(600)
            .with_reserved(50)
            .with_eviction_age_ms(40_000);
        let kb = KvLoad::NONE
            .with_capacity(2_000)
            .with_active(300)
            .with_committed(900)
            .with_reserved(70)
            .with_eviction_age_ms(9_000);
        reports.put(a, epoch, report(ka), start);
        reports.put(b, epoch, report(kb), start + period);
        let both = reports
            .summarize(|_, _| true, 2, start + period)
            .flatten()
            .expect("a summary");
        assert_eq!(both.admitted, 2);
        assert_eq!(both.kv.capacity(), Some(3_000));
        assert_eq!(both.kv.active(), Some(400));
        assert_eq!(both.kv.committed(), Some(1_500));
        assert_eq!(both.kv.reserved(), Some(120));
        assert_eq!(both.kv.eviction_age_ms(), Some(9_000));
        let later = start + period * 3 + Duration::from_millis(1);
        let one = reports
            .summarize(|_, _| true, 2, later)
            .flatten()
            .expect("b's report still counts");
        assert_eq!(one.admitted, 1, "a's report is past three periods");
        assert_eq!(one.kv, kb, "and its KV went with it");
    }

    /// During a roll, an instance of the earlier build reports no KV: the
    /// pool's KV is the other instance's alone, not a guess for two, while
    /// its admission counts both.
    #[test]
    fn an_instance_of_an_earlier_build_adds_no_kv() {
        let period = Duration::from_secs(1);
        let epoch = RegistrationEpoch::new();
        let (new, earlier) = (InstanceId::new_v4(), InstanceId::new_v4());
        let start = Instant::now();
        let mut reports = Reports::new(period);
        let kv = KvLoad::NONE.with_capacity(1_000).with_committed(400);
        reports.put(new, epoch, report(kv), start);
        reports.put(earlier, epoch, report(KvLoad::NONE), start);
        let summary = reports
            .summarize(|_, _| true, 2, start)
            .flatten()
            .expect("a summary");
        assert_eq!(summary.admitted, 2);
        assert_eq!(summary.instances, 2);
        assert_eq!(summary.kv.capacity(), Some(1_000));
        assert_eq!(summary.kv, kv);
    }
}

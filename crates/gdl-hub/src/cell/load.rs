//! The cell's load summary, made from its entry instances' reports.
//!
//! Each usable entry instance reports its admission once a period; the
//! hub keeps each one's latest report, and once a period sums the reports
//! it counts: those of instances still usable, and no older than three
//! periods. The instance and the hub keep their periods on their own
//! clocks, so two reports can arrive between two summaries: the later
//! one's admission stands, and the refusals of the earlier one, which no
//! summary counted, are carried into it. Taking a report and making a summary each cost the work of
//! one instance and of the cell's entry instances, once a period, never
//! per request. The reports are kept under the feed's lock, with the entry
//! instances they belong to, so that whether an instance is usable is
//! judged with its report.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gdl_cells::LoadSummary;
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
}

impl Reports {
    /// The reports of a cell whose summary period is `period`.
    pub(super) fn new(period: Duration) -> Self {
        Self {
            reports: HashMap::new(),
            kept: period * KEPT,
            period_ms: u64::try_from(period.as_millis()).unwrap_or(u64::MAX),
            summary: None,
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

    /// The summary last made.
    pub(super) fn summary(&self) -> Option<LoadSummary> {
        self.summary
    }

    /// Make the summary at `now`, of the reports that `usable` says are of
    /// a usable entry instance in its current registration: the others,
    /// and those older than three periods, are dropped, so that a report
    /// never counts for a later registration of its instance. `None` while
    /// no report is counted. The summary, if it changed.
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
            summary
        });
        (summary != self.summary).then(|| {
            self.summary = summary;
            summary
        })
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

/// Make the cell's summary once a `period` until `cancel`, starting one
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

    fn report(admitted: u64, limit: u64, refusals: u64) -> LoadReport {
        LoadReport {
            models: None,
            admitted,
            limit,
            refusals,
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
        assert_eq!(reports.summarize(|_, _| true, 2, start), None, "no change");
        reports.put(a, *EPOCH, report(3, 8, 1), start);
        reports.put(b, *EPOCH, report(2, 8, 0), start + period);
        let both = LoadSummary {
            admitted: 5,
            limit: 16,
            instances: 2,
            refusals: 1,
            period_ms: 1000,
        };
        assert_eq!(reports.summarize(|_, _| true, 2, start), Some(Some(both)));
        assert_eq!(reports.summarize(|_, _| true, 2, start + period * 3), None);
        assert_eq!(
            reports.summarize(
                |_, _| true,
                2,
                start + period * 3 + Duration::from_millis(1)
            ),
            Some(Some(LoadSummary {
                admitted: 2,
                limit: 8,
                instances: 2,
                refusals: 0,
                period_ms: 1000,
            })),
            "a's report is past three periods"
        );
        assert_eq!(
            reports.summarize(|id, _| *id != b, 1, start + period * 3),
            Some(None)
        );
        reports.put(b, *EPOCH, report(2, 8, 0), start);
        assert_eq!(
            reports.summarize(|_, _| true, 1, start),
            Some(Some(LoadSummary {
                admitted: 2,
                limit: 8,
                instances: 1,
                refusals: 0,
                period_ms: 1000,
            })),
            "a dropped report is not kept"
        );
        assert_eq!(reports.summary().map(|summary| summary.admitted), Some(2));
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
            reports.summarize(|_, _| true, 1, start),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 3,
                period_ms: 1000,
            }))
        );
        reports.put(a, *EPOCH, report(1, 8, 0), start);
        assert_eq!(
            reports.summarize(|_, _| true, 1, start),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 0,
                period_ms: 1000,
            })),
            "refusals a summary counted were carried again"
        );
        reports.put(a, *EPOCH, report(1, 8, 2), start);
        reports.put(a, *EPOCH, report(1, 8, 0), start);
        assert_eq!(
            reports.summarize(|_, _| true, 1, start),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 2,
                period_ms: 1000,
            })),
            "the summary's period lost the refusals of a report it never counted"
        );
        let later = RegistrationEpoch::new();
        reports.put(a, *EPOCH, report(1, 8, 2), start);
        reports.put(a, later, report(1, 8, 0), start);
        assert_eq!(
            reports.summarize(|_, epoch| epoch == later, 1, start),
            Some(Some(LoadSummary {
                admitted: 1,
                limit: 8,
                instances: 1,
                refusals: 0,
                period_ms: 1000,
            })),
            "an earlier registration's refusals were carried into a later one's"
        );
    }
}

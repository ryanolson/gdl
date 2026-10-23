//! An entry instance's load reports to its own cell's hub: once a summary
//! period, what its admission holds, through velo-hub's extension client
//! with the instance's own credential. A report reads counters the
//! admission and the forward entries already keep, so a request's path
//! gains nothing.

use std::time::Duration;

use gdl_watch::LoadReport;
use tokio::task::JoinHandle;
use velo::InstanceId;
use velo_hub::client::ClientError;

use super::HubSession;
use crate::dist::SegmentLoad;

impl HubSession {
    /// Report what `read` gives, as the instance `instance` this session
    /// registered, once every `period`, starting one period from now,
    /// until the handle is aborted: by then the hub has seen the instance
    /// ready, and takes its report. Each report's refusals are the
    /// hand-offs declined since the last report the hub took, or since the
    /// instance started for the first report, so a report it refused, or
    /// that went unanswered, loses none. A refusal is no failure of the
    /// instance's: it is logged once, until a report is taken again. `read`
    /// gives `None` when there is nothing to report.
    pub(crate) fn report_load(
        &self,
        instance: InstanceId,
        period: Duration,
        read: impl Fn() -> Option<SegmentLoad> + Send + 'static,
    ) -> JoinHandle<()> {
        let ext = self.client.ext(gdl_watch::EXTENSION);
        let path = gdl_watch::load_path(instance);
        tokio::spawn(report(period, read, move |report| {
            let (ext, path) = (ext.clone(), path.clone());
            async move {
                ext.control_post::<_, ()>(&path, &report)
                    .await
                    .map_err(|error: ClientError| error.to_string())
            }
        }))
    }
}

/// [`HubSession::report_load`]'s reports, each given to `send`, whose
/// answer says whether the hub took it.
async fn report<F, Fut>(period: Duration, read: impl Fn() -> Option<SegmentLoad>, mut send: F)
where
    F: FnMut(LoadReport) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let start = tokio::time::Instant::now() + period;
    let mut ticks = tokio::time::interval_at(start, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // No report has been sent, so every decline since the instance
    // started is the first report's.
    let mut taken = 0;
    let mut refused: Option<String> = None;
    loop {
        ticks.tick().await;
        let Some(load) = read() else {
            continue;
        };
        let report = LoadReport {
            admitted: wide(load.admitted),
            limit: wide(load.limit),
            refusals: wide(load.declined.saturating_sub(taken)),
        };
        // Bounded by the period, so that a hub that does not answer never
        // stacks reports.
        let answer = match tokio::time::timeout(period, send(report)).await {
            Ok(answer) => answer,
            Err(_) => Err("the cell's hub did not answer in time".to_string()),
        };
        match answer {
            Ok(()) => {
                taken = load.declined;
                if refused.take().is_some() {
                    tracing::info!("the cell's hub takes this instance's load reports again");
                }
            }
            Err(error) => {
                if refused.as_deref() != Some(error.as_str()) {
                    tracing::warn!(%error, "a load report was not taken");
                    refused = Some(error);
                }
            }
        }
    }
}

fn wide(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{LoadReport, SegmentLoad, report};
    use std::time::Duration;
    use tokio::sync::mpsc;

    const PERIOD: Duration = Duration::from_secs(1);

    /// Run the reports of an instance whose admission reads `load`, and
    /// return what it sends, each answered `taken`.
    fn reporting(
        load: SegmentLoad,
        taken: bool,
    ) -> (
        mpsc::UnboundedReceiver<LoadReport>,
        tokio::task::JoinHandle<()>,
    ) {
        let (sent, reports) = mpsc::unbounded_channel();
        let task = tokio::spawn(report(
            PERIOD,
            move || Some(load),
            move |report| {
                let _ = sent.send(report);
                async move {
                    if taken {
                        Ok(())
                    } else {
                        Err("refused".to_string())
                    }
                }
            },
        ));
        (reports, task)
    }

    async fn next(reports: &mut mpsc::UnboundedReceiver<LoadReport>) -> LoadReport {
        tokio::time::timeout(PERIOD * 2, reports.recv())
            .await
            .expect("a report in its period")
            .expect("the reports go on")
    }

    /// The first report counts the hand-offs declined before reporting
    /// began: no report has been sent, so none of them was reported.
    #[tokio::test(start_paused = true)]
    async fn the_first_report_counts_every_decline_before_it() {
        let load = SegmentLoad {
            admitted: 1,
            limit: 4,
            declined: 3,
        };
        let (mut reports, task) = reporting(load, true);
        assert_eq!(
            next(&mut reports).await,
            LoadReport {
                admitted: 1,
                limit: 4,
                refusals: 3,
            }
        );
        assert_eq!(next(&mut reports).await.refusals, 0, "taken, so not again");
        task.abort();
    }

    /// A report the hub refused loses none of its refusals: the next one
    /// carries them.
    #[tokio::test(start_paused = true)]
    async fn a_refused_report_keeps_its_refusals_for_the_next() {
        let load = SegmentLoad {
            admitted: 0,
            limit: 4,
            declined: 2,
        };
        let (mut reports, task) = reporting(load, false);
        assert_eq!(next(&mut reports).await.refusals, 2);
        assert_eq!(next(&mut reports).await.refusals, 2);
        task.abort();
    }
}

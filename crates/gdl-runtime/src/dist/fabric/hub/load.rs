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
    /// hand-offs declined since the last report the hub took, so a report
    /// it refused, or that went unanswered, loses none. A refusal is no
    /// failure of the instance's: it is logged once, until a report is
    /// taken again. `read` gives `None` when there is nothing to report.
    pub(crate) fn report_load(
        &self,
        instance: InstanceId,
        period: Duration,
        read: impl Fn() -> Option<SegmentLoad> + Send + 'static,
    ) -> JoinHandle<()> {
        let ext = self.client.ext(gdl_watch::EXTENSION);
        let path = gdl_watch::load_path(instance);
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + period;
            let mut ticks = tokio::time::interval_at(start, period);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut taken = read().map_or(0, |load| load.declined);
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
                // Bounded by the period, so that a hub that does not
                // answer never stacks reports.
                let sent = tokio::time::timeout(period, ext.control_post::<_, ()>(&path, &report));
                match sent.await {
                    Ok(Ok(())) => {
                        taken = load.declined;
                        if refused.take().is_some() {
                            tracing::info!(
                                "the cell's hub takes this instance's load reports again"
                            );
                        }
                    }
                    Ok(Err(error)) => note(&mut refused, &error),
                    Err(_) => {
                        let error = "the cell's hub did not answer in time";
                        if refused.as_deref() != Some(error) {
                            tracing::warn!(error, "a load report was not taken");
                            refused = Some(error.to_string());
                        }
                    }
                }
            }
        })
    }
}

/// Log `error` unless it is the one last logged.
fn note(refused: &mut Option<String>, error: &ClientError) {
    let error = error.to_string();
    if refused.as_deref() != Some(error.as_str()) {
        tracing::warn!(%error, "a load report was not taken");
        *refused = Some(error);
    }
}

fn wide(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

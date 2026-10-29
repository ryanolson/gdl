//! An entry instance's load reports to its own pool's hub: once a summary
//! period, what its admission holds, through velo-hub's extension client
//! with the instance's own credential, with the models its nodes say it
//! has; and at once when those change. A report reads counters the
//! admission and the forward entries already keep, so a request's path
//! gains nothing.

use std::time::Duration;

use gdl::InstanceModels;
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
    /// gives `None` when there is nothing to report. Each report carries
    /// the list `models` holds, if a node set one, and a change of it is
    /// reported at once; once the hub answers a report, which it does for
    /// one that carries a drain once a record that carries the drain is
    /// stored, `models` hears what it carried, so that a drain it carried
    /// can leave the list.
    pub(crate) fn report_load(
        &self,
        instance: InstanceId,
        period: Duration,
        read: impl Fn() -> Option<SegmentLoad> + Send + 'static,
        models: InstanceModels,
    ) -> Reporter {
        let ext = self.client.ext(gdl_watch::EXTENSION);
        let path = gdl_watch::load_path(instance);
        let task = tokio::spawn(report(period, read, models.clone(), move |report| {
            let (ext, path) = (ext.clone(), path.clone());
            async move {
                ext.control_post::<_, ()>(&path, &report)
                    .await
                    .map_err(|error: ClientError| error.to_string())
            }
        }));
        Reporter { task, models }
    }
}

/// An entry instance's load reports, which end when it is dropped.
pub(crate) struct Reporter {
    task: JoinHandle<()>,
    models: InstanceModels,
}

impl Reporter {
    /// Report until every drain the instance's list carried has been in a
    /// report its hub answered, or until `deadline`, then stop. An instance
    /// that stops waits for it, while it drains, before it retires, since
    /// its hub takes no report from it after: a drain its hub took but had
    /// not stored would be lost if the hub restarted.
    pub(crate) async fn finish(self, deadline: tokio::time::Instant) {
        let _ = tokio::time::timeout_at(deadline, self.models.reported()).await;
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// [`HubSession::report_load`]'s reports, each given to `send`, whose
/// answer says whether the hub took it.
async fn report<F, Fut>(
    period: Duration,
    read: impl Fn() -> Option<SegmentLoad>,
    models: InstanceModels,
    mut send: F,
) where
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
    let mut said = models.watch();
    loop {
        tokio::select! {
            _ = ticks.tick() => {}
            Ok(()) = said.changed() => {}
        }
        let Some(load) = read() else {
            continue;
        };
        let carried = said.borrow_and_update().clone();
        let report = LoadReport {
            models: carried.as_deref().map(<[_]>::to_vec),
            admitted: wide(load.admitted),
            limit: wide(load.limit),
            refusals: wide(load.declined.saturating_sub(taken)),
        };
        // Bounded by the period, so that a hub that does not answer never
        // stacks reports.
        let answer = match tokio::time::timeout(period, send(report)).await {
            Ok(answer) => answer,
            Err(_) => Err("the pool's hub did not answer in time".to_string()),
        };
        match answer {
            Ok(()) => {
                taken = load.declined;
                if let Some(carried) = &carried {
                    models.taken(carried);
                }
                if refused.take().is_some() {
                    tracing::info!("the pool's hub takes this instance's load reports again");
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
    use gdl::InstanceModels;
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
        reporting_models(load, taken, &InstanceModels::new())
    }

    /// [`reporting`], with the models the instance's nodes set on `models`.
    fn reporting_models(
        load: SegmentLoad,
        taken: bool,
        models: &InstanceModels,
    ) -> (
        mpsc::UnboundedReceiver<LoadReport>,
        tokio::task::JoinHandle<()>,
    ) {
        let (sent, reports) = mpsc::unbounded_channel();
        let task = tokio::spawn(report(
            PERIOD,
            move || Some(load),
            models.clone(),
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
                models: None,
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

#[cfg(test)]
mod models {
    use super::{LoadReport, Reporter, SegmentLoad, report};
    use gdl::{InstanceModel, InstanceModelState, InstanceModels};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc;

    const PERIOD: Duration = Duration::from_secs(10);

    fn ready(name: &str) -> InstanceModel {
        InstanceModel {
            model: name.into(),
            routing_key: None,
            state: InstanceModelState::Ready,
        }
    }

    /// A change of the instance's models is reported at once, not at the
    /// next period, and every report after it carries the list, so that a
    /// hub that lost it, by a restart or a refused report, has it again
    /// within a period. With no list set, a report carries none.
    #[tokio::test(start_paused = true)]
    async fn a_change_of_models_is_reported_at_once_and_every_period() {
        let models = InstanceModels::new();
        let (sent, mut reports) = mpsc::unbounded_channel::<(LoadReport, tokio::time::Instant)>();
        let load = SegmentLoad {
            admitted: 0,
            limit: 4,
            declined: 0,
        };
        let start = tokio::time::Instant::now();
        let task = tokio::spawn(report(
            PERIOD,
            move || Some(load),
            models.clone(),
            move |report| {
                let _ = sent.send((report, tokio::time::Instant::now()));
                async { Ok(()) }
            },
        ));
        let mut next = async || {
            tokio::time::timeout(PERIOD * 2, reports.recv())
                .await
                .expect("a report in time")
                .expect("the reports go on")
        };
        let (first, at) = next().await;
        assert_eq!(first.models, None);
        assert_eq!(at - start, PERIOD);

        tokio::time::advance(PERIOD / 2).await;
        models.set([ready("m")]).expect("a list");
        let (changed, at) = next().await;
        assert_eq!(changed.models, Some(vec![ready("m")]));
        assert_eq!(at - start, PERIOD + PERIOD / 2, "at once");
        let (later, _) = next().await;
        assert_eq!(later.models, Some(vec![ready("m")]));
        task.abort();
    }

    /// An instance that stops while its hub has not answered a report that
    /// carries a drain goes on reporting the drain until a report is
    /// answered, then stops; one whose hub answers none stops at its
    /// deadline; and one with no drain to report stops at once.
    #[tokio::test(start_paused = true)]
    async fn a_stopping_instance_reports_its_drain_until_answered() {
        let load = SegmentLoad {
            admitted: 0,
            limit: 4,
            declined: 0,
        };
        let deadline = PERIOD * 10;
        // The hub refuses the first `refused` reports, then answers: how
        // long the stop waited, and whether each report up to the answered
        // one carried the drain.
        let stop = async |models: &InstanceModels, refused: usize| {
            let sent = Arc::new(AtomicUsize::new(0));
            let (carried, mut carries) = mpsc::unbounded_channel();
            let counted = Arc::clone(&sent);
            let task = tokio::spawn(report(
                PERIOD,
                move || Some(load),
                models.clone(),
                move |report: LoadReport| {
                    let n = counted.fetch_add(1, Ordering::SeqCst);
                    let draining = report.models.is_some_and(|models| {
                        models
                            .iter()
                            .any(|model| model.state == InstanceModelState::Draining)
                    });
                    if n <= refused {
                        let _ = carried.send(draining);
                    }
                    async move {
                        if n < refused {
                            Err("not stored yet".to_string())
                        } else {
                            Ok(())
                        }
                    }
                },
            ));
            let reporter = Reporter {
                task,
                models: models.clone(),
            };
            let start = tokio::time::Instant::now();
            reporter.finish(start + deadline).await;
            let waited = start.elapsed();
            let mut all = Vec::new();
            while let Ok(draining) = carries.try_recv() {
                all.push(draining);
            }
            (waited, all)
        };
        let draining = InstanceModel {
            state: InstanceModelState::Draining,
            ..ready("m")
        };

        let models = InstanceModels::new();
        models.set([draining.clone()]).expect("a list");
        models.set([]).expect("the node drops the model");
        assert_eq!(
            stop(&models, 2).await,
            (PERIOD * 3, vec![true; 3]),
            "answered at the third"
        );

        let models = InstanceModels::new();
        models.set([draining]).expect("a list");
        let (waited, _) = stop(&models, usize::MAX).await;
        assert_eq!(waited, deadline, "never answered: held to the deadline");

        let models = InstanceModels::new();
        models.set([ready("m")]).expect("a list");
        assert_eq!(stop(&models, usize::MAX).await, (Duration::ZERO, vec![]));
    }
}

//! `cargo gdl leave`: a joiner of a running pool on a Slurm site removed
//! cleanly. One `SIGTERM` reaches its instances, which retire through
//! their hub, drain, and exit; then it is waited for until it has ended.
//! A retire through the hub alone would not do: a retired instance holds
//! after its drain until a signal ends it. See `docs/src/guides/slurm.md`.

use std::time::{Duration, Instant};

use super::SlurmPlace;
use super::cli::Lines;
use super::jobs::{self, Job, Kind, Step};
use super::join::{Joiner, grace};
use super::requests::Registry;
use crate::Error;
use crate::deployment::slurm::launcher;
use gdl_model::launch::Done;

impl SlurmPlace<'_> {
    /// Remove `joiner`, which `join` started: a joining job, or a joiner
    /// that a launcher started on a spare node of a pool's job. Refused,
    /// with nothing signalled, for any other job, step, or launch.
    pub(in super::super) fn leave_joiner(
        &mut self,
        joiner: &Joiner,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        match joiner {
            Joiner::Job(id) => self.leave_job(id, say),
            Joiner::Step(id) => self.leave_step(id, say),
        }
    }

    /// Remove joining job `id`: cancel it while it waits in the queue, and
    /// otherwise signal each step of its segment, never its batch shell,
    /// once, as the step appears, until the job has ended: srt-slurm may
    /// still be starting the segment when the job runs, and a step that
    /// starts after one signal to the job would never get one. It must end
    /// `COMPLETED`: its instances left on the signal.
    fn leave_job(&mut self, id: &str, say: &mut dyn FnMut(&str)) -> Result<(), Error> {
        let (job, pool) = self.joining_job(id)?;
        let segment = job.segment(&self.fleet).unwrap_or_default().to_string();
        let grace = grace(&self.home.dir.join(&pool), &segment)?;
        let what = format!("removing job {id}, which joins pool `{pool}`");
        if job.pending() {
            let started = self.cancel_pending(&[id], &what)?;
            if started.is_empty() {
                // No longer listed: cancelled as it waited, or it started
                // and ended before the cancel reached it. Only accounting
                // tells which.
                let (state, exit) = self.end_of(id, &what)?;
                if state != "CANCELLED" && state != "COMPLETED" {
                    return Err(Error::new(format!(
                        "{what}: job {id} started before its cancel and ended {state}, exit code {exit}; {}",
                        self.logs_of(id)
                    )));
                }
                say(&format!(
                    "leave: job {id}, which waited to join pool `{pool}`, ended {state}"
                ));
                return Ok(());
            }
        }
        let bound = Duration::from_secs(grace) + self.waits.drain;
        let mut signalled: Vec<String> = Vec::new();
        self.until(bound, &what, |place| {
            let Some(now) = place.list()?.into_iter().find(|job| job.id == id) else {
                return Ok(true);
            };
            // Ending: its processes are being stopped already.
            if now.ending() {
                return Ok(false);
            }
            let text = place.run(&Lines::steps(id, place.waits.objects), &what)?;
            for step in jobs::joining_steps(&text, &segment) {
                if signalled.contains(&step) {
                    continue;
                }
                let reply = place
                    .slurm
                    .run(&Lines::terminate(&[&step], place.waits.objects))?;
                // One that ended before its signal is passed over.
                if !reply.success
                    && jobs::joining_steps(
                        &place.run(&Lines::steps(id, place.waits.objects), &what)?,
                        &segment,
                    )
                    .contains(&step)
                {
                    return Err(Error::new(format!(
                        "{what}: `scancel --signal=TERM {step}` failed: {}",
                        reply.stderr.trim()
                    )));
                }
                say(&format!(
                    "leave: step {step}'s instance of `{segment}` leaves pool `{pool}` on SIGTERM"
                ));
                signalled.push(step);
            }
            Ok(false)
        })?;
        let (state, exit) = self.end_of(id, &what)?;
        if state != "COMPLETED" {
            return Err(Error::new(format!(
                "{what}: job {id} ended {state}, exit code {exit}, not COMPLETED, so its instances may not have drained; {}",
                self.logs_of(id)
            )));
        }
        say(&format!("leave: job {id} ended {state}"));
        Ok(())
    }

    /// How job `id`, which Slurm no longer lists, ended, as Slurm's
    /// accounting says: its state and exit code. Accounting can lag the
    /// job's end, so a read that fails or finds no record, or finds it
    /// still running, is made again, up to the drain wait; then refused,
    /// since only a record says that the job completed.
    fn end_of(&mut self, id: &str, what: &str) -> Result<(String, String), Error> {
        let deadline = Instant::now() + self.waits.drain;
        loop {
            let reply = self.slurm.run(&Lines::ended(id, self.waits.objects))?;
            let found = reply
                .success
                .then(|| jobs::ended(&reply.stdout, id))
                .flatten();
            let last = match found {
                Some((state, exit)) if !jobs::live(&state) => return Ok((state, exit)),
                Some((state, _)) => format!("Slurm's accounting still says {state}"),
                None if reply.success => "Slurm's accounting keeps no record of it".to_string(),
                None => format!("`sacct` failed: {}", reply.stderr.trim()),
            };
            if Instant::now() >= deadline {
                return Err(Error::new(format!(
                    "{what}: job {id} has ended, and how is unknown, so it is not known to have completed: {last}; {}",
                    self.logs_of(id)
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
    }

    /// The fleet's joining job `id`, as Slurm lists it, and the pool it
    /// joins.
    fn joining_job(&mut self, id: &str) -> Result<(Job, String), Error> {
        let job = self
            .list()?
            .into_iter()
            .find(|job| job.id == id)
            .ok_or_else(|| {
                Error::new(format!(
                    "job {id} is no job of fleet `{}` that Slurm lists: it has ended, or it is not the fleet's",
                    self.fleet
                ))
            })?;
        match &job.kind {
            Kind::Join(pool) => {
                let pool = pool.clone();
                Ok((job, pool))
            }
            Kind::Pool(pool) => Err(Error::new(format!(
                "job {id} is pool `{pool}`'s own job, not a joiner: `cargo gdl down` stops a pool"
            ))),
            Kind::Directory => Err(Error::new(format!(
                "job {id} is the directory's job, not a joiner: `cargo gdl down` stops it last"
            ))),
        }
    }

    /// Remove joiner `id`, `<job>.<step>+<launch>`, which `join --in-job`
    /// started as the child of a launcher on a spare node of a pool's job:
    /// under the pool's registry lock, ask that launcher to stop launch
    /// `<launch>`, on which it sends its child one `SIGTERM`; then wait
    /// until it says the child has exited, which must be with code 0: the
    /// instance left on the signal. Its spare node is then free for
    /// another joiner. Refused, with nothing asked, for a step that is no
    /// launcher, and a launch that it did not start or that has ended.
    fn leave_step(&mut self, id: &str, say: &mut dyn FnMut(&str)) -> Result<(), Error> {
        let Some((step_id, launch)) = id.split_once('+').filter(|(step, launch)| {
            !launch.is_empty()
                && !launch.contains(char::is_whitespace)
                && step
                    .split_once('.')
                    .is_some_and(|(job, step)| digits(job) && digits(step))
        }) else {
            return Err(Error::new(format!(
                "`{id}` is not a joiner, `<job>.<step>+<launch>`, as `cargo gdl join --in-job` prints it"
            )));
        };
        let (job_id, _) = step_id.split_once('.').unwrap_or_default();
        let job = self
            .list()?
            .into_iter()
            .find(|job| job.id == job_id)
            .ok_or_else(|| {
                Error::new(format!(
                    "job {job_id} is no job of fleet `{}` that Slurm lists",
                    self.fleet
                ))
            })?;
        let Kind::Pool(pool) = job.kind.clone() else {
            return Err(Error::new(format!(
                "job {job_id} is not a pool's job: a joiner joins a pool in the pool's own job"
            )));
        };
        let what = format!("removing joiner {id} of pool `{pool}`'s job");
        let listed = self.placed(job_id, &what)?;
        let step = listed
            .iter()
            .find(|step| step.id == step_id)
            .ok_or_else(|| {
                Error::new(format!(
                    "{what}: Slurm lists no step {step_id}: its launcher has ended, and its joiner with it"
                ))
            })?
            .clone();
        let dir = self.home.dir.join(&pool);
        let request = {
            let registry = Registry::open(&dir, &pool, &self.waits, None)?;
            let Some(request) = registry
                .requests
                .iter()
                .find(|request| request.job == job_id && request.launch == launch)
                .cloned()
            else {
                return Err(Error::new(match step.launcher() {
                    true => format!(
                        "{what}: {} holds no launch {launch} of job {job_id}: it has ended, or `cargo gdl join` did not ask for it",
                        Registry::path(&dir).display()
                    ),
                    false => format!(
                        "step {step_id} is `{}`, not a launcher on a spare node: `cargo gdl down` stops a pool's own processes",
                        step.name
                    ),
                }));
            };
            if request.step != step_id {
                return Err(Error::new(format!(
                    "{what}: launch {launch} runs in step {}, not {step_id}",
                    request.step
                )));
            }
            if let Some(done) = request.done(&dir) {
                return Err(Error::new(format!(
                    "{what}: launch {launch} on node {} has ended already ({done})",
                    request.node
                )));
            }
            if !request.started(&dir) {
                return Err(Error::new(format!(
                    "{what}: the launcher of `{}` on node {} has not started launch {launch}",
                    request.segment, request.node
                )));
            }
            request.stop(&dir)?;
            request
        };
        let (segment, node) = (request.segment.clone(), request.node.clone());
        let grace = grace(&dir, &segment)?;
        say(&format!(
            "leave: joiner {id}'s instance of `{segment}` leaves pool `{pool}` on SIGTERM"
        ));
        let bound = Duration::from_secs(grace) + self.waits.drain;
        let mut ended = None;
        self.until(bound, &what, |place| {
            if let Some(done) = request.done(&dir) {
                ended = Some(done);
                return Ok(true);
            }
            if !place.placed(job_id, &what)?.iter().any(|step| step.id == step_id) {
                return Err(Error::new(format!(
                    "{what}: its launcher, step {step_id}, ended before it said the joiner exited, and the joiner ended with it"
                )));
            }
            Ok(false)
        })?;
        let mut registry = Registry::open(&dir, &pool, &self.waits, None)?;
        let before = registry.requests.len();
        registry.requests.retain(|kept| kept.launch != launch);
        if registry.requests.len() != before {
            registry.save()?;
        }
        drop(registry);
        if ended != Some(Done::Exited(0)) {
            return Err(Error::new(format!(
                "{what}: its instance {}, not exited 0, so it may not have drained; see {}",
                ended.map_or_else(|| "ended".to_string(), |done| done.to_string()),
                launcher::log(&dir, job_id, &node, launch).display()
            )));
        }
        say(&format!(
            "leave: joiner {id} exited; node {node} is free for another joiner"
        ));
        Ok(())
    }

    /// The steps of job `id`.
    pub(super) fn placed(&mut self, id: &str, what: &str) -> Result<Vec<Step>, Error> {
        Ok(Step::list(
            &self.run(&Lines::placed_steps(id, self.waits.objects), what)?,
        ))
    }

    /// Wait until `done` says so, for at most `bound`.
    fn until(
        &mut self,
        bound: Duration,
        what: &str,
        mut done: impl FnMut(&mut Self) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        let deadline = Instant::now() + bound;
        loop {
            if done(self)? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::new(format!(
                    "{what}: it still runs {} s after its signal, its stop grace and the drain wait",
                    bound.as_secs()
                )));
            }
            std::thread::sleep(self.waits.interval);
        }
    }
}

fn digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

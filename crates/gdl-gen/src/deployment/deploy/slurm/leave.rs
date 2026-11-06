//! `cargo gdl leave`: a joiner of a running pool on a Slurm site removed
//! cleanly. One `SIGTERM` reaches its instances, which retire through
//! their hub, drain, and exit; then it is waited for until Slurm no longer
//! lists it. A retire through the hub alone would not do: a retired
//! instance holds after its drain until a signal ends it. See
//! `docs/src/guides/slurm.md`.

use std::time::{Duration, Instant};

use super::SlurmPlace;
use super::cli::Lines;
use super::jobs::{self, Job, Kind, Step};
use super::join::{Joiner, grace};
use super::reservations::Registry;
use crate::Error;

impl SlurmPlace<'_> {
    /// Remove `joiner`, which `join` started: a joining job, or a step that
    /// runs a joiner on a spare node of a pool's job. Refused, with nothing
    /// signalled, for any other job or step.
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

    /// Remove step `id`, `<job>.<step>`, which `join --in-job` started in a
    /// pool's job: signal it once, and wait until the job no longer lists
    /// it. Its spare node is then free for another joiner.
    fn leave_step(&mut self, id: &str, say: &mut dyn FnMut(&str)) -> Result<(), Error> {
        let Some((job_id, _)) = id
            .split_once('.')
            .filter(|(job, step)| digits(job) && digits(step))
        else {
            return Err(Error::new(format!(
                "`{id}` is not a step, `<job>.<step>`, as `cargo gdl join --in-job` prints it"
            )));
        };
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
                "job {job_id} is not a pool's job: a step joins a pool in the pool's own job"
            )));
        };
        let what = format!("removing step {id} of pool `{pool}`'s job");
        let listed = self.placed(job_id, &what)?;
        let step = listed
            .iter()
            .find(|step| step.id == id)
            .ok_or_else(|| Error::new(format!("{what}: Slurm lists no step {id}: it has ended")))?;
        let (Some(segment), Some(launch)) = (
            step.joined().map(str::to_string),
            step.launch().map(str::to_string),
        ) else {
            return Err(Error::new(format!(
                "step {id} is `{}`, not a joiner that `cargo gdl join --in-job` started: `cargo gdl down` stops a pool's own processes",
                step.name
            )));
        };
        let node = step.nodes.clone();
        let grace = grace(&self.home.dir.join(&pool), &segment)?;
        let reply = self
            .slurm
            .run(&Lines::terminate(&[id], self.waits.objects))?;
        if !reply.success && self.placed(job_id, &what)?.iter().any(|step| step.id == id) {
            return Err(Error::new(format!(
                "{what}: `scancel --signal=TERM {id}` failed: {}",
                reply.stderr.trim()
            )));
        }
        say(&format!(
            "leave: step {id}'s instance of `{segment}` leaves pool `{pool}` on SIGTERM"
        ));
        let bound = Duration::from_secs(grace) + self.waits.drain;
        self.until(bound, &what, |place| {
            Ok(!place
                .placed(job_id, &what)?
                .iter()
                .any(|step| step.id == id))
        })?;
        // A join that did not see this step start left its launch's hold
        // on the node; another launch's hold is its own.
        let dir = self.home.dir.join(&pool);
        let mut registry = Registry::open(&dir, &pool, &self.waits, None)?;
        let before = registry.held.len();
        registry.held.retain(|held| held.launch != launch);
        if registry.held.len() != before {
            registry.save()?;
        }
        say(&format!(
            "leave: step {id} ended; node {node} is free for another joiner"
        ));
        Ok(())
    }

    /// The steps of job `id`.
    fn placed(&mut self, id: &str, what: &str) -> Result<Vec<Step>, Error> {
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

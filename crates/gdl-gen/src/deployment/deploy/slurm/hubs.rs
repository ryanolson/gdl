//! A pool's hub's routes on a Slurm site, called with `curl` at the
//! addresses the hub wrote ([`address`]): from the login node, or, where
//! it cannot reach the compute nodes, from a step of the pool's job
//! (`srun --jobid <job> --overlap`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use gdl_model::SlurmReach;

use super::super::site::{Answer, HubRoutes};
use super::cli::{Slurm, SlurmCommand};
use crate::Error;

/// A pool's hub's routes, under its extension.
const ROUTES: &str = "/v1/ext/gdl-pool";

/// The longest a route's answer is read, beyond the time `curl` takes to
/// start, and `srun` to start a step.
const DIRECT_MARGIN: Duration = Duration::from_secs(1);
const SRUN_MARGIN: Duration = Duration::from_secs(10);

/// The shortest time a request gets: a wait whose bound has all but run
/// out still asks once.
const SHORTEST: Duration = Duration::from_secs(1);

/// Where one hub listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Address {
    /// Its pool's job.
    pub(super) job: String,
    /// `host:port` of its public listener, and of its control listener.
    pub(super) public: String,
    pub(super) control: String,
}

/// The file the hub of job `job` writes its addresses to, in its pool's
/// directory `dir`.
pub(super) fn file(dir: &Path, job: &str) -> PathBuf {
    dir.join(crate::deployment::slurm::HUB_ADDRESS.replace("$SLURM_JOB_ID", job))
}

/// The addresses the hub of job `job` wrote in `dir`, its pool's directory:
/// `None` until it has written them, as it does before it starts.
pub(super) fn address(dir: &Path, job: &str) -> Result<Option<Address>, Error> {
    let path = file(dir, job);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error::new(format!("{}: {e}", path.display()))),
    };
    let mut words = text.split_whitespace();
    match (words.next(), words.next(), words.next()) {
        (Some(public), Some(control), None) if listener(public) && listener(control) => {
            Ok(Some(Address {
                job: job.to_string(),
                public: public.to_string(),
                control: control.to_string(),
            }))
        }
        _ => Err(Error::new(format!(
            "{} holds `{}`, not the hub's public and control addresses, `<host>:<port> <host>:<port>`",
            path.display(),
            text.trim()
        ))),
    }
}

/// Whether `text` is `host:port`, with a host that a URL takes as it is.
fn listener(text: &str) -> bool {
    text.rsplit_once(':').is_some_and(|(host, port)| {
        !host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '[' | ']' | ':'))
            && port.parse::<u16>().is_ok_and(|port| port > 0)
    })
}

/// The hubs of one Slurm site, each at the address its job's hub wrote.
pub(super) struct Hubs<'a> {
    pub(super) slurm: &'a mut dyn Slurm,
    pub(super) reach: SlurmReach,
    /// Each pool's hub, by its pool.
    pub(super) hubs: &'a BTreeMap<String, Address>,
}

/// Which of a hub's listeners a route is on.
#[derive(Clone, Copy)]
enum Listener {
    Public,
    Control,
}

impl Hubs<'_> {
    /// Call `route` on the hub of `pool`, with `post` for a POST, within
    /// `budget`.
    fn call(
        &mut self,
        pool: &str,
        on: Listener,
        route: &str,
        post: bool,
        budget: Duration,
    ) -> Result<Answer, Error> {
        let Some(hub) = self.hubs.get(pool) else {
            return Ok(Answer::NoHub(format!(
                "pool `{pool}` has no running job whose hub wrote where it listens"
            )));
        };
        let at = match on {
            Listener::Public => &hub.public,
            Listener::Control => &hub.control,
        };
        let budget = budget.max(SHORTEST);
        let mut curl = vec![
            "curl".to_string(),
            "--silent".into(),
            "--show-error".into(),
            "--max-time".into(),
            budget.as_secs().max(1).to_string(),
            "--output".into(),
            "-".into(),
            "--write-out".into(),
            "\\n%{http_code}".into(),
        ];
        if post {
            curl.extend([
                "--request".into(),
                "POST".into(),
                "--data".into(),
                String::new(),
            ]);
        }
        curl.push(format!("http://{at}{ROUTES}{route}"));
        let line = match self.reach {
            SlurmReach::Direct => SlurmCommand {
                program: curl.remove(0),
                args: curl,
                dir: None,
                budget: budget + DIRECT_MARGIN,
            },
            SlurmReach::Srun => SlurmCommand {
                program: "srun".into(),
                args: [
                    format!("--jobid={}", hub.job),
                    "--overlap".into(),
                    "--nodes=1".into(),
                    "--ntasks=1".into(),
                    "--quiet".into(),
                ]
                .into_iter()
                .chain(curl)
                .collect(),
                dir: None,
                budget: budget + SRUN_MARGIN,
            },
        };
        let reply = self.slurm.run(&line)?;
        if !reply.success {
            return Ok(Answer::Failed(reply.stderr));
        }
        // `--write-out` puts the status on a line of its own, after the body.
        let (body, status) = reply
            .stdout
            .rsplit_once('\n')
            .unwrap_or(("", reply.stdout.as_str()));
        Ok(match status.trim().parse::<u16>() {
            Ok(200..=299) => Answer::Body(body.to_string()),
            Ok(404) => Answer::NotFound,
            Ok(status) => Answer::Failed(format!("HTTP {status}: {}", body.trim())),
            Err(_) => Answer::Failed(format!("`curl` printed no status: {}", reply.stdout)),
        })
    }
}

impl Hubs<'_> {
    /// `POST /v1/ext/gdl-pool/children/{class}/{pool}/withdraw` on
    /// `parent`'s hub's control port: it writes the withdrawal of `pool`,
    /// whose hub is gone, past its last record. 2xx once stored; 404 when
    /// the parent holds no record of it.
    pub(super) fn withdraw(
        &mut self,
        parent: &str,
        class: &str,
        pool: &str,
        budget: Duration,
    ) -> Result<Answer, Error> {
        let route = format!("/children/{class}/{pool}/withdraw");
        self.call(parent, Listener::Control, &route, true, budget)
    }
}

impl HubRoutes for Hubs<'_> {
    fn drain(&mut self, pool: &str, budget: Duration) -> Result<(), Error> {
        match self.call(pool, Listener::Control, "/drain", true, budget)? {
            Answer::Body(_) => Ok(()),
            Answer::NotFound => Err(Error::new(format!(
                "draining pool `{pool}`: its hub has no drain route"
            ))),
            Answer::NoHub(why) | Answer::Failed(why) => Err(Error::new(format!(
                "draining pool `{pool}`: {}",
                why.trim()
            ))),
        }
    }

    fn health(&mut self, pool: &str, budget: Duration) -> Result<bool, Error> {
        Ok(matches!(
            self.call(pool, Listener::Public, "/health", false, budget)?,
            Answer::Body(_)
        ))
    }

    fn withdrawn(&mut self, pool: &str, budget: Duration) -> Result<Answer, Error> {
        self.call(pool, Listener::Public, "/withdrawn", false, budget)
    }

    fn instances(&mut self, pool: &str, budget: Duration) -> Result<Answer, Error> {
        self.call(pool, Listener::Control, "/instances", false, budget)
    }

    fn report(
        &mut self,
        parent: &str,
        class: &str,
        pool: &str,
        budget: Duration,
    ) -> Result<Answer, Error> {
        let route = format!("/report/{class}/{pool}");
        self.call(parent, Listener::Public, &route, false, budget)
    }
}

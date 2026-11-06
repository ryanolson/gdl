//! A pool's hub's routes, through the API server's service proxy: a hub's
//! Service is named for its pool.

use std::time::Duration;

use super::super::site::{Answer, HubRoutes, Stopping, Withdrawal};
use super::kube::{At, CONTROL, Kube, PUBLIC, bounded};
use super::{Waits, run};
use crate::Error;

/// A pool's hub's routes, as the service proxy reaches them.
const HEALTH: &str = "/v1/ext/gdl-pool/health";
const DRAIN: &str = "/v1/ext/gdl-pool/drain";
const WITHDRAWN: &str = "/v1/ext/gdl-pool/withdrawn";
const INSTANCES: &str = "/v1/ext/gdl-pool/instances";

/// The hubs of one cluster's namespace, through its kube context.
pub(super) struct Proxy<'a> {
    pub(super) kube: &'a mut dyn Kube,
    pub(super) at: At<'a>,
}

impl HubRoutes for Proxy<'_> {
    fn drain(&mut self, pool: &str, budget: Duration) -> Result<(), Error> {
        run(
            self.kube,
            self.at.post(pool, CONTROL, DRAIN),
            budget,
            Some(b""),
            &format!("draining pool `{pool}`"),
        )
        .map(drop)
    }

    fn health(&mut self, pool: &str, budget: Duration) -> Result<bool, Error> {
        let args = bounded(self.at.get(pool, PUBLIC, HEALTH), budget);
        Ok(self.kube.run(&args, None)?.success)
    }

    fn withdrawn(&mut self, pool: &str, budget: Duration) -> Result<Answer, Error> {
        let args = bounded(self.at.get(pool, PUBLIC, WITHDRAWN), budget);
        let reply = self.kube.run(&args, None)?;
        Ok(match reply.success {
            true => Answer::Body(reply.stdout),
            false => Answer::Failed(reply.stderr),
        })
    }

    fn instances(&mut self, pool: &str, budget: Duration) -> Result<Answer, Error> {
        let args = bounded(self.at.get(pool, CONTROL, INSTANCES), budget);
        let reply = self.kube.run(&args, None)?;
        Ok(match reply.success {
            true => Answer::Body(reply.stdout),
            false => Answer::Failed(reply.stderr),
        })
    }

    fn report(
        &mut self,
        parent: &str,
        class: &str,
        pool: &str,
        budget: Duration,
    ) -> Result<Answer, Error> {
        let route = format!("/v1/ext/gdl-pool/report/{class}/{pool}");
        let args = bounded(self.at.get(parent, PUBLIC, &route), budget);
        let reply = self.kube.run(&args, None)?;
        // The API server's own 404 for a Service that does not exist names
        // the Service, and proves nothing about what the Service serves.
        Ok(if reply.proxied_not_found() {
            Answer::NotFound
        } else if reply.no_service(parent) {
            Answer::NoHub(format!(
                "the parent's Service does not exist: {}",
                reply.stderr.trim()
            ))
        } else if reply.success {
            Answer::Body(reply.stdout)
        } else {
            Answer::Failed(reply.stderr)
        })
    }
}

/// Refuse to delete `pool`, whose hub's Deployment is gone, unless its
/// parent's hub answers 404 for its record: with no hub, nothing can drain
/// it, and a record its parent still holds is offered nothing, but waits
/// for an operator to withdraw it.
pub(super) fn withdrawn_without_hub(
    kube: &mut dyn Kube,
    at: At<'_>,
    pool: &Stopping,
    waits: &Waits,
) -> Result<(), Error> {
    // Under a shared directory no parent's hub holds the record, so
    // nothing here can show that the pool withdrew.
    if let Withdrawal::Shared { directory } = pool.seen {
        return Err(Error::new(format!(
            "pool `{}` is not known to have withdrawn from the {directory} directory, so nothing is deleted: its hub's Deployment is gone, so it can neither drain nor say that its record is withdrawn, and no parent's hub holds its record. Withdraw it on a parent's hub (`POST /v1/ext/gdl-pool/children/{}/{}/withdraw` on the control port), which answers 204 once the directory has stored it; then delete its objects (`kubectl --context {} --namespace {} delete deployments,services,configmaps -l gdl.dev/pool={}`) and deploy again",
            pool.pool, pool.class, pool.pool, at.context, at.namespace, pool.pool
        )));
    }
    let home = pool.relay_home()?;
    let route = format!("/v1/ext/gdl-pool/report/{}/{}", pool.class, pool.pool);
    let reply = kube.run(&bounded(at.get(home, PUBLIC, &route), waits.objects), None)?;
    if reply.proxied_not_found() {
        return Ok(());
    }
    let why = if reply.success {
        let withdraw = format!(
            "/api/v1/namespaces/{}/services/http:{}:{CONTROL}/proxy/v1/ext/gdl-pool/children/{}/{}/withdraw",
            at.namespace, home, pool.class, pool.pool
        );
        format!(
            "its hub's Deployment is gone, so it cannot drain, and its parent still holds its record. Withdraw it on the parent's hub (`POST /v1/ext/gdl-pool/children/{}/{}/withdraw` on the control port), for example with `kubectl --context {} create --raw {withdraw} -f /dev/null`, then deploy again",
            pool.class, pool.pool, at.context
        )
    } else {
        format!(
            "its hub's Deployment is gone, and its parent's hub does not answer: {}",
            reply.stderr.trim()
        )
    };
    Err(Error::new(format!(
        "pool `{}` is not known to have withdrawn from its parent `{home}`, so nothing is deleted: {why}",
        pool.pool
    )))
}

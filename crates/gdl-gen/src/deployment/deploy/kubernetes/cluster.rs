//! The clusters of a fleet as `deploy` and `down` reach them: each one's
//! kube context and namespace, the selectors of the fleet's objects
//! there, and what the cluster has.

use std::time::Duration;

use gdl_model::Fleet;
use serde_json::Value;

use super::apply::Objects;
use super::kube::{At, DGDS, GDL, Kube, MONITORING, PODMONITORS, POOLS, SCALERS, bounded};
use super::live::Live;
use super::{CLUSTER_LABEL, FLEET_LABEL, label_value, run};
use crate::Error;

/// Each cluster of a fleet, in name order, and the fleet's label there.
pub(super) struct Clusters(pub(super) Vec<Cluster>);

impl Clusters {
    pub(super) fn of(fleet: &Fleet) -> Result<Self, Error> {
        let fleet_name = fleet.name().ok_or_else(|| {
            Error::new("the fleet has no `[fleet] name`, which labels its objects")
        })?;
        let mut clusters = Vec::new();
        for (cluster, settings) in fleet.clusters() {
            // The driver gives Kubernetes only a fleet whose clusters are
            // all Kubernetes clusters.
            let Some(settings) = settings.kubernetes() else {
                return Err(Error::new(format!(
                    "[cluster.{cluster}]: a Slurm site, which the Kubernetes site does not run"
                )));
            };
            clusters.push(Cluster {
                name: cluster.to_string(),
                context: settings.context.clone(),
                namespace: settings.namespace.clone(),
                selector: format!(
                    "{FLEET_LABEL}={},{CLUSTER_LABEL}={}",
                    label_value(fleet_name),
                    label_value(cluster)
                ),
                unowned: format!("{FLEET_LABEL}={},!{CLUSTER_LABEL}", label_value(fleet_name)),
                monitors: false,
                operator: settings.operator,
                scalers: false,
                pools: false,
            });
        }
        Ok(Self(clusters))
    }

    pub(super) fn get(&self, name: &str) -> &Cluster {
        self.0
            .iter()
            .find(|cluster| cluster.name == name)
            .expect("a cluster of the fleet")
    }
}

/// One cluster of a fleet.
pub(super) struct Cluster {
    pub(super) name: String,
    pub(super) context: String,
    pub(super) namespace: String,
    /// The selector of the fleet's objects that were rendered for this
    /// cluster: another cluster's, in a namespace that two contexts reach,
    /// are not this cluster's to prune.
    pub(super) selector: String,
    /// The selector of the fleet's objects that name no cluster.
    pub(super) unowned: String,
    /// Whether the cluster has the Prometheus operator's PodMonitor CRD:
    /// only then are the fleet's PodMonitors listed, pruned, and deleted,
    /// whatever the fleet asks now, so that one it no longer asks for goes.
    pub(super) monitors: bool,
    /// Whether the fleet asks the gdl operator to scale the cluster's
    /// segments (`[cluster.<name>] operator`).
    pub(super) operator: bool,
    /// Whether the cluster has the gdl operator's `GdlScaler` CRD: only
    /// then are the fleet's scalers listed, pruned, and deleted, so that a
    /// cluster that no longer asks for them loses them.
    pub(super) scalers: bool,
    /// Whether the cluster has the gdl operator's `GdlPool` CRD: only then
    /// are the fleet's `GdlPool`s listed there.
    pub(super) pools: bool,
}

impl Cluster {
    pub(super) fn at(&self) -> At<'_> {
        At {
            context: &self.context,
            namespace: &self.namespace,
        }
    }

    /// Refuse a cluster whose context cannot be reached, or may not create
    /// Deployments in its namespace.
    pub(super) fn check(&self, kube: &mut dyn Kube, budget: Duration) -> Result<(), Error> {
        let reply = kube.run(&bounded(self.at().can_i(), budget), None)?;
        match reply.stdout.trim() {
            "yes" => Ok(()),
            "no" => Err(Error::new(format!(
                "cluster `{}`: context `{}` may not create Deployments in namespace `{}`",
                self.name, self.context, self.namespace
            ))),
            _ => Err(Error::new(format!(
                "cluster `{}`: context `{}` does not answer: {}",
                self.name,
                self.context,
                reply.stderr.trim()
            ))),
        }
    }

    /// Ask the cluster, once, whether it has the PodMonitor CRD.
    pub(super) fn find_monitors(
        &mut self,
        kube: &mut dyn Kube,
        budget: Duration,
    ) -> Result<(), Error> {
        self.monitors = self.discover(kube, MONITORING, PODMONITORS, budget)?;
        Ok(())
    }

    /// Ask the cluster, once, whether it has the gdl operator's
    /// `GdlScaler` and `GdlPool` CRDs.
    pub(super) fn find_operator(
        &mut self,
        kube: &mut dyn Kube,
        budget: Duration,
    ) -> Result<(), Error> {
        self.scalers = self.discover(kube, GDL, SCALERS, budget)?;
        self.pools = self.discover(kube, GDL, POOLS, budget)?;
        Ok(())
    }

    /// Whether the cluster serves dynamo's DGDs at the version the gdl
    /// operator asks for: its group version is served, and lists
    /// `dynamographdeployments`, as the operator itself asks.
    pub(super) fn serves_dgds(&self, kube: &mut dyn Kube, budget: Duration) -> Result<bool, Error> {
        let args = bounded(self.at().served(gdl_k8s::DGD_API_VERSION), budget);
        let reply = kube.run(&args, None)?;
        if !reply.success {
            if reply.stderr.contains("NotFound") {
                return Ok(false);
            }
            return Err(Error::new(format!(
                "cluster `{}`: asking whether it has `{DGDS}`: `kubectl {}` failed: {}",
                self.name,
                args.join(" "),
                reply.stderr.trim()
            )));
        }
        let list: Value = serde_json::from_str(&reply.stdout).map_err(|e| {
            Error::new(format!(
                "cluster `{}`: the resources of `{}` are not JSON: {e}",
                self.name,
                gdl_k8s::DGD_API_VERSION
            ))
        })?;
        let plural = DGDS.split('.').next().unwrap_or_default();
        Ok(list["resources"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|resource| resource["name"] == plural))
    }

    /// Whether the cluster serves `resource` of API group `group`.
    /// `kubectl api-resources` discovers every API group before it filters
    /// by one, and exits 1 when another group's discovery fails, as a
    /// stale metrics server's does, after printing what it found: what it
    /// printed answers unless the failed groups include `group`. When they
    /// do, nothing it printed answers, a resource listed from another of
    /// the group's versions included: the run stops.
    fn discover(
        &self,
        kube: &mut dyn Kube,
        group: &str,
        resource: &str,
        budget: Duration,
    ) -> Result<bool, Error> {
        let args = bounded(self.at().resources(group), budget);
        let reply = kube.run(&args, None)?;
        let found = reply.stdout.lines().any(|line| line.trim() == resource);
        let others_only = reply
            .stderr
            .contains("unable to retrieve the complete list of server APIs")
            && !reply.stderr.contains(group);
        if !reply.success && !others_only {
            return Err(Error::new(format!(
                "cluster `{}`: asking whether it has `{resource}`: `kubectl {}` failed: {}",
                self.name,
                args.join(" "),
                reply.stderr.trim()
            )));
        }
        Ok(found)
    }

    /// Create the cluster's namespace where none exists, or add the
    /// fleet's labels to the one that does. The Namespace is never
    /// applied: a server-side apply of a namespace that its owner made
    /// with a client-side apply takes over the fields that apply set, and
    /// drops those the render does not list. A create fails on a
    /// namespace that another made after the question, which is then
    /// labelled as one that existed. Any other failure stops the deploy.
    pub(super) fn prepare_namespace(
        &self,
        kube: &mut dyn Kube,
        objects: &Objects,
        budget: Duration,
    ) -> Result<(), Error> {
        if !self.namespace_exists(kube, budget)? {
            let args = bounded(self.at().create(), budget);
            let reply = kube.run(&args, Some(objects.namespace.as_bytes()))?;
            if reply.success {
                return Ok(());
            }
            if !reply.stderr.contains("(AlreadyExists)") {
                return Err(Error::new(format!(
                    "cluster `{}`: creating the namespace: `kubectl {}` failed: {}",
                    self.name,
                    args.join(" "),
                    reply.stderr.trim()
                )));
            }
        }
        run(
            kube,
            self.at().label(&objects.labels),
            budget,
            None,
            "adding the fleet's labels to the namespace",
        )?;
        Ok(())
    }

    /// Whether the cluster's namespace exists. A question that fails
    /// stops the deploy.
    fn namespace_exists(&self, kube: &mut dyn Kube, budget: Duration) -> Result<bool, Error> {
        let reply = run(
            kube,
            self.at().namespace(),
            budget,
            None,
            &format!(
                "cluster `{}`: asking whether the namespace exists",
                self.name
            ),
        )?;
        Ok(!reply.stdout.trim().is_empty())
    }

    /// The fleet's objects in the cluster's namespace that were rendered
    /// for this cluster. Refused when the namespace holds an object of the
    /// fleet that names no cluster, as a render before `gdl.dev/cluster`
    /// wrote them: it may be another cluster's, where two contexts reach
    /// one namespace, so it is neither this cluster's to delete nor one to
    /// leave running unseen.
    pub(super) fn live(&self, kube: &mut dyn Kube, budget: Duration) -> Result<Vec<Live>, Error> {
        let reply = run(
            kube,
            self.at().list(&self.unowned, self.monitors, self.scalers),
            budget,
            None,
            "listing the fleet's objects that name no cluster",
        )?;
        let unowned = Live::list(&reply.stdout)
            .map_err(|e| Error::new(format!("cluster `{}`: {e}", self.name)))?;
        if !unowned.is_empty() {
            let objects: Vec<String> = unowned.iter().map(Live::reference).collect();
            return Err(Error::new(format!(
                "cluster `{}`: namespace `{}` holds objects of the fleet that carry no `{CLUSTER_LABEL}` label, so it is not known which cluster's they are: {}. Label the ones that are this cluster's with `kubectl --context {} --namespace {} label {} {CLUSTER_LABEL}={}`, or delete them, then run again",
                self.name,
                self.namespace,
                objects.join(", "),
                self.context,
                self.namespace,
                objects.join(" "),
                label_value(&self.name)
            )));
        }
        let reply = run(
            kube,
            self.at().list(&self.selector, self.monitors, self.scalers),
            budget,
            None,
            "listing the fleet's objects",
        )?;
        // What the gdl operator made is its own: only it changes or
        // deletes it.
        let live = Live::list(&reply.stdout)
            .map_err(|e| Error::new(format!("cluster `{}`: {e}", self.name)))?;
        Ok(live.into_iter().filter(|object| !object.operated).collect())
    }
}

/// The pods of `pool` that are not ready, each with why its containers
/// wait and its latest events: these name a missing Secret or image, and
/// no Secret is read.
pub(super) fn why_not_ready(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    pool: &str,
    budget: Duration,
) -> Result<Vec<String>, Error> {
    /// The events shown for each pod: the latest.
    const EVENTS: usize = 5;
    let selector = format!("{},gdl.dev/pool={}", cluster.selector, label_value(pool));
    let at = cluster.at();
    let reply = run(
        kube,
        at.pods(&selector),
        budget,
        None,
        "listing the pool's pods",
    )?;
    let pods: Value = serde_json::from_str(&reply.stdout)
        .map_err(|e| Error::new(format!("`kubectl get pods` printed no JSON list: {e}")))?;
    let mut lines = Vec::new();
    for pod in pods["items"].as_array().into_iter().flatten() {
        let ready = pod["status"]["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|c| c["type"] == "Ready" && c["status"] == "True");
        if ready {
            continue;
        }
        let name = pod["metadata"]["name"].as_str().unwrap_or_default();
        let mut line = format!(
            "pod `{name}`: {}",
            pod["status"]["phase"].as_str().unwrap_or("Unknown")
        );
        for status in pod["status"]["containerStatuses"]
            .as_array()
            .into_iter()
            .flatten()
        {
            for (state, what) in [("waiting", "waiting"), ("terminated", "terminated")] {
                let found = &status["state"][state];
                if let Some(reason) = found["reason"].as_str() {
                    line.push_str(&format!(", {what}: {reason}"));
                    if let Some(message) = found["message"].as_str() {
                        line.push_str(&format!(" ({message})"));
                    }
                }
            }
        }
        lines.push(line);
        let reply = run(
            kube,
            at.events(name),
            budget,
            None,
            "listing the pod's events",
        )?;
        let events: Value = serde_json::from_str(&reply.stdout)
            .map_err(|e| Error::new(format!("`kubectl get events` printed no JSON list: {e}")))?;
        let events = events["items"].as_array().cloned().unwrap_or_default();
        for event in events.iter().rev().take(EVENTS).rev() {
            lines.push(format!(
                "  {} {}: {}",
                event["type"].as_str().unwrap_or_default(),
                event["reason"].as_str().unwrap_or_default(),
                event["message"].as_str().unwrap_or_default().trim()
            ));
        }
    }
    Ok(lines)
}

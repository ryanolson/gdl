//! The apply of a cluster's pools where the gdl operator scales the
//! segments (`[cluster.<name>] scaler`). Two rules hold there.
//!
//! `gdl` never writes `replicas`. The render leaves it out of each segment
//! Deployment and each `GdlScaler`, and a one-field apply under a manager
//! of its own writes it once: `gdl-scaler` on the Deployment, which the
//! scaler then writes by forced apply, and `gdl-bootstrap` on the scaler,
//! whose field an HPA's or a person's `/scale` then takes over. The
//! bootstrap runs only while no manager but `gdl` owns the field, carries
//! the uid and resourceVersion it read, and is never forced, so a stale
//! one is refused rather than undoing a scale. A Deployment whose
//! `replicas` `gdl` owns from an apply before the operator is
//! bootstrapped with its live count before the apply that leaves the field
//! out, which would otherwise drop it to the default of 1.
//!
//! An entry Service selects on `gdl.dev/routable` only once every ready
//! pod it would select carries the label: a selector that named it while
//! old pods lacked it would leave them out of the Service. So the first
//! deploy that adds the label to the pods leaves the Service's selector as
//! it is, and a later one adds it. See `docs/src/guides/operator.md`.

use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::{Cluster, Kube, Waits, run};
use crate::Error;
use crate::deployment::k8s::ROUTABLE;

/// The manager that owns a segment Deployment's `replicas` from its
/// bootstrap, and under which the operator writes it.
const SCALER_MANAGER: &str = "gdl-scaler";

/// The manager of a `GdlScaler`'s `replicas` until its `/scale` is first
/// written.
const BOOTSTRAP_MANAGER: &str = "gdl-bootstrap";

/// The manager of every apply of `deploy`'s own.
const GDL_MANAGER: &str = "gdl";

/// Apply `pools`, the YAML stream of `cluster`'s pools, where the
/// operator scales the segments: the uid of each object applied.
pub(super) fn apply(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    pools: &str,
    waits: &Waits,
    say: &mut dyn FnMut(&str),
) -> Result<Vec<String>, Error> {
    let mut objects = documents(pools)?;
    let scaled: Vec<(String, i64)> = objects
        .iter()
        .filter(|object| object["kind"] == "GdlScaler")
        .map(|object| {
            let name = object["metadata"]["name"].as_str().unwrap_or_default();
            let quorum = object["spec"]["minReplicas"].as_i64().unwrap_or(1);
            (name.to_string(), quorum)
        })
        .collect();
    // An object that exists is bootstrapped first, while `gdl` may still
    // own its `replicas`; a new one, once the apply has made it.
    bootstrap_all(kube, cluster, &scaled, waits)?;
    for service in objects
        .iter_mut()
        .filter(|object| object["kind"] == "Service")
    {
        if service["spec"]["selector"].get(ROUTABLE).is_some()
            && !routable_now(kube, cluster, service, waits)?
        {
            if let Some(selector) = service["spec"]["selector"].as_object_mut() {
                selector.remove(ROUTABLE);
            }
            say(&format!(
                "apply: Service `{}` in cluster `{}` selects on `{ROUTABLE}` only once every ready pod of its segment carries it: deploy again once the segment's rollout has ended",
                service["metadata"]["name"].as_str().unwrap_or_default(),
                cluster.name
            ));
        }
    }
    let text = crate::deployment::k8s::stream(&objects).map_err(crate::deployment::io)?;
    let applied = super::apply(kube, cluster, &text, waits.objects)?;
    bootstrap_all(kube, cluster, &scaled, waits)?;
    Ok(applied)
}

/// Bootstrap the `replicas` of each of `scaled`, a scaler's name and its
/// quorum: its Deployment's, then its own.
fn bootstrap_all(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    scaled: &[(String, i64)],
    waits: &Waits,
) -> Result<(), Error> {
    for (name, quorum) in scaled {
        let deployment = format!("deployments.apps/{name}");
        bootstrap(kube, cluster, &deployment, SCALER_MANAGER, *quorum, waits)?;
        let scaler = format!("{}/{name}", super::SCALERS);
        bootstrap(kube, cluster, &scaler, BOOTSTRAP_MANAGER, *quorum, waits)?;
    }
    Ok(())
}

/// Write `reference`'s `replicas` under `manager`, once: only while no
/// manager but `gdl` owns the field, to its live value when `gdl` owns it
/// and to `quorum` when none does. Nothing when the object does not exist.
/// The apply carries the uid and resourceVersion it read and is never
/// forced, so a write in between, such as a scale, refuses it: it then
/// reads the object again and decides again, within the objects bound.
fn bootstrap(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    reference: &str,
    manager: &str,
    quorum: i64,
    waits: &Waits,
) -> Result<(), Error> {
    let at = cluster.at();
    let deadline = Instant::now() + waits.objects;
    loop {
        let left = deadline
            .saturating_duration_since(Instant::now())
            .max(Duration::from_secs(1));
        let reply = run(
            kube,
            at.object(reference),
            left,
            None,
            &format!("reading {reference} in cluster `{}`", cluster.name),
        )?;
        if reply.stdout.trim().is_empty() {
            return Ok(());
        }
        let live: Value = serde_json::from_str(&reply.stdout)
            .map_err(|e| Error::new(format!("`kubectl get {reference}` printed no JSON: {e}")))?;
        let owners = replica_owners(&live);
        if owners.iter().any(|owner| owner != GDL_MANAGER) {
            return Ok(());
        }
        let replicas = match owners.is_empty() {
            true => quorum,
            false => live["spec"]["replicas"].as_i64().unwrap_or(quorum),
        };
        let metadata = &live["metadata"];
        let patch = json!({
            "apiVersion": live["apiVersion"],
            "kind": live["kind"],
            "metadata": {
                "name": metadata["name"],
                "namespace": metadata["namespace"],
                "uid": metadata["uid"],
                "resourceVersion": metadata["resourceVersion"],
            },
            "spec": {"replicas": replicas},
        });
        let args = super::bounded(at.apply_as(manager), left);
        let reply = kube.run(&args, Some(patch.to_string().as_bytes()))?;
        if reply.success {
            return Ok(());
        }
        let conflict = reply.stderr.contains("the object has been modified");
        if !conflict || Instant::now() >= deadline {
            return Err(Error::new(format!(
                "cluster `{}`: writing {reference}'s replicas under `{manager}`: `kubectl {}` failed: {}",
                cluster.name,
                args.join(" "),
                reply.stderr.trim()
            )));
        }
        std::thread::sleep(waits.interval.min(Duration::from_millis(200)));
    }
}

/// The managers that own `object`'s `spec.replicas`, by its
/// `managedFields`.
fn replica_owners(object: &Value) -> Vec<String> {
    object["metadata"]["managedFields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry["fieldsV1"]["f:spec"].get("f:replicas").is_some())
        .filter_map(|entry| entry["manager"].as_str().map(str::to_string))
        .collect()
}

/// Whether `service`, as rendered with `gdl.dev/routable` in its
/// selector, may be applied so: its live selector names the label
/// already, or every ready pod that its selector without the label picks
/// carries the label.
fn routable_now(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    service: &Value,
    waits: &Waits,
) -> Result<bool, Error> {
    let name = service["metadata"]["name"].as_str().unwrap_or_default();
    let at = cluster.at();
    let reply = run(
        kube,
        at.object(&format!("services/{name}")),
        waits.objects,
        None,
        &format!("reading Service `{name}` in cluster `{}`", cluster.name),
    )?;
    if !reply.stdout.trim().is_empty() {
        let live: Value = serde_json::from_str(&reply.stdout).map_err(|e| {
            Error::new(format!(
                "`kubectl get services/{name}` printed no JSON: {e}"
            ))
        })?;
        if live["spec"]["selector"].get(ROUTABLE).is_some() {
            return Ok(true);
        }
    }
    let selector: Vec<String> = service["spec"]["selector"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| key.as_str() != ROUTABLE)
        .map(|(key, value)| format!("{key}={}", value.as_str().unwrap_or_default()))
        .collect();
    let reply = run(
        kube,
        at.pods(&selector.join(",")),
        waits.objects,
        None,
        &format!(
            "listing Service `{name}`'s pods in cluster `{}`",
            cluster.name
        ),
    )?;
    let pods: Value = serde_json::from_str(&reply.stdout)
        .map_err(|e| Error::new(format!("`kubectl get pods` printed no JSON list: {e}")))?;
    Ok(pods["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|pod| {
            pod["metadata"]["deletionTimestamp"].is_null()
                && pod["status"]["conditions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|c| c["type"] == "Ready" && c["status"] == "True")
        })
        .all(|pod| pod["metadata"]["labels"].get(ROUTABLE).is_some()))
}

/// The objects of `text`, a YAML stream of JSON documents.
fn documents(text: &str) -> Result<Vec<Value>, Error> {
    text.split("---\n")
        .filter(|document| !document.trim().is_empty())
        .map(|document| {
            serde_json::from_str(document)
                .map_err(|e| Error::new(format!("a rendered object is not JSON: {e}")))
        })
        .collect()
}

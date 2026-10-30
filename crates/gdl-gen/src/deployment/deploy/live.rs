//! The fleet's objects as a cluster holds them: what `deploy` prunes, and
//! `down` deletes. Each is read for its kind, its name, and the labels the
//! render gives it, by their whole values ([`super::super::k8s`] labels a
//! long value by its start and a hash, and keeps it whole in an
//! annotation). A hub's Deployment is also read for the pool's parent and
//! its segments' Deployments for how long their pods may take to stop:
//! nothing else is read.

use serde_json::Value;

use crate::Error;

/// One object of the fleet in a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Live {
    pub(super) kind: String,
    pub(super) name: String,
    /// `gdl.dev/pool`: the pool it is of, for a pool's object.
    pub(super) pool: Option<String>,
    /// `gdl.dev/class`.
    pub(super) class: Option<String>,
    /// `gdl.dev/segment`: `hub` for a hub's.
    pub(super) segment: Option<String>,
    /// For a hub's Deployment, the pool's parent (`GDL_POOL__HOME`): the
    /// pool whose hub holds its record.
    pub(super) home: Option<String>,
    /// For a Deployment, its pods' stop grace, in seconds.
    pub(super) grace: Option<u64>,
}

impl Live {
    /// The objects of a `kubectl get --output json` list.
    pub(super) fn list(text: &str) -> Result<Vec<Self>, Error> {
        let list: Value = serde_json::from_str(text)
            .map_err(|e| Error::new(format!("`kubectl get` printed no JSON list: {e}")))?;
        list["items"]
            .as_array()
            .ok_or_else(|| Error::new("`kubectl get` printed a list with no items"))?
            .iter()
            .map(Self::of)
            .collect()
    }

    fn of(object: &Value) -> Result<Self, Error> {
        let text = |value: &Value| value.as_str().map(str::to_string);
        let kind = text(&object["kind"])
            .ok_or_else(|| Error::new("`kubectl get` listed an object with no kind"))?;
        let metadata = &object["metadata"];
        let name = text(&metadata["name"])
            .ok_or_else(|| Error::new(format!("`kubectl get` listed a {kind} with no name")))?;
        // A label's whole value: in the annotation of its key when the
        // label holds a shortened one.
        let whole = |key: &str| {
            text(&metadata["annotations"][key]).or_else(|| text(&metadata["labels"][key]))
        };
        let pod = &object["spec"]["template"]["spec"];
        let home = pod["containers"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|container| container["env"].as_array().into_iter().flatten())
            .find(|var| var["name"] == "GDL_POOL__HOME")
            .and_then(|var| text(&var["value"]));
        Ok(Self {
            pool: whole("gdl.dev/pool"),
            class: whole("gdl.dev/class"),
            segment: whole("gdl.dev/segment"),
            home: (kind == "Deployment").then_some(home).flatten(),
            grace: pod["terminationGracePeriodSeconds"].as_u64(),
            kind,
            name,
        })
    }

    /// The object as `kubectl delete` takes it: `<kind>/<name>`.
    pub(super) fn reference(&self) -> String {
        format!("{}/{}", self.kind.to_ascii_lowercase(), self.name)
    }

    /// Whether it is a pool's hub's Deployment.
    pub(super) fn is_hub(&self) -> bool {
        self.kind == "Deployment" && self.segment.as_deref() == Some("hub")
    }

    /// Whether it is a Deployment of a pool's segment.
    pub(super) fn is_segment(&self) -> bool {
        self.kind == "Deployment" && !self.is_hub()
    }
}

/// A pool that `deploy` removes or `down` stops, as its cluster holds it:
/// drained first when it has a parent, since the drain is what withdraws
/// its record from its parent, and a pool deleted without it stays held
/// there, missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Stopping {
    pub(super) pool: String,
    pub(super) class: String,
    /// Its parent pool, whose hub's Service is named for it.
    pub(super) home: String,
    /// The longest stop grace of its Deployments, in seconds.
    pub(super) grace: u64,
}

impl Stopping {
    /// The pools of `objects` with a hub's Deployment there and a parent,
    /// in name order.
    pub(super) fn of(objects: &[&Live]) -> Vec<Self> {
        let mut pools: Vec<Self> = objects
            .iter()
            .filter(|object| object.is_hub())
            .filter_map(|hub| {
                let pool = hub.pool.clone()?;
                let grace = objects
                    .iter()
                    .filter(|object| object.kind == "Deployment")
                    .filter(|object| object.pool.as_deref() == Some(pool.as_str()))
                    .filter_map(|object| object.grace)
                    .max()
                    .unwrap_or_default();
                Some(Self {
                    class: hub.class.clone()?,
                    home: hub.home.clone()?,
                    pool,
                    grace,
                })
            })
            .collect();
        pools.sort_by(|a, b| a.pool.cmp(&b.pool));
        pools
    }
}

//! The fleet's objects as a cluster holds them: what `deploy` prunes, and
//! `down` deletes. Each is read for its kind, its name, its uid, and the labels the
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
    /// The cluster's own name for it, which no other object of the
    /// cluster has.
    pub(super) uid: String,
    /// `gdl.dev/pool`: the pool it is of, for a pool's object.
    pub(super) pool: Option<String>,
    /// `gdl.dev/class`.
    pub(super) class: Option<String>,
    /// `gdl.dev/segment`: `hub` for a hub's.
    pub(super) segment: Option<String>,
    /// For a hub's Deployment, the pool's parent (`GDL_POOL__HOME`): the
    /// pool whose hub holds its record.
    pub(super) home: Option<String>,
    /// For a pool's Deployment, its parents' class (`GDL_POOL__PARENTS`),
    /// for a shared pool.
    pub(super) parents: Option<String>,
    /// For a Deployment, its pods' stop grace, in seconds.
    pub(super) grace: Option<u64>,
    /// For a pool's Deployment, whether its pool publishes to the DynamoDB
    /// directory (`GDL_POOL__DIRECTORY=dynamodb`).
    pub(super) dynamodb: bool,
}

impl Live {
    /// The objects of a `kubectl get --output json` list, or of what
    /// `kubectl apply --output json` printed: one object, or a list.
    pub(super) fn list(text: &str) -> Result<Vec<Self>, Error> {
        let list: Value = serde_json::from_str(text)
            .map_err(|e| Error::new(format!("`kubectl` printed no JSON: {e}")))?;
        if list.get("items").is_none() {
            return Self::of(&list).map(|object| vec![object]);
        }
        list["items"]
            .as_array()
            .ok_or_else(|| Error::new("`kubectl` printed a list with no items"))?
            .iter()
            .map(Self::of)
            .collect()
    }

    fn of(object: &Value) -> Result<Self, Error> {
        let text = |value: &Value| value.as_str().map(str::to_string);
        let kind = text(&object["kind"])
            .ok_or_else(|| Error::new("`kubectl` printed an object with no kind"))?;
        let metadata = &object["metadata"];
        let name = text(&metadata["name"])
            .ok_or_else(|| Error::new(format!("`kubectl` printed a {kind} with no name")))?;
        let uid = text(&metadata["uid"])
            .ok_or_else(|| Error::new(format!("`kubectl` printed {kind} `{name}` with no uid")))?;
        // A label's whole value: in the annotation of its key when the
        // label holds a shortened one.
        let whole = |key: &str| {
            text(&metadata["annotations"][key]).or_else(|| text(&metadata["labels"][key]))
        };
        let pod = &object["spec"]["template"]["spec"];
        let variable = |name: &str| {
            pod["containers"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|container| container["env"].as_array().into_iter().flatten())
                .find(|var| var["name"] == name)
                .and_then(|var| text(&var["value"]))
        };
        let home = variable("GDL_POOL__HOME");
        let parents = variable("GDL_POOL__PARENTS");
        let dynamodb = variable("GDL_POOL__DIRECTORY").as_deref() == Some("dynamodb");
        Ok(Self {
            pool: whole("gdl.dev/pool"),
            class: whole("gdl.dev/class"),
            segment: whole("gdl.dev/segment"),
            home: (kind == "Deployment").then_some(home).flatten(),
            parents: (kind == "Deployment").then_some(parents).flatten(),
            grace: pod["terminationGracePeriodSeconds"].as_u64(),
            dynamodb: kind == "Deployment" && dynamodb,
            kind,
            name,
            uid,
        })
    }

    /// The object as `kubectl delete` takes it: `<resource>/<name>`, its
    /// resource qualified by its group, as the listing named it
    /// ([`super::kube::RESOURCES`]). Only a listed kind is deleted; another
    /// is named by its kind, for a message.
    pub(super) fn reference(&self) -> String {
        let resource = super::kube::RESOURCES
            .iter()
            .find(|(kind, _)| *kind == self.kind)
            .map_or_else(
                || self.kind.to_ascii_lowercase(),
                |(_, resource)| resource.to_string(),
            );
        format!("{resource}/{}", self.name)
    }

    /// Whether it is a pool's hub's Deployment.
    pub(super) fn is_hub(&self) -> bool {
        self.kind == "Deployment" && self.segment.as_deref() == Some("hub")
    }

    /// Whether it is a Deployment of a pool's segment: not the DynamoDB
    /// Local's, which is no pool's, and goes after every hub that uses it.
    pub(super) fn is_segment(&self) -> bool {
        self.kind == "Deployment" && self.pool.is_some() && !self.is_hub()
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
    /// Its parent pool, whose hub's Service is named for it: none for a
    /// shared pool under the DynamoDB directory.
    pub(super) home: Option<String>,
    /// The longest stop grace of its Deployments, in seconds.
    pub(super) grace: u64,
    /// Whether its hub's Deployment is there to drain it. A pool whose hub
    /// is gone cannot drain: only its parent can show that it withdrew.
    pub(super) hub: bool,
    /// Whether it publishes to the DynamoDB directory, where its parent's
    /// hub holds no record of it to show that it withdrew.
    pub(super) dynamodb: bool,
}

impl Stopping {
    /// The pools of `objects` that have a Deployment there and a parent,
    /// `home` or, for a shared pool, `parents`, in name order: every
    /// Deployment of a pool names its parent, so a pool whose hub's
    /// Deployment is gone is still found by its segments'. A pool with no
    /// parent, such as a global pool, cannot drain: its hub refuses it.
    pub(super) fn of(objects: &[&Live]) -> Vec<Self> {
        let mut pools: std::collections::BTreeMap<String, Self> = Default::default();
        for deployment in objects.iter().filter(|object| object.kind == "Deployment") {
            let (Some(pool), Some(class)) = (&deployment.pool, &deployment.class) else {
                continue;
            };
            if deployment.home.is_none() && deployment.parents.is_none() {
                continue;
            }
            let stopping = pools.entry(pool.clone()).or_insert_with(|| Self {
                pool: pool.clone(),
                class: class.clone(),
                home: deployment.home.clone(),
                grace: 0,
                hub: false,
                dynamodb: false,
            });
            stopping.grace = stopping.grace.max(deployment.grace.unwrap_or_default());
            stopping.hub |= deployment.is_hub();
            stopping.dynamodb |= deployment.dynamodb;
        }
        pools.into_values().collect()
    }
}

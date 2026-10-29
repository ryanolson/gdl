//! What a node in an entry segment says of the models its instance has,
//! for its pool's hub to publish in the pool's record. A dynamo node reads
//! each model's card, a Stargate adapter its model list; the runtime sends
//! what the node said to the hub with the instance's load reports, and as
//! soon as it changes, never on a request's path.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use gdl_model::{InstanceModel, InstanceModelState, ModelKey};
use tokio::sync::watch;

use crate::{Error, ErrorKind, Result};

tokio::task_local! {
    /// The handle of the segment whose nodes are being built.
    static BUILDING: InstanceModels;
}

/// A handle by which a node says which models its instance has, and
/// whether it can serve each now. Cheap to clone; every clone sets the
/// same list.
///
/// A node takes it once, in [`Build::build`](crate::Build::build), with
/// [`InstanceModels::current`], and calls [`InstanceModels::set`] whenever
/// what it has changes: a model loads, becomes ready, or is removed on
/// purpose (`draining`, then left out of the list once it is gone). An
/// instance whose nodes never call it reports no list, and serves the
/// models its pool's settings declare. The runtime reports the list for an
/// entry instance in a pool, and for no other.
///
/// A model that a node drops from its list while its drain has not been
/// reported yet stays in the list as `draining` until a report that
/// carries it is taken, so that its hub sees it drain, not vanish.
#[derive(Clone)]
pub struct InstanceModels {
    said: Arc<watch::Sender<Option<Arc<[InstanceModel]>>>>,
    drains: Arc<Mutex<Drains>>,
    /// Whether a drain awaits a report that carries it being taken.
    unreported: Arc<watch::Sender<bool>>,
}

/// The drains a report has yet to carry.
#[derive(Debug, Default)]
struct Drains {
    /// The node's own list, as it last set it.
    set: Option<Vec<InstanceModel>>,
    /// The models the node said are draining whose drain no report taken
    /// has carried.
    unreported: BTreeSet<ModelKey>,
    /// Those of them the node has dropped from its list since: they stay
    /// in the list, as `draining`, until a report carries them.
    leaving: BTreeMap<ModelKey, InstanceModel>,
}

impl Drains {
    /// The list a report carries: the node's own, with the models leaving.
    fn list(&self) -> Option<Arc<[InstanceModel]>> {
        let set = self.set.as_ref()?;
        Some(set.iter().chain(self.leaving.values()).cloned().collect())
    }
}

impl InstanceModels {
    /// A handle whose list nothing reads until the runtime takes it.
    pub fn new() -> Self {
        Self {
            said: Arc::new(watch::Sender::new(None)),
            drains: Arc::default(),
            unreported: Arc::new(watch::Sender::new(false)),
        }
    }

    /// The handle of the segment whose nodes the runtime is building:
    /// call it in [`Build::build`](crate::Build::build). Outside a build, a
    /// handle whose list nothing reads.
    pub fn current() -> Self {
        BUILDING.try_with(Self::clone).unwrap_or_default()
    }

    /// Say that the instance has `models`, and no others: the list
    /// replaces the one set before. Refused, and nothing changed, when a
    /// model has no name or an empty routing key, or a model is named
    /// twice.
    pub fn set(&self, models: impl IntoIterator<Item = InstanceModel>) -> Result<()> {
        let models: Vec<InstanceModel> = models.into_iter().collect();
        let mut keys = BTreeSet::new();
        for model in &models {
            let key = model.key();
            key.check()
                .map_err(|why| Error::new(ErrorKind::Node, why))?;
            if !keys.insert(key) {
                return Err(Error::new(
                    ErrorKind::Node,
                    format!("model `{}` is named twice", model.model),
                ));
            }
        }
        let mut drains = self.drains.lock().unwrap_or_else(PoisonError::into_inner);
        let Drains {
            set,
            unreported,
            leaving,
        } = &mut *drains;
        for model in set.iter().flatten() {
            let key = model.key();
            if !keys.contains(&key) && unreported.contains(&key) {
                let draining = InstanceModel {
                    state: InstanceModelState::Draining,
                    ..model.clone()
                };
                leaving.insert(key, draining);
            }
        }
        leaving.retain(|key, _| !keys.contains(key));
        let draining = |list: &[InstanceModel]| -> BTreeSet<ModelKey> {
            list.iter()
                .filter(|model| model.state == InstanceModelState::Draining)
                .map(InstanceModel::key)
                .collect()
        };
        let (was, now) = (
            draining(set.as_deref().unwrap_or_default()),
            draining(&models),
        );
        unreported.extend(now.difference(&was).cloned());
        unreported.retain(|key| now.contains(key) || leaving.contains_key(key));
        *set = Some(models);
        self.publish(&drains);
        Ok(())
    }

    /// The hub answered a report that carried `reported`, which it does
    /// once a record that carries the report's drains is stored: the
    /// drains in it are reported, and the models that left meanwhile leave
    /// the list.
    #[doc(hidden)]
    pub fn taken(&self, reported: &[InstanceModel]) {
        let mut drains = self.drains.lock().unwrap_or_else(PoisonError::into_inner);
        let mut changed = false;
        for model in reported {
            if model.state == InstanceModelState::Draining {
                let key = model.key();
                drains.unreported.remove(&key);
                changed |= drains.leaving.remove(&key).is_some();
            }
        }
        if changed {
            self.publish(&drains);
        } else {
            self.note(&drains);
        }
    }

    /// Resolves once every drain the list carried has been in a report
    /// the hub took: the runtime waits for it, within its drain bound,
    /// before an instance that stops stops reporting, so that a hub that
    /// restarts keeps the drain.
    #[doc(hidden)]
    pub async fn reported(&self) {
        let mut unreported = self.unreported.subscribe();
        // The sender lives as long as `self`.
        let _ = unreported.wait_for(|unreported| !*unreported).await;
    }

    fn publish(&self, drains: &Drains) {
        self.note(drains);
        let list = drains.list();
        self.said.send_if_modified(|said| {
            let changed = *said != list;
            *said = list;
            changed
        });
    }

    /// Note whether a drain awaits a report that carries it being taken.
    fn note(&self, drains: &Drains) {
        let now = !drains.unreported.is_empty();
        self.unreported
            .send_if_modified(|unreported| std::mem::replace(unreported, now) != now);
    }

    /// The list as it changes: `None` until a node sets one.
    #[doc(hidden)]
    pub fn watch(&self) -> watch::Receiver<Option<Arc<[InstanceModel]>>> {
        self.said.subscribe()
    }

    /// Run `build` with this handle as the one [`InstanceModels::current`]
    /// gives.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

impl Default for InstanceModels {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InstanceModels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstanceModels")
            .field("models", &*self.said.borrow())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::InstanceModels;
    use gdl_model::{InstanceModel, InstanceModelState};

    fn model(name: &str, state: InstanceModelState) -> InstanceModel {
        InstanceModel {
            model: name.into(),
            routing_key: None,
            state,
        }
    }

    /// A node built in the segment's scope sets the segment's list, which
    /// the runtime reads; outside it, a list nobody reads.
    #[tokio::test]
    async fn a_node_sets_the_list_of_the_segment_it_is_built_in() {
        let segment = InstanceModels::new();
        let read = segment.watch();
        assert!(read.borrow().is_none(), "no list until a node sets one");
        let taken = segment.scope(async { InstanceModels::current() }).await;
        taken
            .set([model("m", InstanceModelState::Ready)])
            .expect("a list");
        assert_eq!(
            read.borrow().as_deref(),
            Some(&[model("m", InstanceModelState::Ready)][..])
        );
        InstanceModels::current()
            .set([model("other", InstanceModelState::Ready)])
            .expect("a list");
        assert_eq!(read.borrow().as_deref().map(<[_]>::len), Some(1));
    }

    /// A list with a model without a name, or one named twice, is refused
    /// whole, and the list set before stands.
    #[test]
    fn a_malformed_list_is_refused_whole() {
        let models = InstanceModels::new();
        models
            .set([model("m", InstanceModelState::Starting)])
            .expect("a list");
        for bad in [
            vec![model("", InstanceModelState::Ready)],
            vec![
                model("m", InstanceModelState::Ready),
                model("m", InstanceModelState::Draining),
            ],
        ] {
            assert!(models.set(bad).is_err());
        }
        assert_eq!(
            models.watch().borrow().as_deref(),
            Some(&[model("m", InstanceModelState::Starting)][..])
        );
    }

    /// A model dropped before a report carried its drain stays in the
    /// list as `draining` until one does; one whose drain was reported
    /// leaves at once. A model loaded again is not kept as leaving.
    #[test]
    fn a_drain_stays_in_the_list_until_a_report_carries_it() {
        let models = InstanceModels::new();
        let read = models.watch();
        let list = || read.borrow().as_deref().map(<[_]>::to_vec);
        models
            .set([model("m", InstanceModelState::Ready)])
            .expect("a list");
        models
            .set([model("m", InstanceModelState::Draining)])
            .expect("a list");
        models.set([]).expect("a list");
        assert_eq!(list(), Some(vec![model("m", InstanceModelState::Draining)]));
        models.taken(&[model("m", InstanceModelState::Draining)]);
        assert_eq!(list(), Some(vec![]));

        models
            .set([model("n", InstanceModelState::Draining)])
            .expect("a list");
        models.taken(&[model("n", InstanceModelState::Draining)]);
        models
            .set([model("n", InstanceModelState::Draining)])
            .expect("a list");
        models.set([]).expect("a list");
        assert_eq!(list(), Some(vec![]), "its drain was reported");

        models
            .set([model("o", InstanceModelState::Draining)])
            .expect("a list");
        models.set([]).expect("a list");
        models
            .set([model("o", InstanceModelState::Starting)])
            .expect("a list");
        assert_eq!(list(), Some(vec![model("o", InstanceModelState::Starting)]));
        models.set([]).expect("a list");
        assert_eq!(list(), Some(vec![]), "loaded again, it is not draining");
    }

    /// `reported` waits while a drain no taken report carried is in the
    /// list, whether the node still lists the model or dropped it, and
    /// resolves once one carried it; a list with no drain does not wait.
    #[tokio::test(start_paused = true)]
    async fn reported_waits_for_each_drain_to_be_taken() {
        let models = InstanceModels::new();
        let waits = |models: &InstanceModels| {
            let models = models.clone();
            async move {
                tokio::time::timeout(std::time::Duration::from_secs(1), models.reported())
                    .await
                    .is_err()
            }
        };
        models
            .set([model("m", InstanceModelState::Ready)])
            .expect("a list");
        assert!(!waits(&models).await, "no drain");
        models
            .set([model("m", InstanceModelState::Draining)])
            .expect("a list");
        assert!(waits(&models).await, "the drain is not reported");
        models.set([]).expect("a list");
        assert!(
            waits(&models).await,
            "dropped, the drain is still not reported"
        );
        models.taken(&[model("m", InstanceModelState::Draining)]);
        assert!(!waits(&models).await, "reported");

        models
            .set([model("n", InstanceModelState::Draining)])
            .expect("a list");
        assert!(waits(&models).await);
        models.taken(&[model("n", InstanceModelState::Draining)]);
        assert!(!waits(&models).await, "taken while still listed");
    }

    /// Setting the list it holds changes nothing, so the runtime sends no
    /// report for it.
    #[test]
    fn the_same_list_again_is_no_change() {
        let models = InstanceModels::new();
        let mut read = models.watch();
        models
            .set([model("m", InstanceModelState::Ready)])
            .expect("a list");
        assert!(read.has_changed().expect("open"));
        read.mark_unchanged();
        models
            .set([model("m", InstanceModelState::Ready)])
            .expect("a list");
        assert!(!read.has_changed().expect("open"));
    }
}

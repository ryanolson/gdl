use gdl_cells::{ModelEntry, ModelState};
use gdl_model::{InstanceModel, InstanceModelState, ModelKey};

use super::{ModelList, Models, Offer, floor};

fn says(model: &str, state: InstanceModelState) -> InstanceModel {
    InstanceModel {
        model: model.into(),
        routing_key: None,
        state,
    }
}

fn entry(model: &str, state: ModelState, since: u64) -> ModelEntry {
    ModelEntry {
        model: model.into(),
        routing_key: None,
        state,
        since,
    }
}

/// The state of each entry, by model.
fn states(models: &Models) -> Vec<(String, ModelState)> {
    models
        .list()
        .entries
        .into_iter()
        .map(|entry| (entry.model, entry.state))
        .collect()
}

fn state(models: &Models, model: &str) -> Option<ModelState> {
    states(models)
        .into_iter()
        .find_map(|(name, state)| (name == model).then_some(state))
}

/// A table restored from no record, as a cell's first hub starts it.
fn fresh() -> Models {
    let mut models = Models::new(Vec::new());
    models.restore(None, 100);
    models
}

use InstanceModelState::{Draining as SaysDraining, Ready as SaysReady, Starting as SaysStarting};
use ModelState::{Drained, Draining, Ready, Starting, Unavailable};

/// The state keeps its history, not the current union: once an instance
/// could serve M, M never reads as a model that never served, though the
/// instance left and another still loads it.
#[test]
fn a_model_that_served_is_unavailable_not_starting_when_its_server_leaves() {
    let mut models = fresh();
    let a = [says("m", SaysReady)];
    let b = [says("m", SaysStarting)];
    assert!(models.update([Offer::Listed(&a), Offer::Listed(&b)]));
    assert_eq!(state(&models, "m"), Some(Ready));
    assert!(models.update([Offer::Listed(&b)]));
    assert_eq!(state(&models, "m"), Some(Unavailable));
    // Back once an instance can serve it again.
    assert!(models.update([Offer::Listed(&b), Offer::Listed(&a)]));
    assert_eq!(state(&models, "m"), Some(Ready));
}

/// An instance that can serve a model makes it `ready`, whatever the
/// others say; one that removes it on purpose, while none can serve it,
/// makes it `draining`; one that loads it, while none of those, keeps it
/// `starting` until it serves once.
#[test]
fn the_union_takes_ready_then_draining_then_starting() {
    let mut models = fresh();
    let starting = [says("m", SaysStarting)];
    let draining = [says("m", SaysDraining)];
    let ready = [says("m", SaysReady)];
    models.update([Offer::Listed(&starting)]);
    assert_eq!(state(&models, "m"), Some(Starting));
    models.update([Offer::Listed(&starting), Offer::Listed(&draining)]);
    assert_eq!(state(&models, "m"), Some(Draining));
    models.update([Offer::Listed(&draining), Offer::Listed(&ready)]);
    assert_eq!(state(&models, "m"), Some(Ready));
}

/// An entry leaves the record only once `drained`: one that every
/// instance stops naming without a drain stays, as `starting` if it never
/// served and `unavailable` if it did, since silence is not a removal.
#[test]
fn an_entry_leaves_only_once_drained() {
    let mut models = fresh();
    let both = [says("served", SaysReady), says("loading", SaysStarting)];
    models.update([Offer::Listed(&both)]);
    models.update([Offer::Listed(&[])]);
    assert_eq!(
        states(&models),
        [("loading".into(), Starting), ("served".into(), Unavailable)]
    );
    models.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(
        states(&models),
        [("loading".into(), Starting), ("served".into(), Unavailable)]
    );

    let draining = [says("served", SaysDraining)];
    models.update([Offer::Listed(&draining)]);
    assert_eq!(state(&models, "served"), Some(Draining));
    models.update([Offer::Listed(&[])]);
    assert_eq!(state(&models, "served"), Some(Drained));
    assert_eq!(state(&models, "loading"), Some(Starting));
    // A drained model deployed again starts afresh.
    let again = [says("served", SaysStarting)];
    models.update([Offer::Listed(&again)]);
    assert_eq!(state(&models, "served"), Some(Starting));
}

/// A drain ends only once every usable instance has said it no longer
/// has the model: an instance not heard from yet may still be draining
/// it.
#[test]
fn a_drain_waits_for_every_instance_to_report() {
    let mut models = fresh();
    let draining = [says("m", SaysDraining)];
    models.update([Offer::Listed(&draining)]);
    models.update([Offer::Listed(&[]), Offer::Unknown]);
    assert_eq!(state(&models, "m"), Some(Draining));
    models.update([Offer::Listed(&[]), Offer::Listed(&[])]);
    assert_eq!(state(&models, "m"), Some(Drained));
}

/// The version rises once for each change of the list, and the entries
/// that changed take it as their `since`; an update that changes nothing
/// keeps it.
#[test]
fn the_version_rises_on_each_change_and_only_then() {
    let mut models = fresh();
    assert_eq!(
        models.list().version,
        100,
        "a cell whose last record holds nothing counts from the floor"
    );
    assert!(!models.update([Offer::Listed(&[])]));
    assert_eq!(models.list().version, 100);
    let ready = [says("a", SaysReady), says("b", SaysStarting)];
    assert!(models.update([Offer::Listed(&ready)]));
    let ModelList {
        entries: list,
        version,
        ..
    } = models.list();
    assert_eq!(version, 101);
    assert_eq!(list, [entry("a", Ready, 101), entry("b", Starting, 101)]);
    assert!(!models.update([Offer::Listed(&ready)]));
    assert_eq!(models.list().version, 101);
    let b_ready = [says("a", SaysReady), says("b", SaysReady)];
    assert!(models.update([Offer::Listed(&b_ready)]));
    assert_eq!(
        models.list(),
        ModelList {
            entries: vec![entry("a", Ready, 101), entry("b", Ready, 102)],
            version: 102,
            revoked: Vec::new(),
        }
    );
}

/// At most [`gdl_cells::CellRecord::MAX_DRAINED`] drained entries are
/// kept, the oldest dropped first; no other entry is dropped for them.
/// The cap is the record's, so the case drains 1,001 models, in two
/// updates to keep it fast.
#[test]
fn the_oldest_drained_entries_are_dropped_past_the_cap() {
    let mut models = fresh();
    let max = gdl_cells::CellRecord::MAX_DRAINED;
    let names: Vec<String> = (0..=max).map(|i| format!("m{i:04}")).collect();
    let mut live = vec![says("kept", SaysReady)];
    live.extend(names.iter().map(|name| says(name, SaysReady)));
    models.update([Offer::Listed(&live)]);
    // The first drains alone, then the rest together: the first is the
    // oldest.
    let drain = |live: &mut Vec<InstanceModel>, models: &mut Models, names: &[String]| {
        live.retain(|model| !names.contains(&model.model));
        let draining: Vec<InstanceModel> = live
            .iter()
            .cloned()
            .chain(names.iter().map(|name| says(name, SaysDraining)))
            .collect();
        models.update([Offer::Listed(&draining)]);
        models.update([Offer::Listed(live)]);
    };
    drain(&mut live, &mut models, &names[..1]);
    assert_eq!(state(&models, "m0000"), Some(Drained));
    drain(&mut live, &mut models, &names[1..]);
    let list = models.list().entries;
    let drained: Vec<&str> = list
        .iter()
        .filter(|entry| entry.state == Drained)
        .map(|entry| entry.model.as_str())
        .collect();
    assert_eq!(drained.len(), max);
    assert!(!drained.contains(&"m0000"), "the oldest was kept");
    assert_eq!(state(&models, "kept"), Some(Ready));
}

/// A table restored from a last record that holds `m` in `state` since
/// version 7, by a hub whose writes count from 50.
fn restarted(state: ModelState) -> Models {
    let mut models = Models::new(Vec::new());
    models.restore(Some((&[entry("m", state, 7)], 7, &[])), 50);
    models
}

/// Nothing is computed before the last record is read: a report that
/// arrives first waits for it.
#[test]
fn nothing_is_computed_before_the_last_record_is_read() {
    let mut models = Models::new(Vec::new());
    let ready = [says("m", SaysReady)];
    assert!(!models.update([Offer::Listed(&ready)]));
    assert_eq!(models.list(), ModelList::default());
    models.restore(None, 50);
    assert!(models.update([Offer::Listed(&ready)]));
    assert_eq!(state(&models, "m"), Some(Ready));
}

/// Restart row: a `starting` entry stays `starting`, with its version.
#[test]
fn a_restart_keeps_a_starting_entry_starting() {
    let models = restarted(Starting);
    assert_eq!(
        models.list(),
        ModelList {
            entries: vec![entry("m", Starting, 7)],
            version: 7,
            revoked: Vec::new(),
        }
    );
}

/// Restart row: a `drained` entry stays `drained`, with its version.
#[test]
fn a_restart_keeps_a_drained_entry_drained() {
    let mut models = restarted(Drained);
    assert_eq!(models.list().entries, [entry("m", Drained, 7)]);
    assert!(!models.update([Offer::Listed(&[])]));
    assert_eq!(models.list().version, 7);
}

/// Restart row: a `draining` entry goes on draining until an instance
/// reports, and is `drained` once every usable instance has reported and
/// none has it.
#[test]
fn a_restart_goes_on_draining_a_draining_entry() {
    let mut models = restarted(Draining);
    assert_eq!(models.list().entries, [entry("m", Draining, 7)]);
    assert!(!models.update(std::iter::empty::<Offer<'_>>()));
    assert!(!models.update([Offer::Unknown]));
    assert_eq!(state(&models, "m"), Some(Draining));
    assert!(models.update([Offer::Listed(&[])]));
    assert_eq!(
        models.list(),
        ModelList {
            entries: vec![entry("m", Drained, 50)],
            version: 50,
            revoked: Vec::new(),
        }
    );
}

/// Restart row: a `ready` entry is `unavailable` until an instance
/// reports it can serve it.
#[test]
fn a_restart_makes_a_ready_entry_unavailable() {
    let mut models = restarted(Ready);
    assert_eq!(
        models.list(),
        ModelList {
            entries: vec![entry("m", Unavailable, 50)],
            version: 50,
            revoked: Vec::new(),
        }
    );
    let loading = [says("m", SaysStarting)];
    models.update([Offer::Listed(&loading)]);
    assert_eq!(state(&models, "m"), Some(Unavailable), "never starting");
    let ready = [says("m", SaysReady)];
    models.update([Offer::Listed(&ready)]);
    assert_eq!(state(&models, "m"), Some(Ready));
}

/// Restart row: an `unavailable` entry stays `unavailable`, with its
/// version, and never `starting`.
#[test]
fn a_restart_keeps_an_unavailable_entry_unavailable() {
    let mut models = restarted(Unavailable);
    assert_eq!(
        models.list(),
        ModelList {
            entries: vec![entry("m", Unavailable, 7)],
            version: 7,
            revoked: Vec::new(),
        }
    );
    let loading = [says("m", SaysStarting)];
    assert!(!models.update([Offer::Listed(&loading)]));
    assert_eq!(state(&models, "m"), Some(Unavailable));
}

/// A restart whose rules change nothing keeps the version: the list is
/// the one readers hold at it. One that reads no record counts from the
/// floor, so that its versions are above any an earlier hub of the cell
/// published, as when a relay parent that restarted holds none.
#[test]
fn a_restart_that_changes_nothing_keeps_the_version() {
    let held = [entry("a", Starting, 3), entry("b", Drained, 4)];
    let mut models = Models::new(Vec::new());
    models.restore(Some((&held, 4, &[])), 50);
    assert_eq!(
        models.list(),
        ModelList {
            entries: held.to_vec(),
            version: 4,
            revoked: Vec::new(),
        }
    );
    let ready = [says("a", SaysReady)];
    models.update([Offer::Listed(&ready)]);
    assert_eq!(models.list().version, 50);

    let mut unread = Models::new(Vec::new());
    unread.restore(None, 70);
    unread.update([Offer::Listed(&ready)]);
    assert_eq!(
        unread.list(),
        ModelList {
            entries: vec![entry("a", Ready, 71)],
            version: 71,
            revoked: Vec::new(),
        }
    );
}

/// Multiple changes can happen within one millisecond. A newer epoch
/// must not reuse their versions when a restarted parent holds no record.
#[test]
fn a_restart_without_a_record_does_not_reuse_a_models_version() {
    let ready = [says("m", SaysReady)];
    let draining = [says("m", SaysDraining)];
    let mut before = Models::new(Vec::new());
    before.restore(None, floor(1_000));
    before.update([Offer::Listed(&ready)]);
    before.update([Offer::Listed(&draining)]);
    let published = before.list();
    assert_eq!(published.version, floor(1_000) + 2);

    let mut retained = Models::new(Vec::new());
    retained.restore(
        Some((&published.entries, published.version, &[])),
        floor(1_001),
    );
    retained.update([Offer::Listed(&ready)]);
    assert!(retained.list().version > published.version);

    let mut after = Models::new(Vec::new());
    // The next hub starts one millisecond later and reads a successful
    // empty snapshot, as when the relay parent restarted too.
    after.restore(None, floor(1_001));
    after.update([Offer::Listed(&ready)]);
    let restarted = after.list();
    assert_ne!(restarted.entries, published.entries);
    assert!(
        restarted.version > published.version,
        "different lists reuse version {} across newer epochs: before={published:?}, after={restarted:?}",
        restarted.version
    );
}

/// A cell whose instances report no models of their own serves the models
/// its settings declare: they are registered, `starting`, from the start,
/// and each usable entry instance, and one not heard from yet, can serve
/// them now. An instance that reports a list of its own reports only it.
#[test]
fn declared_models_are_served_by_every_instance_that_reports_none() {
    let declared = vec![ModelKey::new("x"), ModelKey::new("y")];
    let mut models = Models::new(declared);
    models.restore(None, 10);
    assert_eq!(
        models.list(),
        ModelList {
            entries: vec![entry("x", Starting, 11), entry("y", Starting, 11)],
            version: 11,
            revoked: Vec::new(),
        }
    );
    assert!(!models.update(std::iter::empty::<Offer<'_>>()));
    models.update([Offer::Unknown]);
    assert_eq!(states(&models), [("x".into(), Ready), ("y".into(), Ready)]);
    models.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(
        states(&models),
        [("x".into(), Unavailable), ("y".into(), Unavailable)]
    );
    models.update([Offer::Silent]);
    assert_eq!(states(&models), [("x".into(), Ready), ("y".into(), Ready)]);
    let own = [says("z", SaysReady)];
    models.update([Offer::Listed(&own)]);
    assert_eq!(
        states(&models),
        [
            ("x".into(), Unavailable),
            ("y".into(), Unavailable),
            ("z".into(), Ready)
        ]
    );
}

/// A model loaded again while it drains starts afresh, with or without a
/// restart between: the record holds no more than its state.
#[test]
fn a_restart_preserves_whether_a_draining_model_ever_served() {
    let starting = [says("m", SaysStarting)];
    let draining = [says("m", SaysDraining)];
    let mut uninterrupted = fresh();
    uninterrupted.update([Offer::Listed(&starting)]);
    assert_eq!(state(&uninterrupted, "m"), Some(Starting));
    uninterrupted.update([Offer::Listed(&draining)]);
    assert_eq!(state(&uninterrupted, "m"), Some(Draining));
    let held = uninterrupted.list();

    uninterrupted.update([Offer::Listed(&starting)]);
    assert_eq!(state(&uninterrupted, "m"), Some(Starting));

    let mut restored = Models::new(Vec::new());
    restored.restore(Some((&held.entries, held.version, &[])), held.version + 100);
    assert_eq!(state(&restored, "m"), Some(Draining));
    restored.update([Offer::Listed(&starting)]);
    assert_eq!(state(&restored, "m"), Some(Starting));
}

/// A report that changes no state still counts: a drain read from the
/// last record that an instance reports ends once that instance leaves.
#[test]
fn a_restored_drain_finishes_after_its_reporting_instance_leaves() {
    let draining = [says("m", SaysDraining)];
    let mut uninterrupted = fresh();
    uninterrupted.update([Offer::Listed(&draining)]);
    uninterrupted.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(state(&uninterrupted, "m"), Some(Drained));

    let mut restored = restarted(Draining);
    let before = restored.list().version;
    assert!(!restored.update([Offer::Listed(&draining)]));
    assert_eq!(restored.list().version, before);
    restored.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(state(&restored, "m"), Some(Drained));
}

/// An empty report still proves the instance no longer holds a restored
/// drain. That evidence must survive its departure while another
/// instance has not reported yet.
#[test]
fn a_restored_drain_keeps_an_empty_report_after_its_reporter_leaves() {
    let mut uninterrupted = fresh();
    uninterrupted.update([Offer::Listed(&[says("m", SaysDraining)])]);
    uninterrupted.update([Offer::Listed(&[]), Offer::Unknown]);
    assert_eq!(state(&uninterrupted, "m"), Some(Draining));
    uninterrupted.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(state(&uninterrupted, "m"), Some(Drained));

    let mut restored = restarted(Draining);
    restored.update([Offer::Listed(&[]), Offer::Unknown]);
    assert_eq!(state(&restored, "m"), Some(Draining));
    restored.update(std::iter::empty::<Offer<'_>>());
    assert_eq!(
        state(&restored, "m"),
        Some(Drained),
        "an empty report was received, and no usable instance remains"
    );
}

/// A writer that moves to a later epoch raises the floor: the list's
/// version rises to it at once, and later changes count from it. A floor
/// below the version changes nothing.
#[test]
fn a_raised_floor_raises_the_version() {
    let mut models = fresh();
    let ready = [says("m", SaysReady)];
    models.update([Offer::Listed(&ready)]);
    assert_eq!(models.list().version, 101);
    assert!(!models.raise_floor(50));
    assert!(models.raise_floor(500));
    assert_eq!(models.list().version, 500);
    let draining = [says("m", SaysDraining)];
    models.update([Offer::Listed(&draining)]);
    assert_eq!(models.list().version, 501);
}

/// A cell declares that it serves each model on its own: one model that
/// goes away, by a drain or by silence, changes no other model's state.
#[test]
fn a_model_that_goes_away_leaves_the_others_as_they_are() {
    let mut models = fresh();
    let both = [says("llama-70b", SaysReady), says("acme-legal", SaysReady)];
    models.update([Offer::Listed(&both)]);
    let llama = |models: &Models| state(models, "llama-70b");
    let draining = [
        says("llama-70b", SaysReady),
        says("acme-legal", SaysDraining),
    ];
    models.update([Offer::Listed(&draining)]);
    assert_eq!(llama(&models), Some(Ready));
    let llama_only = [says("llama-70b", SaysReady)];
    models.update([Offer::Listed(&llama_only)]);
    assert_eq!(state(&models, "acme-legal"), Some(Drained));
    assert_eq!(llama(&models), Some(Ready));

    let mut silent = fresh();
    silent.update([Offer::Listed(&both)]);
    silent.update([Offer::Listed(&llama_only)]);
    assert_eq!(state(&silent, "acme-legal"), Some(Unavailable));
    assert_eq!(llama(&silent), Some(Ready));
}

fn key(model: &str) -> ModelKey {
    ModelKey::new(model)
}

/// A revoked model leaves the list at once, and the version rises; the
/// instances' reports, which still name it, never bring it back. The
/// other models stay as they are.
#[test]
fn a_revoked_model_leaves_the_list_whatever_the_instances_say() {
    let mut models = fresh();
    let both = [says("acme-legal", SaysReady), says("llama-70b", SaysReady)];
    assert!(models.update([Offer::Listed(&both)]));
    let before = models.list().version;

    assert!(models.revoke(key("acme-legal")));
    let revoked = models.list();
    assert_eq!(revoked.entries, [entry("llama-70b", Ready, before)]);
    assert_eq!(revoked.revoked, [key("acme-legal")]);
    assert!(revoked.version > before);

    assert!(!models.update([Offer::Listed(&both)]), "nothing changed");
    assert_eq!(models.list(), revoked);
    assert!(!models.revoke(key("acme-legal")), "revoked already");
    assert_eq!(models.list(), revoked);
}

/// A model the cell's settings declare is held out too, though every
/// instance that reports no list serves it.
#[test]
fn a_revoked_declared_model_is_held_out() {
    let mut models = Models::new(vec![key("acme-legal"), key("llama-70b")]);
    models.restore(None, 10);
    assert!(models.update([Offer::Silent]));
    assert!(models.revoke(key("acme-legal")));
    assert!(!models.update([Offer::Silent]));
    assert_eq!(states(&models), [("llama-70b".to_string(), Ready)]);
}

/// A hub that restarts keeps its cell's revocations from the last record:
/// what an instance reports of a revoked model is not taken, and a
/// declared one is not listed.
#[test]
fn a_restart_keeps_the_revocations() {
    let mut models = Models::new(vec![key("acme-legal")]);
    models.restore(
        Some((&[entry("llama-70b", Ready, 9)], 9, &[key("acme-legal")])),
        50,
    );
    let restored = models.list();
    assert_eq!(restored.revoked, [key("acme-legal")]);
    assert_eq!(restored.entries, [entry("llama-70b", Unavailable, 50)]);
    let both = [says("acme-legal", SaysReady), says("llama-70b", SaysReady)];
    models.update([Offer::Listed(&both)]);
    assert_eq!(states(&models), [("llama-70b".to_string(), Ready)]);
}

/// Once lifted, the model is listed again as the instances say, starting
/// afresh: its history went with the revocation.
#[test]
fn a_lifted_model_is_listed_again_as_the_instances_say() {
    let mut models = fresh();
    let both = [says("acme-legal", SaysReady), says("llama-70b", SaysReady)];
    models.update([Offer::Listed(&both)]);
    models.revoke(key("acme-legal"));
    let revoked = models.list().version;

    assert!(models.lift(&key("acme-legal")));
    assert!(models.list().revoked.is_empty());
    assert!(models.update([Offer::Listed(&both)]));
    let lifted = models.list();
    assert!(lifted.version > revoked);
    assert_eq!(
        lifted.entries,
        [
            entry("acme-legal", Ready, lifted.version),
            entry("llama-70b", Ready, 101)
        ]
    );
    assert!(!models.lift(&key("acme-legal")), "lifted already");
}

/// A revocation or a lift taken before the last record is read applies
/// over the record's own: the hub answers it once its first record is
/// stored.
#[test]
fn a_revocation_or_lift_before_the_record_is_read_applies_over_it() {
    let mut models = Models::new(Vec::new());
    assert!(models.revoke(key("qwen-32b")));
    assert!(models.lift(&key("acme-legal")));
    models.restore(
        Some((&[entry("llama-70b", Ready, 9)], 9, &[key("acme-legal")])),
        50,
    );
    let restored = models.list();
    assert_eq!(restored.revoked, [key("qwen-32b")]);
    assert!(restored.version > 9, "the revocations changed");
    let all = [
        says("acme-legal", SaysReady),
        says("llama-70b", SaysReady),
        says("qwen-32b", SaysReady),
    ];
    models.update([Offer::Listed(&all)]);
    assert_eq!(
        states(&models),
        [
            ("acme-legal".to_string(), Ready),
            ("llama-70b".to_string(), Ready)
        ]
    );
}

fn revoke_then_lift_history(before_read: bool) {
    let held = [entry("acme-legal", Unavailable, 9)];
    let loading = [says("acme-legal", SaysStarting)];
    let mut models = Models::new(Vec::new());
    if !before_read {
        models.restore(Some((&held, 9, &[])), 50);
    }
    assert!(models.revoke(key("acme-legal")));
    assert!(models.lift(&key("acme-legal")));
    if before_read {
        models.restore(Some((&held, 9, &[])), 50);
    }
    models.update([Offer::Listed(&loading)]);
    assert_eq!(
        state(&models, "acme-legal"),
        Some(Starting),
        "a lifted model starts afresh, including before the restart read: before_read={before_read}, list={:?}",
        models.list()
    );
}

#[test]
fn a_revoke_then_lift_discards_history_after_the_record_is_read() {
    revoke_then_lift_history(false);
}

/// A revoke followed by a lift discards the model's history, whether
/// the hub has read its last record yet or still waits for it.
#[test]
fn a_startup_revoke_then_lift_discards_the_models_history() {
    revoke_then_lift_history(true);
}

#[test]
fn a_declared_model_recreated_at_startup_takes_the_new_version() {
    let held = [entry("acme-legal", Starting, 9)];
    let mut unchanged = Models::new(vec![key("acme-legal")]);
    unchanged.restore(Some((&held, 9, &[])), 50);
    assert_eq!(unchanged.list().entries, held);
    assert_eq!(unchanged.list().version, 9);

    let mut models = Models::new(vec![key("acme-legal")]);
    assert!(models.revoke(key("acme-legal")));
    assert!(models.lift(&key("acme-legal")));
    models.restore(Some((&held, 9, &[])), 50);
    let recreated = models.list();
    assert!(recreated.revoked.is_empty());
    assert!(recreated.version > 9);
    assert_eq!(
        recreated.entries,
        [entry("acme-legal", Starting, recreated.version)],
        "a recreated model takes the version of its fresh state"
    );
}

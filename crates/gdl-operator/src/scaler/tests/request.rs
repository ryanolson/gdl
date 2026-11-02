//! `retire: request` against the fake cluster, a fake hub that lists the
//! pods' instances and takes retire requests, and fake `/metrics` that
//! show each pod's leave hooks.

use super::*;
use crate::{Handoff, Retire};

/// The pod at `10.0.0.n` is instance `i-n`, registered in incarnation `n`.
fn id(n: u32) -> String {
    format!("i-{n}")
}

/// The fake's hub listing each of `pods`, by number, in `phase`.
fn list(fake: &Fake, pods: &[(u32, &str)]) {
    let instances = pods
        .iter()
        .map(|(n, phase)| {
            serde_json::json!({
                "segment": "front", "peer": {"instance_id": id(*n), "worker_address": []},
                "addresses": [format!("10.0.0.{n}:7000")], "phase": phase, "incarnation": n,
            })
        })
        .collect::<Vec<_>>();
    *fake.instances.lock().expect("instances") = Some(
        serde_json::from_value(serde_json::json!({ "instances": instances })).expect("a list"),
    );
}

/// The phase the fake's hub lists pod `n`'s instance in, from now.
fn phase(fake: &Fake, n: u32, phase: &str) {
    let mut instances = fake.instances.lock().expect("instances");
    let listed = instances
        .as_mut()
        .expect("a list")
        .instances
        .iter_mut()
        .find(|i| i.registration().is_some_and(|(at, _)| at == id(n)))
        .expect("listed");
    listed.phase = phase.into();
}

/// Pod `n`'s leave hooks, as its `/metrics` shows them: each hook's
/// outcome, `pending` for one that has not ended.
fn hooks(fake: &Fake, n: u32, hooks: &[(&str, &str)]) {
    let text: String = hooks
        .iter()
        .flat_map(|(hook, outcome)| {
            let pending = u8::from(*outcome == "pending");
            let mut lines = vec![format!(
                "dynamo_component_leave_outcome{{hook=\"{hook}\",outcome=\"pending\"}} {pending}\n"
            )];
            if *outcome != "pending" {
                lines.push(format!(
                    "dynamo_component_leave_outcome{{hook=\"{hook}\",outcome=\"{outcome}\"}} 1\n"
                ));
            }
            lines
        })
        .collect();
    fake.leave
        .lock()
        .expect("leave")
        .insert(format!("10.0.0.{n}"), text);
}

fn request(replicas: i32) -> GdlScalerSpec {
    let mut spec = spec(Some(replicas));
    spec.retire = Some(Retire::Request);
    spec.drain_bound = Some("20s".into());
    spec
}

fn victim(fake: &Fake) -> Retiring {
    status(fake).retiring[0].clone()
}

/// Three pods listed `ready`, the middle one the fewest admitted.
fn three() -> Fake {
    let fake = Fake::with(&[5, 0, 5]);
    list(&fake, &[(1, "ready"), (2, "ready"), (3, "ready")]);
    fake
}

/// The victim's hub takes the retire request, and the scaler writes
/// nothing to the pod or the count until the hub lists it `drained` and
/// its `/metrics` shows every hook's outcome. It asks once, and stores
/// each step before it takes it: the retire before the request, the
/// commit before the cost.
#[tokio::test]
async fn a_victim_is_deleted_only_once_it_has_drained_and_its_hooks_have_reported() {
    let fake = three();
    hooks(&fake, 2, &[("kv", "pending"), ("flush", "pending")]);
    let spec = request(2);
    assert_eq!(once(&fake, &spec, 0).await, BUSY);
    assert_eq!(
        events(&fake),
        [
            "status [retiring uid-2]",
            "retire i-2 2",
            "status [draining uid-2]"
        ]
    );
    let chosen = victim(&fake);
    assert_eq!(
        (chosen.instance_id.as_deref(), chosen.incarnation),
        (Some("i-2"), Some(2))
    );
    assert!(chosen.asked.is_some() && chosen.retired.is_some());

    phase(&fake, 2, "retired");
    once(&fake, &spec, 1_000).await;
    phase(&fake, 2, "drained");
    once(&fake, &spec, 2_000).await;
    hooks(&fake, 2, &[("kv", "done"), ("flush", "pending")]);
    once(&fake, &spec, 3_000).await;
    assert_eq!(fake.take_writes(), ["retire i-2 2"], "nothing before both");
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    assert_eq!(victim(&fake).handoff, Some(Handoff::Done));

    hooks(&fake, 2, &[("kv", "done"), ("flush", "partial")]);
    events(&fake);
    once(&fake, &spec, 4_000).await;
    assert_eq!(
        events(&fake),
        [
            "status [committing uid-2]",
            "pod-2 cost Some(-2147483648)",
            "replicas 2",
            "status [removing uid-2]"
        ]
    );
    fake.settle(false);
    assert!(fake.pod("pod-2").view.terminating);
    fake.exit();
    list(&fake, &[(1, "ready"), (3, "ready")]);
    once(&fake, &spec, 5_000).await;
    let last = status(&fake).last.expect("ended");
    assert_eq!(
        (last.outcome, last.chosen.as_str(), last.handoff),
        (Outcome::Removed, "uid-2", Some(Handoff::Partial))
    );
    assert!(
        !fake.take_writes().iter().any(|w| w.starts_with("retire")),
        "asked once"
    );
}

/// An instance with no hook, or of a build without hooks, shows no series:
/// its drain alone is waited for.
#[tokio::test]
async fn without_hooks_the_drain_alone_is_waited_for() {
    let fake = three();
    let spec = request(2);
    once(&fake, &spec, 0).await;
    once(&fake, &spec, 1_000).await;
    assert_eq!(fake.state().replicas, 3, "not drained yet");
    phase(&fake, 2, "drained");
    once(&fake, &spec, 2_000).await;
    assert_eq!(fake.state().replicas, 2);
    assert_eq!(victim(&fake).handoff, None);
}

/// A victim that has not drained, or whose hook has not reported, is
/// deleted once `drainBound` has passed since its hub took the request,
/// never before, and its outcome says it did not report.
#[tokio::test]
async fn a_victim_that_does_not_drain_is_deleted_at_the_bound() {
    for drains in [false, true] {
        let fake = three();
        hooks(&fake, 2, &[("kv", "pending")]);
        let spec = request(2);
        once(&fake, &spec, 1_000).await;
        if drains {
            phase(&fake, 2, "drained");
        }
        once(&fake, &spec, 20_999).await;
        assert_eq!(fake.state().replicas, 3, "before the bound ({drains})");
        once(&fake, &spec, 21_000).await;
        assert_eq!(fake.state().replicas, 2, "at the bound ({drains})");
        assert_eq!(victim(&fake).handoff, Some(Handoff::Unreported));
    }
    // Drained and reported, but the pod's `/metrics` no longer answers at
    // the bound: unreported as well.
    let fake = three();
    let spec = request(2);
    once(&fake, &spec, 0).await;
    fake.silent
        .lock()
        .expect("silent")
        .insert("10.0.0.2".into());
    phase(&fake, 2, "drained");
    once(&fake, &spec, 10_000).await;
    assert_eq!(fake.state().replicas, 3);
    once(&fake, &spec, 20_000).await;
    assert_eq!(victim(&fake).handoff, Some(Handoff::Unreported));
}

/// The ReplicaSet deletes another pod, one gone unready, after the
/// victim retired and its count was lowered. The victim cannot serve
/// again, so the scaler deletes its pod itself, never gives back its
/// Service or its cost, and records `kubernetesChoseAnother`; the
/// ReplicaSet replaces it, and the segment gets back to its count.
#[tokio::test]
async fn a_retired_victim_passed_over_for_a_pod_gone_unready_is_deleted() {
    let fake = Fake::with(&[9, 0, 9]);
    list(&fake, &[(1, "ready"), (2, "ready"), (3, "ready")]);
    let mut spec = request(2);
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    once(&fake, &spec, 2_000).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    phase(&fake, 2, "drained");
    once(&fake, &spec, 3_000).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Removing);
    fake.state().pods[0].view.ready = false;
    fake.settle(false);
    assert!(fake.pod("pod-1").view.terminating);
    assert!(!fake.pod("pod-2").view.terminating);
    fake.take_writes();
    events(&fake);
    once(&fake, &spec, 4_000).await;
    assert_eq!(
        events(&fake)[..2],
        ["status [deleting uid-2]", "delete pod-2"]
    );
    assert_eq!(fake.take_writes(), ["delete pod-2"]);
    let last = status(&fake).last.expect("ended");
    assert_eq!(last.outcome, Outcome::KubernetesChoseAnother);
    assert_eq!(
        (last.chosen.as_str(), last.deleted.as_str()),
        ("uid-2", "uid-1")
    );
    assert_eq!(victim(&fake).phase, RetiringPhase::Deleting);
    fake.exit();
    fake.settle(false);
    once(&fake, &spec, 5_000).await;
    assert!(status(&fake).retiring.is_empty());
    let live = fake
        .state()
        .pods
        .iter()
        .filter(|p| !p.view.terminating)
        .count();
    assert_eq!((live, fake.state().replicas), (2, 2), "back to its count");
    assert!(
        !fake.take_writes().iter().any(|w| w.starts_with("pod-2")),
        "the victim never serves again"
    );
}

/// A scale-up after the retire request undoes nothing: the victim is not
/// given back its Service or its cost. Its pod is deleted, the count
/// stays, and the ReplicaSet makes a new pod.
#[tokio::test]
async fn a_scale_up_after_the_retire_deletes_the_victim() {
    let fake = Fake::with(&[9, 0, 9]);
    list(&fake, &[(1, "ready"), (2, "ready"), (3, "ready")]);
    let mut spec = request(2);
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    once(&fake, &spec, 2_000).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    fake.take_writes();
    spec.replicas = Some(3);
    once(&fake, &spec, 3_000).await;
    assert_eq!(fake.take_writes(), ["delete pod-2"]);
    assert_eq!(fake.state().replicas, 3);
    let last = status(&fake).last.expect("ended");
    assert_eq!(
        (last.outcome, last.chosen.as_str()),
        (Outcome::Replaced, "uid-2")
    );
    fake.settle(false);
    fake.exit();
    once(&fake, &spec, 4_000).await;
    assert!(status(&fake).retiring.is_empty());
    assert_eq!(fake.state().pods.len(), 3);
    assert!(fake.take_writes().iter().all(|w| !w.starts_with("pod-2")));
}

/// Before the retire step is stored, a scale-up still gives the victim
/// back: a hub that does not list its instance holds it in `unrouting`,
/// and says why.
#[tokio::test]
async fn a_victim_not_yet_asked_is_given_back() {
    let fake = Fake::with(&[9, 0, 9]);
    let mut spec = request(2);
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    assert_eq!(once(&fake, &spec, 2_000).await, BLOCKED);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "HubUnavailable".into())
    );
    assert_eq!(victim(&fake).phase, RetiringPhase::Unrouting);
    fake.take_writes();
    spec.replicas = Some(3);
    once(&fake, &spec, 3_000).await;
    assert_eq!(fake.take_writes(), ["pod-2 routable true"]);
    assert_eq!(
        status(&fake).last.expect("ended").outcome,
        Outcome::Cancelled
    );
}

/// A hub that refuses the request keeps the victim asked, with no pod
/// deleted and no count lowered: until `drainBound` it waits; from then
/// the scale-down is blocked, and one event says so.
#[tokio::test]
async fn a_refused_request_blocks_at_the_bound_and_says_so_once() {
    let fake = three();
    *fake.refuse.lock().expect("refuse") = Some(Asked::Refused("the pool drains".into()));
    let spec = request(2);
    once(&fake, &spec, 0).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Retiring);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("False".into(), "NotBlocked".into())
    );
    assert_eq!(once(&fake, &spec, 20_000).await, BLOCKED);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "RetireRefused".into())
    );
    once(&fake, &spec, 25_000).await;
    assert_eq!(*fake.warned.lock().expect("warned"), ["RetireRefused"]);
    let writes = fake.take_writes();
    assert_eq!(writes.len(), 3, "{writes:?}");
    assert!(writes.iter().all(|w| w == "retire i-2 2"), "{writes:?}");
    // The hub takes it at last.
    *fake.refuse.lock().expect("refuse") = None;
    once(&fake, &spec, 26_000).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("False".into(), "NotBlocked".into())
    );
}

/// The victim's instance registered again: the new registration is a
/// new instance, recorded before it is asked, never asked under the
/// earlier one's name.
#[tokio::test]
async fn a_new_registration_is_recorded_before_it_is_asked() {
    let fake = three();
    *fake.refuse.lock().expect("refuse") = Some(Asked::Unknown);
    let spec = request(2);
    once(&fake, &spec, 0).await;
    *fake.refuse.lock().expect("refuse") = None;
    {
        let mut instances = fake.instances.lock().expect("instances");
        let listed = &mut instances.as_mut().expect("a list").instances[1];
        listed.peer = serde_json::from_value(serde_json::json!({"instance_id": "i-9"})).ok();
        listed.incarnation = Some(9);
    }
    fake.take_writes();
    events(&fake);
    once(&fake, &spec, 1_000).await;
    assert_eq!(
        fake.take_writes(),
        Vec::<String>::new(),
        "recorded, not asked"
    );
    let recorded = victim(&fake);
    assert_eq!(
        (recorded.instance_id.as_deref(), recorded.incarnation),
        (Some("i-9"), Some(9))
    );
    once(&fake, &spec, 2_000).await;
    assert_eq!(fake.take_writes(), ["retire i-9 9"]);
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
}

/// Request mode needs a bound to wait for a victim.
#[tokio::test]
async fn request_mode_needs_a_drain_bound() {
    let fake = three();
    let mut spec = request(2);
    spec.drain_bound = None;
    assert_eq!(once(&fake, &spec, 0).await, BLOCKED);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "DrainBoundRequired".into())
    );
    assert!(fake.take_writes().is_empty());
}

/// A victim asked to retire is still waited for at most `drainBound` once
/// the spec leaves request mode: without a bound the scale-down blocks,
/// and with one the victim's drain goes on to its commit.
#[tokio::test]
async fn a_victim_asked_before_the_mode_changed_still_needs_its_bound() {
    let fake = three();
    let mut spec = request(2);
    once(&fake, &spec, 0).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    spec.retire = Some(Retire::Signal);
    spec.drain_bound = None;
    assert_eq!(once(&fake, &spec, 1_000).await, BLOCKED);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "DrainBoundRequired".into())
    );
    spec.drain_bound = Some("20s".into());
    phase(&fake, 2, "drained");
    once(&fake, &spec, 2_000).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Removing);
    assert_eq!(fake.state().replicas, 2);
}

/// A probe whose retire request waits until the test lets it go, so that
/// time passes between the reconcile's start and the hub's acceptance.
struct SlowRetire<'a> {
    fake: &'a Fake,
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl Probe for SlowRetire<'_> {
    async fn instances(&self, pool: &str) -> Result<Instances, String> {
        self.fake.instances(pool).await
    }

    async fn metrics(&self, ip: &str, port: u16) -> Result<String, String> {
        self.fake.metrics(ip, port).await
    }

    async fn retire(&self, pool: &str, instance: &str, incarnation: u64) -> Result<Asked, String> {
        self.arrived.notify_one();
        self.resume.notified().await;
        self.fake.retire(pool, instance, incarnation).await
    }
}

/// F3: the drain bound counts from the hub's acceptance, not from the
/// start of the reconcile that asked. A request accepted 2 s into its
/// reconcile, with a bound of 2 s, holds the victim until 4 s.
#[tokio::test]
async fn the_drain_bound_counts_from_the_hubs_acceptance() {
    let fake = three();
    hooks(&fake, 2, &[("kv", "pending")]);
    let mut spec = request(2);
    spec.drain_bound = Some("2s".into());
    let probe = SlowRetire {
        fake: &fake,
        arrived: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    };
    let now = std::sync::atomic::AtomicI64::new(0);
    let clock = || now.load(std::sync::atomic::Ordering::SeqCst);
    let victims = Victims::default();
    let cx = Context {
        cluster: &fake,
        probe: &probe,
        victims: &victims,
        clock: &clock,
    };
    let reconcile_at = |at: i64| {
        now.store(at, std::sync::atomic::Ordering::SeqCst);
        let mut scaler = GdlScaler::new("global-front", spec.clone());
        scaler.metadata.generation = Some(1);
        scaler.status = fake.state().status.clone();
        scaler.metadata.resource_version = Some(fake.state().scaler_version.to_string());
        let cx = &cx;
        async move { reconcile(cx, &scaler, at).await.expect("reconciles") }
    };
    let accepted_late = async {
        probe.arrived.notified().await;
        now.store(2_000, std::sync::atomic::Ordering::SeqCst);
        probe.resume.notify_one();
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(reconcile_at(0), accepted_late)
    })
    .await
    .expect("the request is let go");
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    reconcile_at(3_000).await;
    assert_eq!(
        fake.state().replicas,
        3,
        "2 s after the hub took it, not 3 s"
    );
    reconcile_at(4_000).await;
    assert_eq!(fake.state().replicas, 2, "at the bound");
}

/// The fake's hub listing `entries`: each one's instance id, the pod
/// number whose IP it is at, its incarnation, and its phase, in order.
fn list_at(fake: &Fake, entries: &[(&str, u32, u64, &str)]) {
    let instances = entries
        .iter()
        .map(|(id, n, incarnation, phase)| {
            serde_json::json!({
                "segment": "front", "peer": {"instance_id": id, "worker_address": []},
                "addresses": [format!("10.0.0.{n}:7000")], "phase": phase,
                "incarnation": incarnation,
            })
        })
        .collect::<Vec<_>>();
    *fake.instances.lock().expect("instances") = Some(
        serde_json::from_value(serde_json::json!({ "instances": instances })).expect("a list"),
    );
}

/// The victim's container restarts during the hold: a new process, which
/// was never asked, while the earlier registration is still listed at
/// the pod's IP and says `drained`. A restart ends the hold. The victim
/// goes on its SIGTERM at once, as in signal mode, and says why; nothing
/// the earlier registration or the new process shows counts as its
/// drain or its hooks' outcome.
#[tokio::test]
async fn a_restart_during_the_hold_ends_it() {
    let fake = three();
    hooks(&fake, 2, &[("kv", "pending")]);
    let spec = request(2);
    once(&fake, &spec, 0).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    assert_eq!(victim(&fake).restarts, Some(0));
    fake.state().pods[1].view.restarts = 1;
    fake.leave.lock().expect("leave").clear();
    list_at(
        &fake,
        &[
            ("i-1", 1, 1, "ready"),
            ("i-2", 2, 2, "drained"),
            ("i-9", 2, 9, "ready"),
            ("i-3", 3, 3, "ready"),
        ],
    );
    fake.take_writes();
    once(&fake, &spec, 1_000).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-2 cost Some(-2147483648)", "replicas 2"],
        "at once, and never asked"
    );
    let gone = victim(&fake);
    assert!(gone.restarted);
    assert_eq!(gone.handoff, Some(Handoff::Unreported));
    assert_eq!(
        *fake.warned.lock().expect("warned"),
        ["RestartedDuringHold"]
    );
    fake.settle(false);
    fake.exit();
    list(&fake, &[(1, "ready"), (3, "ready")]);
    once(&fake, &spec, 2_000).await;
    let last = status(&fake).last.expect("ended");
    assert_eq!(last.outcome, Outcome::Removed);
    assert!(
        last.reason.contains("restartedDuringHold"),
        "{}",
        last.reason
    );
    assert_eq!(last.handoff, Some(Handoff::Unreported));
}

/// A commit stored and not written, then a restart before it is taken
/// again: the restart is said, and the commit goes on, as in signal mode.
#[tokio::test]
async fn a_restart_before_a_stored_commit_is_taken_again_is_said() {
    let fake = three();
    let spec = request(2);
    once(&fake, &spec, 0).await;
    // The commit was stored, and its cost and count never written.
    fake.state().status.as_mut().expect("a status").retiring[0].phase = RetiringPhase::Committing;
    fake.state().pods[1].view.restarts = 2;
    list_at(
        &fake,
        &[
            ("i-1", 1, 1, "ready"),
            ("i-9", 2, 9, "ready"),
            ("i-3", 3, 3, "ready"),
        ],
    );
    fake.take_writes();
    once(&fake, &spec, 1_000).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-2 cost Some(-2147483648)", "replicas 2"]
    );
    assert!(victim(&fake).restarted);
    assert_eq!(
        *fake.warned.lock().expect("warned"),
        ["RestartedDuringHold"]
    );
}

/// The hold reads only the registration it asked, by its id and
/// incarnation: another at the pod's IP that says `drained` is not its
/// drain, and is not asked.
#[tokio::test]
async fn the_hold_reads_only_the_registration_it_asked() {
    let fake = three();
    let spec = request(2);
    once(&fake, &spec, 0).await;
    list_at(
        &fake,
        &[
            ("i-1", 1, 1, "ready"),
            ("i-0", 2, 1, "drained"),
            ("i-2", 2, 2, "retired"),
            ("i-3", 3, 3, "ready"),
        ],
    );
    fake.take_writes();
    once(&fake, &spec, 1_000).await;
    assert!(fake.take_writes().is_empty());
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    assert_eq!(victim(&fake).instance_id.as_deref(), Some("i-2"));
    list_at(
        &fake,
        &[
            ("i-1", 1, 1, "ready"),
            ("i-0", 2, 1, "drained"),
            ("i-2", 2, 2, "drained"),
            ("i-3", 3, 3, "ready"),
        ],
    );
    once(&fake, &spec, 2_000).await;
    assert_eq!(fake.state().replicas, 2);
}

/// F1 of the third review: the hub restarted after it took the request,
/// and lists nobody, while the victim still holds work. Absence is not
/// evidence of a drain: the victim is held until its bound.
#[tokio::test]
async fn an_absent_registration_is_not_a_drain() {
    let fake = Fake::with(&[9, 4, 9]);
    list(&fake, &[(1, "ready"), (2, "ready"), (3, "ready")]);
    let spec = request(2);
    once(&fake, &spec, 0).await;
    assert_eq!(victim(&fake).pod, "pod-2");
    list(&fake, &[]);
    once(&fake, &spec, 1_000).await;
    assert_eq!(fake.state().replicas, 3, "the hub lists nobody");
    once(&fake, &spec, 19_999).await;
    assert_eq!(fake.state().replicas, 3);
    once(&fake, &spec, 20_000).await;
    assert_eq!(fake.state().replicas, 2, "at the bound");
    assert!(!victim(&fake).restarted);
}

/// F2 of the third review: the victim's container is made again, as
/// after its node restarts, and its restart count starts over at the
/// count stored. Another container is another process: the hold ends.
#[tokio::test]
async fn a_new_container_at_the_same_restart_count_ends_the_hold() {
    let fake = three();
    let spec = request(2);
    once(&fake, &spec, 0).await;
    assert_eq!(victim(&fake).restarts, Some(0));
    fake.state().pods[1].view.containers = vec!["containerd://again".into()];
    list_at(
        &fake,
        &[
            ("i-1", 1, 1, "ready"),
            ("i-9", 2, 9, "ready"),
            ("i-3", 3, 3, "ready"),
        ],
    );
    hooks(&fake, 2, &[("kv", "pending")]);
    fake.take_writes();
    once(&fake, &spec, 1_000).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-2 cost Some(-2147483648)", "replicas 2"]
    );
    assert!(victim(&fake).restarted);
    assert_eq!(victim(&fake).handoff, Some(Handoff::Unreported));
}

/// F3 of the third review: one victim at a time in request mode, its
/// removal included: a victim committed at its bound, whose pod is not
/// gone yet, still holds work, and no other is asked beside it.
#[tokio::test]
async fn request_mode_takes_one_victim_until_its_pod_is_gone() {
    let fake = Fake::with(&[9, 0, 1, 9]);
    list(
        &fake,
        &[(1, "ready"), (2, "ready"), (3, "ready"), (4, "ready")],
    );
    hooks(&fake, 2, &[("kv", "pending")]);
    let mut spec = request(2);
    spec.max_concurrent_drains = Some(4);
    once(&fake, &spec, 0).await;
    once(&fake, &spec, 20_000).await;
    let writes = fake.take_writes();
    assert_eq!(status(&fake).retiring.len(), 1, "{writes:?}");
    assert!(
        writes.iter().filter(|w| w.starts_with("retire")).count() == 1,
        "{writes:?}"
    );
    fake.settle(false);
    fake.exit();
    list(&fake, &[(1, "ready"), (3, "ready"), (4, "ready")]);
    once(&fake, &spec, 21_000).await;
    assert_eq!(
        victim(&fake).pod,
        "pod-3",
        "the next, once the first is gone"
    );
}

/// F4 of the third review: a victim asked before the spec left request
/// mode without a bound, whose container then restarted, ends its hold
/// at once: a restart needs no bound.
#[tokio::test]
async fn a_restart_after_the_mode_changed_ends_the_hold_without_a_bound() {
    let fake = three();
    let mut spec = request(2);
    once(&fake, &spec, 0).await;
    spec.retire = Some(Retire::Signal);
    spec.drain_bound = None;
    fake.state().pods[1].view.restarts = 1;
    once(&fake, &spec, 1_000).await;
    assert_eq!(fake.state().replicas, 2);
    assert!(victim(&fake).restarted);
}

/// F2: the Deployment is made again under its name and keeps its pods
/// (an orphan delete, then adoption) after the victim retired. The victim
/// cannot serve again: it is never given back its Service or its cost,
/// and its pod is deleted, without the old Deployment's count.
#[tokio::test]
async fn a_retired_victim_of_a_deployment_made_again_is_deleted_not_given_back() {
    let fake = Fake::with(&[9, 0, 9]);
    list(&fake, &[(1, "ready"), (2, "ready"), (3, "ready")]);
    let mut spec = request(2);
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    once(&fake, &spec, 2_000).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    fake.state().deployment = "deployment-b".into();
    fake.take_writes();
    spec.replicas = Some(3);
    once(&fake, &spec, 3_000).await;
    let writes = fake.take_writes();
    assert_eq!(writes, ["delete pod-2"]);
    assert_eq!(victim(&fake).phase, RetiringPhase::Deleting);
    assert_eq!(
        status(&fake).last.expect("ended").outcome,
        Outcome::Abandoned
    );
    assert_eq!(fake.state().replicas, 3);
    fake.settle(false);
    fake.exit();
    once(&fake, &spec, 4_000).await;
    assert!(status(&fake).retiring.is_empty());
    assert!(fake.take_writes().iter().all(|w| !w.starts_with("pod-2")));
}

/// F4: once the victim asked before the mode changed has gone, its entry
/// ends, and a scaler in signal mode without a bound scales again.
#[tokio::test]
async fn a_victim_gone_after_the_mode_changed_no_longer_needs_a_bound() {
    let fake = three();
    let mut spec = request(2);
    once(&fake, &spec, 0).await;
    spec.retire = Some(Retire::Signal);
    spec.drain_bound = None;
    assert_eq!(once(&fake, &spec, 1_000).await, BLOCKED);
    fake.state().pods.retain(|p| p.view.name != "pod-2");
    list(&fake, &[(1, "ready"), (3, "ready")]);
    once(&fake, &spec, 2_000).await;
    assert!(status(&fake).retiring.is_empty());
    assert_eq!(
        condition(&fake, "Blocked"),
        ("False".into(), "NotBlocked".into())
    );
    assert_eq!(status(&fake).last.expect("ended").outcome, Outcome::Lost);
    fake.take_writes();
    spec.replicas = Some(4);
    once(&fake, &spec, 3_000).await;
    assert_eq!(fake.take_writes(), ["replicas 4"]);
}

/// F2 of the fourth review: in request mode, with the bound removed, a
/// victim whose container was made again still ends its hold at once: a
/// restart needs no bound. A new victim is not asked without one.
#[tokio::test]
async fn a_restart_ends_the_hold_in_request_mode_without_a_bound() {
    let fake = three();
    let mut spec = request(2);
    once(&fake, &spec, 0).await;
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
    spec.drain_bound = None;
    fake.state().pods[1].view.containers = vec!["containerd://again".into()];
    once(&fake, &spec, 1_000).await;
    assert_eq!(fake.state().replicas, 2);
    assert!(victim(&fake).restarted);
    assert_eq!(victim(&fake).handoff, Some(Handoff::Unreported));
    // The victim is gone; no other is asked without a bound.
    fake.settle(false);
    fake.exit();
    list(&fake, &[(1, "ready"), (3, "ready")]);
    spec.replicas = Some(1);
    fake.take_writes();
    assert_eq!(once(&fake, &spec, 2_000).await, BLOCKED);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "DrainBoundRequired".into())
    );
    assert!(fake.take_writes().is_empty());
}

/// F1 of the fifth review: the victim's container restarted before it
/// was chosen, and its predecessor's registration is still listed,
/// `drained`, at the pod's IP, before the replacement. The pod is mapped
/// only to the one registration there still live, the replacement, and
/// that one is asked; the predecessor's `drained` is never its drain.
#[tokio::test]
async fn a_pod_is_asked_by_its_one_live_registration() {
    let fake = three();
    fake.state().pods[1].view.restarts = 1;
    fake.state().pods[1].view.containers = vec!["containerd://again".into()];
    list_at(
        &fake,
        &[
            ("i-1", 1, 1, "ready"),
            ("i-2", 2, 2, "drained"),
            ("i-9", 2, 9, "ready"),
            ("i-3", 3, 3, "ready"),
        ],
    );
    let spec = request(2);
    once(&fake, &spec, 0).await;
    once(&fake, &spec, 1_000).await;
    assert_eq!(fake.state().replicas, 3);
    let writes = fake.take_writes();
    assert!(!writes.iter().any(|w| w.contains("i-2")), "{writes:?}");
    assert_eq!(victim(&fake).instance_id.as_deref(), Some("i-9"));
}

/// Two live registrations at one pod's IP cannot tell which one is the
/// pod's: that pod is not chosen this round, and another is.
#[tokio::test]
async fn a_pod_with_two_live_registrations_is_not_chosen() {
    let fake = three();
    list_at(
        &fake,
        &[
            ("i-1", 1, 1, "ready"),
            ("i-2", 2, 2, "ready"),
            ("i-9", 2, 9, "ready"),
            ("i-3", 3, 3, "ready"),
        ],
    );
    let spec = request(2);
    once(&fake, &spec, 0).await;
    assert_ne!(victim(&fake).pod, "pod-2");
    let writes = fake.take_writes();
    assert!(
        !writes
            .iter()
            .any(|w| w.contains("i-2") || w.contains("i-9")),
        "{writes:?}"
    );
}

/// What [`Between`] does to the fake once.
type Change<'a> = Box<dyn FnOnce(&Fake) + Send + 'a>;

/// A probe that runs `action` on the fake once, as its first hub read
/// begins: what changes between the scaler's read of the pods and its
/// read of the hub.
struct Between<'a> {
    fake: &'a Fake,
    action: std::sync::Mutex<Option<Change<'a>>>,
}

impl Probe for Between<'_> {
    async fn instances(&self, pool: &str) -> Result<Instances, String> {
        let action = self.action.lock().expect("action").take();
        if let Some(action) = action {
            action(self.fake);
        }
        self.fake.instances(pool).await
    }

    async fn metrics(&self, ip: &str, port: u16) -> Result<String, String> {
        self.fake.metrics(ip, port).await
    }

    async fn retire(&self, pool: &str, instance: &str, incarnation: u64) -> Result<Asked, String> {
        self.fake.retire(pool, instance, incarnation).await
    }
}

/// F1 of the sixth review: the victim's container restarts after the
/// scaler read its pod and before it read the hub, so the one live
/// registration is the replacement's, and the container read is the
/// predecessor's. The step is not stored from that mixed read: the pod
/// is read again, found changed, and asked next round, by what it is
/// then. The replacement is held, not taken as restarted.
#[tokio::test]
async fn a_pod_that_changed_while_its_hub_was_read_is_asked_next_round() {
    let fake = three();
    hooks(&fake, 2, &[("kv", "pending")]);
    let spec = request(2);
    let probe = Between {
        fake: &fake,
        action: std::sync::Mutex::new(Some(Box::new(|fake: &Fake| {
            {
                let mut state = fake.state();
                let pod = &mut state.pods[1].view;
                pod.containers = vec!["containerd://again".into()];
                pod.restarts = 1;
                pod.resource_version = "2".into();
            }
            list_at(
                fake,
                &[
                    ("i-1", 1, 1, "ready"),
                    ("i-9", 2, 9, "ready"),
                    ("i-3", 3, 3, "ready"),
                ],
            );
        }))),
    };
    let victims = Victims::default();
    for at in [0, 1_000, 2_000] {
        let mut scaler = GdlScaler::new("global-front", spec.clone());
        scaler.metadata.generation = Some(1);
        scaler.status = fake.state().status.clone();
        scaler.metadata.resource_version = Some(fake.state().scaler_version.to_string());
        let cx = Context {
            cluster: &fake,
            probe: &probe,
            victims: &victims,
            clock: &move || at,
        };
        reconcile(&cx, &scaler, at).await.expect("reconciles");
    }
    assert_eq!(fake.state().replicas, 3);
    let held = victim(&fake);
    assert!(!held.restarted);
    assert_eq!(held.phase, RetiringPhase::Draining);
    assert_eq!(
        (held.instance_id.as_deref(), held.restarts),
        (Some("i-9"), Some(1))
    );
}

/// A controller that writes the chosen pod again and again, an
/// annotation of its own, moves only its resourceVersion: its process is
/// the same, so the scaler still asks it.
#[tokio::test]
async fn a_pod_written_by_another_controller_is_still_asked() {
    let fake = three();
    let spec = request(2);
    let probe = Between {
        fake: &fake,
        action: std::sync::Mutex::new(None),
    };
    let victims = Victims::default();
    for (round, at) in [0, 1_000, 2_000].into_iter().enumerate() {
        *probe.action.lock().expect("action") = Some(Box::new(move |fake: &Fake| {
            fake.state().pods[1].view.resource_version = format!("{}", 10 + round);
        }));
        let mut scaler = GdlScaler::new("global-front", spec.clone());
        scaler.metadata.generation = Some(1);
        scaler.status = fake.state().status.clone();
        scaler.metadata.resource_version = Some(fake.state().scaler_version.to_string());
        let cx = Context {
            cluster: &fake,
            probe: &probe,
            victims: &victims,
            clock: &move || at,
        };
        reconcile(&cx, &scaler, at).await.expect("reconciles");
    }
    let writes = fake.take_writes();
    assert!(writes.iter().any(|w| w == "retire i-2 2"), "{writes:?}");
    assert_eq!(victim(&fake).phase, RetiringPhase::Draining);
}

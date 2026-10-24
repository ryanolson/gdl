//! How the follower opens its watches again: after a wait that grows with
//! each failure up to a short cap, drawn apart for each follower, back
//! within the base once a watch was healthy, and with every deadline kept
//! while it waits.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use gdl_cells::CellState;
use gdl_watch::{FollowEvent, UnknownFollowEvent, WatchEnd, Watched};
use tokio::time::Instant;

use super::super::follow::{ACK_RETRY, Asked};
use super::super::served::Server;
use super::super::task::{REWATCH, REWATCH_CAP, Relaying, Source};
use super::bounded::Served;
use super::{DRAIN, M1, exposed, follow, follower, left, peer, retired, run};

/// One watch a [`Timed`] relay answers: its events, and how long it stays
/// open after them before it ends; or a refusal.
type Answer = Result<(Vec<FollowEvent>, Duration), ()>;

/// A relay that answers its watches from a script, then refuses every
/// watch, or, if `hangs`, holds every watch open with nothing in it; it
/// records when each watch was asked for, and when each acknowledgement
/// of a retirement was sent, none of which it takes. It takes the
/// follower's declaration unless `refuses`, and records when each was sent.
struct Timed {
    script: Mutex<VecDeque<Answer>>,
    hangs: bool,
    refuses: bool,
    opened: tokio::sync::watch::Sender<Vec<Instant>>,
    avoided: tokio::sync::watch::Sender<Vec<Instant>>,
    declared: tokio::sync::watch::Sender<Vec<Instant>>,
}

impl Timed {
    fn new(script: impl IntoIterator<Item = Answer>, hangs: bool) -> Self {
        Self {
            script: Mutex::new(script.into_iter().collect()),
            hangs,
            refuses: false,
            opened: tokio::sync::watch::Sender::new(Vec::new()),
            avoided: tokio::sync::watch::Sender::new(Vec::new()),
            declared: tokio::sync::watch::Sender::new(Vec::new()),
        }
    }
}

impl Source for Timed {
    async fn watch(&self) -> Result<BoxStream<'static, Relaying>, String> {
        self.opened
            .send_modify(|opened| opened.push(Instant::now()));
        match self.script.lock().expect("lock").pop_front() {
            Some(Ok((events, lives))) => Ok(futures::stream::iter(events)
                .map(|event| Ok(Watched::Event(event)))
                .chain(
                    futures::stream::once(tokio::time::sleep(lives))
                        .filter_map(|()| async { None }),
                )
                .boxed()),
            Some(Err(())) => Err("refused".into()),
            None if self.hangs => Ok(futures::stream::pending().boxed()),
            None => Err("refused".into()),
        }
    }

    async fn watch_of(&self, _: &str, _: &str) -> Result<BoxStream<'static, Relaying>, String> {
        Err("no reading".into())
    }

    async fn declare(&self) -> Result<(), String> {
        self.declared
            .send_modify(|declared| declared.push(Instant::now()));
        if self.refuses {
            return Err("no extension".into());
        }
        Ok(())
    }

    fn acknowledge(&self, _: gdl_watch::Unoffered) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn avoid(&self, _: gdl_watch::Avoids) -> BoxFuture<'static, Result<(), String>> {
        self.avoided
            .send_modify(|avoided| avoided.push(Instant::now()));
        Box::pin(async { Err("not taken".into()) })
    }
}

/// The most watches a reader asks for in `window` while every one fails
/// at once: each wait is at least half its step, and the steps double
/// from [`REWATCH`] to [`REWATCH_CAP`].
fn most_attempts(window: Duration) -> usize {
    let (mut step, mut elapsed, mut attempts) = (REWATCH, Duration::ZERO, 1);
    loop {
        elapsed += step / 2;
        if elapsed > window {
            return attempts;
        }
        attempts += 1;
        step = (step * 2).min(REWATCH_CAP);
    }
}

/// A hub that takes each connection and closes it is asked for the relay
/// a bounded number of times in 5 s, set by the cap, rather than once
/// every 100 ms. On the real clock, since the watch client's own timers
/// would run ahead on a paused one while it waits on the socket; the 5 s
/// window is the measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_that_closes_every_watch_is_asked_a_bounded_number_of_times() {
    const WINDOW: Duration = Duration::from_secs(5);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let address = listener.local_addr().expect("an address").to_string();
    let accepted = std::sync::Arc::new(AtomicUsize::new(0));
    let counted = std::sync::Arc::clone(&accepted);
    let closer = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });
    let (follows, _, _, _) = follower();
    let task = tokio::spawn(run(Served::at(&address), follows));
    tokio::time::sleep(WINDOW).await;
    task.abort();
    closer.abort();
    let attempts = accepted.load(Ordering::SeqCst);
    assert!(
        (3..=most_attempts(WINDOW)).contains(&attempts),
        "{attempts} watches in {WINDOW:?}, at most {} allowed",
        most_attempts(WINDOW)
    );
}

/// A hub that refuses the follower's declaration, as one without the
/// cell's extension does, is asked as a hub that refuses the watch is: a
/// bounded number of times in 5 s, set by the cap.
#[tokio::test(start_paused = true)]
async fn a_hub_that_refuses_the_declaration_is_asked_a_bounded_number_of_times() {
    const WINDOW: Duration = Duration::from_secs(5);
    let (follows, _, _, _) = follower();
    let source = Timed {
        refuses: true,
        ..Timed::new([], false)
    };
    let declared = source.declared.subscribe();
    let task = tokio::spawn(run(source, follows));
    tokio::time::sleep(WINDOW).await;
    task.abort();
    let attempts = declared.borrow().len();
    assert!(
        (3..=most_attempts(WINDOW)).contains(&attempts),
        "{attempts} declarations in {WINDOW:?}, at most {} allowed",
        most_attempts(WINDOW)
    );
}

/// While the relay is refused and its follower waits at the cap, an
/// unregistration falls due inside a wait and is made at its instant, not
/// when the wait ends, and an acknowledgement of a retirement the hub
/// does not take is sent again every [`ACK_RETRY`] throughout.
#[tokio::test(start_paused = true)]
async fn deadlines_are_kept_while_the_relay_waits_at_its_cap() {
    let (follows, cells, _, _) = follower();
    let (x, y) = (peer(), peer());
    let snapshot = vec![
        follow(M1, 1, CellState::Ready),
        exposed(M1, 1, &x),
        exposed(M1, 1, &y),
        FollowEvent::Synced,
        retired(M1, 1, &y),
        left(M1, 1, &x),
    ];
    let source = Timed::new([Ok((snapshot, Duration::ZERO))], false);
    let mut opened = source.opened.subscribe();
    let mut avoided = source.avoided.subscribe();
    let task = tokio::spawn(run(source, follows));
    let mut changes = cells.changes();
    changes
        .wait_for(|_| cells.is_registered(x.instance_id()))
        .await
        .expect("the task runs");
    tokio::time::timeout(
        DRAIN * 2,
        changes.wait_for(|_| !cells.is_registered(x.instance_id())),
    )
    .await
    .expect("the unregistration was made")
    .expect("the task runs");
    let unregistered = Instant::now();
    let first = opened.borrow()[0];
    let due = first + DRAIN;
    assert!(
        unregistered.duration_since(due) <= Duration::from_millis(1),
        "unregistered {:?} after it fell due",
        unregistered.duration_since(due)
    );
    // The wait the deadline fell in is one at the cap.
    let before = *opened
        .borrow()
        .iter()
        .rev()
        .find(|at| **at < due)
        .expect("a watch before the deadline");
    let after = *opened
        .wait_for(|opened| opened.last().is_some_and(|at| *at > due))
        .await
        .expect("the task runs")
        .last()
        .expect("a watch after the deadline");
    assert!(
        after - before >= REWATCH_CAP / 2,
        "the deadline fell in a wait of {:?}",
        after - before
    );
    let sent = avoided
        .wait_for(|sent| sent.last().is_some_and(|at| *at >= after))
        .await
        .expect("the task runs")
        .clone();
    let gaps: BTreeSet<Duration> = sent.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert_eq!(
        gaps,
        BTreeSet::from([ACK_RETRY]),
        "the acknowledgement was not sent again on time"
    );
    task.abort();
}

/// A healthy relay watch that ends, as when its hub restarts once, is
/// asked for again within the base wait; so is one after a run of
/// refusals, once a watch has then been healthy for 30 s.
#[tokio::test(start_paused = true)]
async fn a_healthy_watch_is_asked_for_again_within_the_base() {
    let snapshot = || vec![follow(M1, 1, CellState::Ready), FollowEvent::Synced];
    for refusals in [0, 6] {
        let (follows, _, _, _) = follower();
        let script = std::iter::repeat_n(Err(()), refusals)
            .chain([Ok((snapshot(), Duration::from_secs(30)))]);
        let source = Timed::new(script, true);
        let mut opened = source.opened.subscribe();
        let task = tokio::spawn(run(source, follows));
        let opens = opened
            .wait_for(|opened| opened.len() > refusals + 1)
            .await
            .expect("the task runs")
            .clone();
        let ended = opens[refusals] + Duration::from_secs(30);
        let again = opens[refusals + 1] - ended;
        assert!(
            again <= REWATCH,
            "after {refusals} refusals, a healthy watch was asked for again after {again:?}"
        );
        task.abort();
    }
}

/// Many followers whose watches end at the same instant do not all ask
/// again at the same instant: each draws its own wait, over the window
/// from half the base to the base.
#[tokio::test(start_paused = true)]
async fn followers_that_fail_together_spread_their_next_watch() {
    let start = Instant::now();
    let mut tasks = Vec::new();
    let mut opens = Vec::new();
    for _ in 0..32 {
        let (follows, _, _, _) = follower();
        let source = Timed::new([Ok((vec![FollowEvent::Synced], Duration::ZERO))], true);
        opens.push(source.opened.subscribe());
        tasks.push(tokio::spawn(run(source, follows)));
    }
    let mut again = BTreeSet::new();
    for opened in &mut opens {
        let at = opened
            .wait_for(|opened| opened.len() >= 2)
            .await
            .expect("the task runs")[1];
        let waited = at - start;
        assert!(
            waited >= REWATCH / 2 && waited <= REWATCH,
            "asked again after {waited:?}"
        );
        again.insert(waited);
    }
    assert!(again.len() >= 16, "only {} distinct waits", again.len());
    let spread = *again.last().expect("waits") - *again.first().expect("waits");
    assert!(spread >= REWATCH / 8, "the waits span only {spread:?}");
    for task in tasks {
        task.abort();
    }
}

/// A cell read from a watch of its own that keeps ending is asked for
/// again after waits that grow to the cap, each within its step; one that
/// an event after its snapshot replaces is asked for at once.
#[test]
fn a_cells_own_watch_that_keeps_ending_waits_longer_each_time() {
    let (mut follows, _, _, _) = follower();
    let now = Instant::now();
    super::live(&mut follows, now, [follow(M1, 1, CellState::Ready)]);
    follows.unknown(UnknownFollowEvent {
        kind: "paused".into(),
        class: "model_x".into(),
        cell: M1.into(),
        generation: 1,
    });
    let Asked { key, watch, pause } = follows.asked().pop().expect("a watch of its own");
    assert_eq!(pause, Duration::ZERO, "the first reading is at once");
    let mut watch = watch;
    let mut step = REWATCH;
    for n in 0..8 {
        follows.own(key.clone(), watch, Err(WatchEnd::Closed), now);
        let asked = follows.asked().pop().expect("a watch of its own again");
        assert!(
            asked.pause >= step / 2 && asked.pause <= step,
            "reading {n} waited {:?}, its step {step:?}",
            asked.pause
        );
        watch = asked.watch;
        step = (step * 2).min(REWATCH_CAP);
    }
}

/// A relay watch that ran for 30 s after its snapshot and then failed, at
/// an event that does not parse, advances the backoff as any failure
/// does: its healthy life returns the step to the base, and the failure
/// doubles it, so the wait after the next watch, which is refused, is
/// drawn from twice the base. The control: the same watch ended cleanly
/// by its hub leaves the step at the base, so that wait is drawn from the
/// base. On a paused clock: the server runs on this test's one thread, so
/// what it writes is read before the clock moves.
#[tokio::test(start_paused = true)]
async fn a_failure_after_a_healthy_span_advances_the_backoff() {
    for failed in [true, false] {
        let (follows, _, _, _) = follower();
        let mut server = Server::new().await;
        let task = tokio::spawn(run(Served::at(&server.address), follows));
        let mut first = server.next().await;
        first.answer().await;
        first.send(&FollowEvent::Synced).await;
        // Within the watch's idle bound, for longer than the healthy span.
        for _ in 0..7 {
            tokio::time::sleep(Duration::from_secs(5)).await;
            first.comment().await;
        }
        if failed {
            first.raw("not json").await;
        } else {
            drop(first);
        }
        let refused = server.next().await;
        let at = refused.at;
        drop(refused);
        let third = server.next().await;
        let waited = third.at - at;
        let (least, most) = if failed {
            (REWATCH, REWATCH * 2)
        } else {
            (REWATCH / 2, REWATCH)
        };
        assert!(
            waited >= least && waited <= most,
            "failed: {failed}; after the refusal that followed, waited {waited:?}"
        );
        task.abort();
    }
}

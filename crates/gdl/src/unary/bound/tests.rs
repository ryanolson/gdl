//! A segment's services and the clients its nodes take: what a build may
//! take, a call during the build, a call that waits for the service's
//! readiness, and a call once the service is ready.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

use super::*;

/// Counts its words; its `ready` runs once, and ends when `open` is
/// notified, or at once without one, with `fails` as its error.
#[derive(Default)]
struct Words {
    open: Option<Arc<Notify>>,
    fails: Option<ErrorKind>,
    readied: AtomicUsize,
    called: AtomicUsize,
}

impl Build for Words {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self::default())
    }

    async fn ready(&self) -> Result<()> {
        self.readied.fetch_add(1, Ordering::Relaxed);
        if let Some(open) = &self.open {
            open.notified().await;
        }
        match self.fails {
            Some(kind) => Err(Error::new(kind, "not loaded")),
            None => Ok(()),
        }
    }
}

impl Unary for Words {
    type Req = String;
    type Resp = usize;

    async fn call(&self, _cx: &Cx, text: String) -> Result<usize> {
        self.called.fetch_add(1, Ordering::Relaxed);
        Ok(text.split_whitespace().count())
    }
}

/// A table where `Router` calls `Words`, which it hosts.
fn hosting(words: Arc<Words>) -> UnaryServices {
    let services = UnaryServices::new(
        "front",
        [
            ("Router".to_string(), vec!["Words".to_string()]),
            ("Words".to_string(), Vec::new()),
        ],
    );
    services.serve("Words", words).expect("served");
    services
}

fn take<Req: Send + 'static, Resp: Send + 'static>(
    services: &UnaryServices,
    node: &str,
    name: &str,
) -> Result<UnaryClient<Req, Resp>> {
    futures::executor::block_on(
        services.scope(node, async { UnaryClient::<Req, Resp>::current(name) }),
    )
}

#[test]
fn a_build_takes_the_client_its_calls_names() {
    let services = hosting(Arc::new(Words::default()));
    take::<String, usize>(&services, "Router", "Words").expect("the client");
}

#[test]
fn a_client_is_taken_only_in_a_build() {
    let error = UnaryClient::<String, usize>::current("Words").expect_err("no build");
    assert_eq!(error.kind(), ErrorKind::Placement);
    assert!(
        error.message().contains("only in a node's build"),
        "{error}"
    );
}

#[test]
fn a_build_that_takes_a_client_its_calls_does_not_name_fails() {
    let services = hosting(Arc::new(Words::default()));
    let error = take::<String, usize>(&services, "Words", "Words").expect_err("not named");
    assert_eq!(error.kind(), ErrorKind::Placement);
    assert_eq!(
        error.message(),
        "node `Words` takes a client of unary node `Words`, but its `calls` does not name `Words`"
    );
    // A node the graph does not know names nothing.
    let error = take::<String, usize>(&services, "Adapter", "Words").expect_err("not named");
    assert_eq!(error.kind(), ErrorKind::Placement);
}

#[test]
fn a_build_that_takes_a_client_of_other_types_fails() {
    let services = hosting(Arc::new(Words::default()));
    let error = take::<String, u64>(&services, "Router", "Words").expect_err("other types");
    assert_eq!(error.kind(), ErrorKind::Placement);
    assert_eq!(
        error.message(),
        "node `Router` takes a client of unary node `Words` as `UnaryClient<alloc::string::String, u64>`, but `Words` takes `alloc::string::String` and answers `usize`"
    );
}

#[test]
fn a_service_the_segment_neither_hosts_nor_links_is_refused() {
    let services = UnaryServices::new("front", [("Router".to_string(), vec!["Words".to_string()])]);
    let error = take::<String, usize>(&services, "Router", "Words").expect_err("no copy");
    assert_eq!(error.kind(), ErrorKind::Placement);
    assert!(error.message().contains("no copy of `Words`"), "{error}");
}

#[test]
fn a_service_is_hosted_once() {
    let services = hosting(Arc::new(Words::default()));
    let error = services
        .serve("Words", Arc::new(Words::default()))
        .expect_err("twice");
    assert_eq!(error.kind(), ErrorKind::Placement);
}

#[test]
fn a_call_during_the_build_fails_at_once_with_routing() {
    let words = Arc::new(Words::default());
    let services = hosting(Arc::clone(&words));
    let called = futures::executor::block_on(services.scope("Router", async {
        let client = UnaryClient::<String, usize>::current("Words")?;
        client.call(&Cx::new(), "a b".to_string()).await
    }));
    let error = called.expect_err("called during build");
    assert_eq!(error.kind(), ErrorKind::Routing);
    assert_eq!(error.message(), "called during build");
    assert_eq!(words.called.load(Ordering::Relaxed), 0);
    assert_eq!(words.readied.load(Ordering::Relaxed), 0, "ready ran early");
}

/// A call from another node's `ready` waits for the service's own, which
/// runs once, then is made.
#[tokio::test]
async fn a_call_after_the_build_waits_for_the_services_ready() {
    let open = Arc::new(Notify::new());
    let words = Arc::new(Words {
        open: Some(Arc::clone(&open)),
        ..Words::default()
    });
    let services = hosting(Arc::clone(&words));
    let client = take::<String, usize>(&services, "Router", "Words").expect("the client");
    services.built();
    let cx = Cx::new();
    let mut call = std::pin::pin!(client.call(&cx, "a b c".to_string()));
    assert!(futures::poll!(call.as_mut()).is_pending(), "not ready yet");
    assert_eq!(words.called.load(Ordering::Relaxed), 0);
    let ready = services.ready();
    open.notify_one();
    let (called, ready) = tokio::join!(call, ready);
    assert_eq!(called.expect("called once ready"), 3);
    ready.expect("ready");
    assert_eq!(words.readied.load(Ordering::Relaxed), 1, "ready runs once");
    assert_eq!(words.called.load(Ordering::Relaxed), 1);
    // Once ready, a call goes straight to the service.
    assert_eq!(client.call(&cx, "d".to_string()).await.expect("ready"), 1);
    assert_eq!(words.readied.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_call_waiting_on_a_ready_that_failed_fails_with_its_error() {
    let words = Arc::new(Words {
        fails: Some(ErrorKind::Node),
        ..Words::default()
    });
    let services = hosting(Arc::clone(&words));
    let client = take::<String, usize>(&services, "Router", "Words").expect("the client");
    services.built();
    let error = client
        .call(&Cx::new(), "a".to_string())
        .await
        .expect_err("ready failed");
    assert_eq!(error.kind(), ErrorKind::Node);
    assert_eq!(error.message(), "not loaded");
    let ready = services.ready().await.expect_err("ready failed");
    assert_eq!(ready.kind(), ErrorKind::Node);
    assert_eq!(words.readied.load(Ordering::Relaxed), 1, "ready runs once");
    assert_eq!(words.called.load(Ordering::Relaxed), 0);
}

/// A service added after the build waits for its readiness too.
#[tokio::test]
async fn a_service_served_after_the_build_is_ready_after_its_own_ready() {
    let services = UnaryServices::new("front", [("Router".to_string(), vec!["Words".to_string()])]);
    services.built();
    let words = Arc::new(Words::default());
    services.serve("Words", Arc::clone(&words)).expect("served");
    let client = take::<String, usize>(&services, "Router", "Words").expect("the client");
    assert_eq!(
        client
            .call(&Cx::new(), "a b".to_string())
            .await
            .expect("called"),
        2
    );
    assert_eq!(words.readied.load(Ordering::Relaxed), 1);
}

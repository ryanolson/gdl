//! A node's closure runs inside a scrape: the listener must keep answering
//! probes while it runs, and answer the scrape when it overruns.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use tokio::sync::Notify;

use super::*;

/// The longest a test waits for something it expects.
const WAIT: Duration = Duration::from_secs(5);

/// How long a held closure waits for its release before it gives up, so
/// that a listener that blocks its worker fails the test, not hangs it.
const HOLD: Duration = Duration::from_secs(3);

#[derive(gdl::Metrics)]
struct GateMetrics {
    /// 1 for each model's status.
    #[gdl(labels = ["model"])]
    model_status: gdl::ScrapeGaugeVec,
}

/// A closure that the test holds inside the scrape until it releases it.
struct Held {
    /// Told each time the closure starts.
    entered: Arc<Notify>,
    /// Closures started.
    runs: Arc<AtomicUsize>,
    /// Closures that waited out `HOLD` unreleased.
    unreleased: Arc<AtomicUsize>,
    release: mpsc::Sender<()>,
}

impl Held {
    fn fill(gauge: &gdl::ScrapeGaugeVec) -> Self {
        let (release, released) = mpsc::channel();
        let released = Mutex::new(released);
        let entered = Arc::new(Notify::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let unreleased = Arc::new(AtomicUsize::new(0));
        let (tell, run, missed) = (entered.clone(), runs.clone(), unreleased.clone());
        gauge.fill_with(move |gauges| {
            run.fetch_add(1, Ordering::SeqCst);
            tell.notify_one();
            let released = released.lock().expect("the release");
            if released.recv_timeout(HOLD).is_err() {
                missed.fetch_add(1, Ordering::SeqCst);
            }
            gauges.set(&["llama"], 1.0).expect("one label");
        });
        Self {
            entered,
            runs,
            unreleased,
            release,
        }
    }
}

/// A listener on loopback serving `health`.
async fn serve(health: Arc<Health>) -> SocketAddr {
    let settings = SystemSettings {
        host: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        ..SystemSettings::default()
    };
    listen(&settings, health)
        .await
        .expect("bound")
        .expect("a listener")
}

/// A health with a registry holding `GateMetrics`.
fn health(limit: Duration) -> (Arc<Health>, GateMetrics) {
    let registry = prometheus::Registry::new();
    let metrics = <GateMetrics as gdl::Metrics>::register(&registry, "Gate").expect("registered");
    let health = Health {
        scrape_limit: limit,
        ..Health::new()
    };
    assert!(health.serve_metrics(registry));
    (Arc::new(health), metrics)
}

/// The status code and body of `GET path`, or why there is none.
async fn get(address: SocketAddr, path: &str) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(address).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response = String::from_utf8_lossy(&response).into_owned();
    let code = response
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| std::io::Error::other(format!("no status line: {response:?}")))?;
    let body = response
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, body)| body.to_string());
    Ok((code, body))
}

/// One worker thread: a scrape that ran its closure there would hold up
/// every other connection until the closure returned.
#[tokio::test(flavor = "current_thread")]
async fn a_held_scrape_holds_up_no_probe() {
    let (health, metrics) = health(SCRAPE_LIMIT);
    let held = Held::fill(&metrics.model_status);
    let address = serve(health).await;
    let scrape = tokio::spawn(async move { get(address, "/metrics").await });
    tokio::time::timeout(WAIT, held.entered.notified())
        .await
        .expect("the scrape ran the closure");
    let probe = tokio::time::timeout(WAIT, get(address, "/live"))
        .await
        .expect("the probe answered")
        .expect("a response");
    assert_eq!(probe.0, 200);
    assert_eq!(
        held.unreleased.load(Ordering::SeqCst),
        0,
        "the probe answered only after the closure gave up"
    );
    held.release.send(()).expect("the closure waits");
    let (code, body) = tokio::time::timeout(WAIT, scrape)
        .await
        .expect("the scrape answered")
        .expect("joined")
        .expect("a response");
    assert_eq!(code, 200, "{body}");
    assert!(
        body.contains("model_status{dynamo_endpoint=\"Gate\",model=\"llama\"} 1"),
        "{body}"
    );
}

/// A closure may take a blocking read of a tokio lock, which panics on a
/// runtime worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_closure_may_block_on_a_tokio_lock() {
    let (health, metrics) = health(SCRAPE_LIMIT);
    let status = Arc::new(tokio::sync::RwLock::new(1.0));
    let state = status.clone();
    metrics.model_status.fill_with(move |gauges| {
        let value = *state.blocking_read();
        gauges.set(&["llama"], value).expect("one label");
    });
    let address = serve(health).await;
    let (code, body) = tokio::time::timeout(WAIT, get(address, "/metrics"))
        .await
        .expect("the scrape answered")
        .expect("a response, not a dropped connection");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("model=\"llama\"} 1"), "{body}");
}

/// A scrape that overruns its limit is answered with 503, and a scrape
/// that comes while it still runs waits for it rather than starting the
/// closure again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_overrunning_scrape_is_answered_and_not_repeated() {
    let limit = Duration::from_millis(200);
    let (health, metrics) = health(limit);
    let held = Held::fill(&metrics.model_status);
    let address = serve(health).await;
    for _ in 0..2 {
        let (code, body) = tokio::time::timeout(WAIT, get(address, "/metrics"))
            .await
            .expect("the scrape answered")
            .expect("a response");
        assert_eq!(code, 503, "{body}");
        assert!(body.contains("longer than"), "{body}");
    }
    assert_eq!(held.runs.load(Ordering::SeqCst), 1, "one closure at a time");
    held.release.send(()).expect("the closure waits");
    // Once the held closure returns, the next scrape runs it again.
    held.release.send(()).expect("the closure waits");
    let (code, body) = tokio::time::timeout(WAIT, get(address, "/metrics"))
        .await
        .expect("the scrape answered")
        .expect("a response");
    assert_eq!(code, 200, "{body}");
    assert_eq!(held.runs.load(Ordering::SeqCst), 2);
    assert_eq!(held.unreleased.load(Ordering::SeqCst), 0);
}

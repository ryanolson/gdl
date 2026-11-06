use super::*;

use std::{
    net::{SocketAddr, TcpListener as StdListener},
    process::Stdio,
    sync::Mutex,
};

use etcd_client::{Client, ConnectOptions, WatchOptions};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    task::{AbortHandle, JoinHandle, JoinSet},
};

// Only Raft traffic is cut: client keepalive still succeeds throughout the wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_leaves_a_member_that_loses_its_leader() {
    let cluster = Cluster::start().await;
    let writer = open(&cluster.urls[1], "/gdl/pools", LIMITS).await;
    let reader = EtcdDirectory::with_limits(cluster.urls.clone(), "/gdl/pools".into(), LIMITS)
        .await
        .expect("a directory");
    let first = record("m1", 1, 1, 0);
    writer
        .publish(first.clone())
        .await
        .expect("the first write");
    let mut live = watch(&reader, MODEL, None);
    assert_eq!(
        snapshot(&mut live).await.0.as_slice(),
        std::slice::from_ref(&first)
    );

    let mut control = Client::connect(
        [&cluster.urls[0]],
        Some(ConnectOptions::new().with_require_leader(true)),
    )
    .await
    .expect("a client that requires a leader");
    let mut control = control
        .watch(
            "/gdl/pools/model_x/",
            Some(WatchOptions::new().with_prefix()),
        )
        .await
        .expect("a control watch");
    assert!(
        control
            .message()
            .await
            .expect("a response")
            .expect("created")
            .created()
    );

    let mut isolated = Client::connect([&cluster.urls[0]], None)
        .await
        .expect("a client");
    let status = isolated.status().await.expect("the first member's status");
    let id = status.header().expect("a header").member_id();
    for peer in &cluster.peers {
        peer.isolate(id);
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if isolated
                .status()
                .await
                .expect("the member still answers")
                .leader()
                == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the isolated member loses its leader");
    let stopped = tokio::time::timeout(Duration::from_secs(4), control.message())
        .await
        .expect("the control watch notices the lost leader");
    assert!(
        stopped.is_err() || stopped.as_ref().is_ok_and(|event| event.is_none()),
        "{stopped:?}"
    );

    let latest = heartbeat(&first, 2);
    writer
        .publish(latest.clone())
        .await
        .expect("the majority takes a write");
    assert_eq!(
        writer
            .last_write(MODEL, "m1")
            .await
            .expect("the majority answers"),
        Some(LastWrite::Record(Box::new(latest.clone())))
    );
    let event = tokio::time::timeout(Duration::from_secs(3), live.next()).await;
    assert!(
        matches!(&event, Ok(Some(Ok(PoolEvent::Put { record, .. }))) if record == &latest),
        "the watch must use a healthy member while its first member still answers TCP: {event:?}"
    );
}

struct Cluster {
    children: Vec<Child>,
    peers: Vec<Peer>,
    urls: Vec<String>,
    _data: tempfile::TempDir,
}

impl Cluster {
    async fn start() -> Self {
        let data = tempfile::Builder::new()
            .prefix("gdl-etcd-cluster-")
            .tempdir()
            .expect("a data directory");
        let clients: Vec<_> = (0..3).map(|_| free()).collect();
        let addresses: Vec<_> = (0..3).map(|_| free()).collect();
        let mut peers = Vec::new();
        for (index, upstream) in addresses.iter().copied().enumerate() {
            peers.push(Peer::to(upstream, index == 0).await);
        }
        let initial = peers
            .iter()
            .enumerate()
            .map(|(n, peer)| format!("m{n}=http://{}", peer.address))
            .collect::<Vec<_>>()
            .join(",");
        let binary = std::env::var("GDL_ETCD").expect("an etcd binary");
        let urls: Vec<_> = clients
            .iter()
            .map(|client| format!("http://{client}"))
            .collect();
        let mut children = Vec::new();
        for n in 0..3 {
            children.push(
                Command::new(&binary)
                    .arg(format!("--name=m{n}"))
                    .arg(format!(
                        "--data-dir={}",
                        data.path().join(format!("m{n}")).display()
                    ))
                    .arg(format!("--listen-client-urls={}", urls[n]))
                    .arg(format!("--advertise-client-urls={}", urls[n]))
                    .arg(format!("--listen-peer-urls=http://{}", addresses[n]))
                    .arg(format!(
                        "--initial-advertise-peer-urls=http://{}",
                        peers[n].address
                    ))
                    .arg(format!("--initial-cluster={initial}"))
                    .args([
                        "--heartbeat-interval=50",
                        "--election-timeout=500",
                        "--log-level=error",
                    ])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::inherit())
                    .kill_on_drop(true)
                    .spawn()
                    .expect("etcd starts"),
            );
        }
        let cluster = Self {
            children,
            peers,
            urls,
            _data: data,
        };
        for url in &cluster.urls {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let mut raw = Client::connect([url], None).await.expect("a client");
                    if tokio::time::timeout(Duration::from_millis(500), raw.get("/ready", None))
                        .await
                        .is_ok_and(|result| result.is_ok())
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("every member serves a linearizable read");
        }
        cluster
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.start_kill();
        }
    }
}

fn free() -> SocketAddr {
    StdListener::bind("127.0.0.1:0")
        .expect("a listener")
        .local_addr()
        .expect("an address")
}

struct Peer {
    address: SocketAddr,
    accept: JoinHandle<()>,
    state: Arc<Mutex<Links>>,
}

#[derive(Default)]
struct Links {
    target: bool,
    blocked: Option<u64>,
    forwards: Vec<(u64, AbortHandle)>,
}

impl Peer {
    async fn to(upstream: SocketAddr, target: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a peer proxy");
        let address = listener.local_addr().expect("an address");
        let state = Arc::new(Mutex::new(Links {
            target,
            ..Links::default()
        }));
        let forwarding = Arc::clone(&state);
        let accept = tokio::spawn(async move {
            let mut headers = JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let Ok((mut inbound, _)) = incoming else { return };
                        let forwarding = Arc::clone(&forwarding);
                        headers.spawn(async move {
                            let mut request = Vec::new();
                            let end = loop {
                                let mut buffer = [0; 1_024];
                                let Ok(n) = inbound.read(&mut buffer).await else { return };
                                if n == 0 { return; }
                                request.extend_from_slice(&buffer[..n]);
                                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                                    break end + 4;
                                }
                            };
                            let text = String::from_utf8_lossy(&request[..end]);
                            let source = text.lines().find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("X-Server-From")
                                    .then(|| u64::from_str_radix(value.trim(), 16).ok()).flatten()
                            }).unwrap_or(0);
                            let mut state = forwarding.lock().expect("the peer links");
                            if state.blocked.is_some_and(|id| state.target || id == source) {
                                return;
                            }
                            let task = tokio::spawn(async move {
                                let Ok(mut outbound) = TcpStream::connect(upstream).await else { return };
                                if outbound.write_all(&request).await.is_ok() {
                                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                                }
                            });
                            state.forwards.push((source, task.abort_handle()));
                        });
                    }
                    _ = headers.join_next(), if !headers.is_empty() => {}
                }
            }
        });
        Self {
            address,
            accept,
            state,
        }
    }

    fn isolate(&self, id: u64) {
        let mut state = self.state.lock().expect("the peer links");
        state.blocked = Some(id);
        for (source, task) in &state.forwards {
            if state.target || *source == id {
                task.abort();
            }
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.accept.abort();
        for (_, task) in &self.state.lock().expect("the peer links").forwards {
            task.abort();
        }
    }
}

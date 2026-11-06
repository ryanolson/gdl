//! The etcd processes the cases run against, each behind a TCP proxy that a
//! case can cut, reset, or point at another store. Every case starts its
//! own, on OS-assigned ports, from the binary `GDL_ETCD` names
//! (`scripts/etcd.sh` sets it), so that a case that restarts or replaces
//! its store disturbs no other.

use std::{
    net::{SocketAddr, TcpListener as StdListener},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    task::{AbortHandle, JoinSet},
};

/// How long a store has to answer after it starts.
const READY: Duration = Duration::from_secs(20);

/// One etcd process over its own data directory, killed on drop.
pub struct Etcd {
    child: Child,
    pub client: SocketAddr,
    data: PathBuf,
    flags: Vec<String>,
}

impl Etcd {
    /// A new etcd over a new, empty data directory under `dir`, with the
    /// server flags the book prescribes and `flags` after them.
    pub async fn start(dir: &Path, flags: &[&str]) -> Self {
        let data = tempfile::Builder::new()
            .prefix("gdl-etcd-data-")
            .tempdir_in(dir)
            .expect("a data directory")
            // Kept past this handle, so that a restart reuses it: the
            // store's own `TempDir`, which holds it, removes it.
            .keep();
        Self::over(data, flags.iter().map(|flag| flag.to_string()).collect()).await
    }

    async fn over(data: PathBuf, flags: Vec<String>) -> Self {
        let binary =
            std::env::var("GDL_ETCD").expect("GDL_ETCD names the etcd binary: run scripts/etcd.sh");
        let client = free();
        let peer = free();
        let child = Command::new(binary)
            .arg("--name=default")
            .arg(format!("--data-dir={}", data.display()))
            .arg(format!("--listen-client-urls=http://{client}"))
            .arg(format!("--advertise-client-urls=http://{client}"))
            .arg(format!("--listen-peer-urls=http://{peer}"))
            .arg(format!("--initial-advertise-peer-urls=http://{peer}"))
            .arg(format!("--initial-cluster=default=http://{peer}"))
            .arg("--auto-compaction-mode=periodic")
            .arg("--auto-compaction-retention=1h")
            .arg("--quota-backend-bytes=8589934592")
            .arg("--log-level=error")
            .args(&flags)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("etcd starts");
        let etcd = Self {
            child,
            client,
            data,
            flags,
        };
        etcd.ready().await;
        etcd
    }

    /// Wait until the store answers a read: a precondition shown by a
    /// signal, not by a sleep.
    async fn ready(&self) {
        let began = Instant::now();
        let url = format!("http://{}", self.client);
        loop {
            let answered = async {
                let mut client = etcd_client::Client::connect([url.as_str()], None)
                    .await
                    .ok()?;
                client.get("/", None).await.ok()
            };
            if tokio::time::timeout(Duration::from_secs(1), answered)
                .await
                .ok()
                .flatten()
                .is_some()
            {
                return;
            }
            assert!(
                began.elapsed() < READY,
                "etcd at {url} did not answer within {READY:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Stop the process, and start it again over the same data directory,
    /// on new ports.
    pub async fn restart(mut self) -> Self {
        self.child.kill().await.expect("etcd is killed");
        Self::over(self.data.clone(), self.flags.clone()).await
    }

    /// The store's client URL, past the proxy.
    pub fn client_url(&self) -> String {
        format!("http://{}", self.client)
    }

    /// A client straight to this store, past the proxy.
    pub async fn raw(&self) -> etcd_client::Client {
        let url = format!("http://{}", self.client);
        etcd_client::Client::connect([url.as_str()], None)
            .await
            .expect("a raw client")
    }
}

impl Drop for Etcd {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// An OS-assigned port on loopback, free when this returns.
fn free() -> SocketAddr {
    let listener = StdListener::bind("127.0.0.1:0").expect("a free port");
    listener.local_addr().expect("its address")
}

/// A TCP proxy on an OS-assigned port, to one store at a time.
pub struct Proxy {
    pub address: SocketAddr,
    state: Arc<Mutex<ProxyState>>,
    accept: AbortHandle,
}

struct ProxyState {
    upstream: Option<SocketAddr>,
    links: JoinSet<()>,
}

impl Proxy {
    pub async fn to(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a proxy");
        let address = listener.local_addr().expect("its address");
        let state = Arc::new(Mutex::new(ProxyState {
            upstream: Some(upstream),
            links: JoinSet::new(),
        }));
        let accepting = Arc::clone(&state);
        let accept = tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    return;
                };
                let mut state = accepting.lock().expect("the proxy's state");
                // While cut, a connection is closed as it arrives.
                let Some(upstream) = state.upstream else {
                    continue;
                };
                state.links.spawn(link(inbound, upstream));
            }
        })
        .abort_handle();
        Self {
            address,
            state,
            accept,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Close every connection, and refuse new ones until [`Proxy::switch`].
    pub fn cut(&self) {
        let mut state = self.state.lock().expect("the proxy's state");
        state.upstream = None;
        state.links.abort_all();
    }

    /// Close every connection; new ones reach the same store.
    pub fn reset(&self) {
        self.state
            .lock()
            .expect("the proxy's state")
            .links
            .abort_all();
    }

    /// Close every connection; new ones reach `upstream`.
    pub fn switch(&self, upstream: SocketAddr) {
        self.switcher().switch(upstream);
    }

    /// A handle that switches the proxy from another task.
    pub fn switcher(&self) -> Switcher {
        Switcher {
            state: Arc::clone(&self.state),
        }
    }
}

/// Switches a [`Proxy`] to another store.
#[derive(Clone)]
pub struct Switcher {
    state: Arc<Mutex<ProxyState>>,
}

impl Switcher {
    /// Close every connection; new ones reach `upstream`.
    pub fn switch(&self, upstream: SocketAddr) {
        let mut state = self.state.lock().expect("the proxy's state");
        state.links.abort_all();
        state.upstream = Some(upstream);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
        if let Ok(mut state) = self.state.lock() {
            state.links.abort_all();
        }
    }
}

async fn link(mut inbound: TcpStream, upstream: SocketAddr) {
    let Ok(mut outbound) = TcpStream::connect(upstream).await else {
        return;
    };
    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
}

/// A store behind its proxy, with the directory that holds its data.
pub struct Store {
    pub dir: TempDir,
    pub etcd: Option<Etcd>,
    pub proxy: Proxy,
}

impl Store {
    pub async fn start(flags: &[&str]) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gdl-etcd-")
            .tempdir()
            .expect("a directory");
        let etcd = Etcd::start(dir.path(), flags).await;
        let proxy = Proxy::to(etcd.client).await;
        Self {
            dir,
            etcd: Some(etcd),
            proxy,
        }
    }

    pub fn etcd(&self) -> &Etcd {
        self.etcd.as_ref().expect("a running etcd")
    }

    /// Restart the store's process over its data, and point the proxy at it.
    pub async fn restart(&mut self) {
        self.proxy.cut();
        let etcd = self.etcd.take().expect("a running etcd").restart().await;
        self.proxy.switch(etcd.client);
        self.etcd = Some(etcd);
    }

    /// A new, empty store that the proxy does not point at yet.
    pub async fn another(&self, flags: &[&str]) -> Etcd {
        Etcd::start(self.dir.path(), flags).await
    }

    /// Point the proxy at `etcd`, which replaces the store, and keep it.
    pub fn replace(&mut self, etcd: Etcd) {
        self.proxy.switch(etcd.client);
        self.etcd = Some(etcd);
    }

    pub async fn raw(&self) -> etcd_client::Client {
        self.etcd().raw().await
    }
}

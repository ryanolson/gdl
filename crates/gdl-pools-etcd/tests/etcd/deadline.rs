use super::*;

use std::net::SocketAddr;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

// The delays exceed the snapshot's deadline without timing out a single RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paged_snapshot_has_one_deadline() {
    let store = Store::start(&[]).await;
    let writer = open(&store.etcd().client_url(), "/gdl/pools", LIMITS).await;
    for n in 0..24 {
        writer
            .publish(record(&format!("m{n}"), 1, 1, 0))
            .await
            .expect("a write");
    }
    let delayed = Delayed::to(store.etcd().client).await;
    let limits = EtcdLimits {
        request_bytes: 1_024,
        response_bytes: 1_300,
        timeout: Duration::from_millis(250),
        ..LIMITS
    };
    let reader = open(&delayed.url, "/gdl/pools", limits).await;
    let mut live = watch(&reader, MODEL, None);
    let began = tokio::time::Instant::now();
    let first = tokio::time::timeout(Duration::from_secs(5), live.next())
        .await
        .expect("the delayed store keeps answering");
    assert!(
        matches!(first, Some(Err(DirectoryError::Unavailable(_)))),
        "a 250 ms snapshot deadline must expire while pages keep arriving; after {:?}: {first:?}",
        began.elapsed(),
    );
}

struct Delayed {
    url: String,
    accept: JoinHandle<()>,
}

impl Delayed {
    async fn to(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a listener");
        let url = format!("http://{}", listener.local_addr().expect("the address"));
        let accept = tokio::spawn(async move {
            let mut links = JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let Ok((inbound, _)) = incoming else { return };
                        links.spawn(async move {
                            let outbound = TcpStream::connect(upstream).await.expect("etcd");
                            inbound.set_nodelay(true).expect("nodelay");
                            outbound.set_nodelay(true).expect("nodelay");
                            let (read_in, write_in) = inbound.into_split();
                            let (read_out, write_out) = outbound.into_split();
                            let _ = tokio::try_join!(
                                forward(read_in, write_out),
                                forward(read_out, write_in),
                            );
                        });
                    }
                    _ = links.join_next(), if !links.is_empty() => {}
                }
            }
        });
        Self { url, accept }
    }
}

impl Drop for Delayed {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn forward(
    mut read: impl AsyncRead + Unpin,
    mut write: impl AsyncWrite + Unpin,
) -> std::io::Result<()> {
    let mut buffer = [0; 8_192];
    loop {
        let bytes = read.read(&mut buffer).await?;
        if bytes == 0 {
            return write.shutdown().await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        write.write_all(&buffer[..bytes]).await?;
    }
}

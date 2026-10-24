//! A server for tests that answers every request as a hub's watch, on an
//! OS-assigned port, and hands each to the test, which chooses what it
//! sends: a peer that misbehaves as no hub of this build does.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;

/// How long a test waits for a watch to be asked for.
const WAIT: Duration = Duration::from_secs(5);

/// The server, and the watches asked of it.
pub(crate) struct Server {
    /// `host:port`, as a watcher dials it.
    pub(crate) address: String,
    accepted: mpsc::UnboundedReceiver<Served>,
}

impl Server {
    pub(crate) async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let address = listener.local_addr().expect("an address").to_string();
        let (accept, accepted) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let accept = accept.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                            return;
                        }
                        head.push(byte[0]);
                    }
                    let path = String::from_utf8_lossy(&head)
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .split('?')
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    let _ = accept.send(Served {
                        stream,
                        path,
                        at: Instant::now(),
                    });
                });
            }
        });
        Self { address, accepted }
    }

    /// The next watch asked for, unanswered until the test answers it.
    pub(crate) async fn next(&mut self) -> Served {
        tokio::time::timeout(WAIT, self.accepted.recv())
            .await
            .expect("a watch in time")
            .expect("the server runs")
    }
}

/// One watch asked of the server.
pub(crate) struct Served {
    stream: TcpStream,
    /// Its path, without the query.
    pub(crate) path: String,
    /// When it was asked for.
    pub(crate) at: Instant,
}

impl Served {
    /// Answer it as a watch, whose events follow.
    pub(crate) async fn answer(&mut self) {
        let head =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
        self.write(head.as_bytes()).await;
    }

    /// Send `event` as one server-sent event.
    pub(crate) async fn send(&mut self, event: &impl serde::Serialize) {
        let data = serde_json::to_string(event).expect("an event serializes");
        self.raw(&data).await;
    }

    /// Send `data` as one server-sent event's data.
    pub(crate) async fn raw(&mut self, data: &str) {
        self.write(format!("data: {data}\n\n").as_bytes()).await;
    }

    async fn write(&mut self, bytes: &[u8]) {
        self.stream
            .write_all(bytes)
            .await
            .expect("the watcher reads");
    }

    /// Send keep-alives, every 50 ms, until the watcher hangs up.
    pub(crate) fn keep_alive(mut self) {
        tokio::spawn(async move {
            while self.stream.write_all(b":\n\n").await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    }
}

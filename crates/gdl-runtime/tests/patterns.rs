//! The test proxy's patterns, across TCP reads, reused connections, and
//! pattern changes: a refusal that misses a request split across reads
//! lets it reach the hub, and a test that relies on it fails against a
//! correct runtime.

#[allow(dead_code)] // Only the proxy is used here, not all of it.
#[path = "common/proxy.rs"]
mod proxy;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

const SERVICE: &[u8] = b"gdl-ready-656e67696e65";
const PREFIX: &[u8] = b"PUT /v1/services/gdl-ready-656e";
const SUFFIX: &[u8] = b"67696e65/instances/00000000-0000-0000-0000-000000000002 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
const HEARTBEAT: &[u8] = b"POST /v1/instances/00000000-0000-0000-0000-000000000002/heartbeat HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

/// A request that does not hold the pattern passes, and the refused one
/// after it on the same connection does not.
#[tokio::test]
async fn a_ready_refusal_leaves_heartbeats_on_a_reused_connection_alone() {
    check_refusal(false).await;
}

/// A service name split across two reads is refused all the same.
#[tokio::test]
async fn a_ready_refusal_catches_a_service_name_split_across_reads() {
    check_refusal(true).await;
}

async fn check_refusal(split: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let upstream = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let proxy = proxy::Proxy::new(upstream.local_addr().expect("address")).await;
        proxy.refuse(SERVICE);
        let (prefix_tx, prefix_rx) = oneshot::channel();
        let (requests_tx, mut requests) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = upstream.accept().await.expect("connection");
            let mut prefix_tx = Some(prefix_tx);
            loop {
                let mut request = Vec::new();
                loop {
                    let Ok(byte) = socket.read_u8().await else {
                        return;
                    };
                    request.push(byte);
                    // The upstream receipt proves that the proxy consumed
                    // the prefix before the client sends the suffix.
                    if request == PREFIX
                        && let Some(prefix_tx) = prefix_tx.take()
                    {
                        let _ = prefix_tx.send(());
                    }
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                requests_tx.send(request).expect("request receiver");
                socket.write_all(OK).await.expect("answer");
            }
        });
        let mut client = TcpStream::connect(("127.0.0.1", proxy.port))
            .await
            .expect("proxy connection");
        client.write_all(HEARTBEAT).await.expect("heartbeat");
        assert_eq!(response(&mut client).await, OK);
        assert_eq!(
            requests.recv().await.expect("upstream heartbeat"),
            HEARTBEAT
        );

        if split {
            client.write_all(PREFIX).await.expect("prefix");
            prefix_rx.await.expect("the proxy forwarded the prefix");
            client.write_all(SUFFIX).await.expect("suffix");
        } else {
            client
                .write_all(&[PREFIX, SUFFIX].concat())
                .await
                .expect("registration");
        }
        let answer = response(&mut client).await;
        server.abort();
        assert!(
            answer.starts_with(b"HTTP/1.1 503 "),
            "a refused ready registration reached upstream: {}",
            String::from_utf8_lossy(&answer)
        );
        assert!(requests.try_recv().is_err(), "the hub received ready");
    })
    .await
    .expect("the refusal check finishes");
}

async fn response(client: &mut TcpStream) -> Vec<u8> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        header.push(client.read_u8().await.expect("response header"));
    }
    header
}

/// A refusal set, or replaced, after part of a request went upstream
/// still catches the request.
#[tokio::test]
async fn a_longer_refusal_catches_a_request_already_in_flight() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut answers = Vec::new();
        for replacing in [false, true] {
            let (proxy, mut client, mut server) = connection().await;
            if replacing {
                proxy.refuse(b"/other");
            }
            forward(&mut client, &mut server, PREFIX).await;
            // Upstream receipt proves the prefix was read before the change.
            proxy.refuse(SERVICE);
            client.write_all(SUFFIX).await.expect("suffix");
            let mut upstream = vec![0; SUFFIX.len()];
            let answer = tokio::select! {
                answer = response(&mut client) => answer,
                read = server.read_exact(&mut upstream) => match read {
                    Ok(_) => {
                        assert_eq!(upstream, SUFFIX);
                        server.write_all(OK).await.expect("upstream answer");
                        response(&mut client).await
                    }
                    // The proxy closed upstream: the request went no further.
                    Err(_) => response(&mut client).await,
                },
            };
            answers.push(String::from_utf8(answer).expect("HTTP header"));
        }
        assert!(
            answers
                .iter()
                .all(|answer| answer.starts_with("HTTP/1.1 503 ")),
            "a newly set or replaced refusal missed the split request: {answers:?}"
        );
    })
    .await
    .expect("the refusal check finishes");
}

/// A `DELETE ` that an earlier request on the connection carried, kept
/// between reads, does not silence the next request.
#[tokio::test]
async fn silent_at_delete_does_not_match_a_completed_request_again() {
    check_delete_tail(true).await;
}

/// The same, with no refusal set.
#[tokio::test]
async fn a_short_tail_does_not_replay_a_completed_request() {
    check_delete_tail(false).await;
}

async fn check_delete_tail(longer_tail: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (proxy, mut client, mut server) = connection().await;
        if longer_tail {
            proxy.refuse(SERVICE);
        }
        let earlier = b"POST /records HTTP/1.1\r\nHost: localhost\r\nContent-Length: 7\r\n\r\nDELETE ";
        forward(&mut client, &mut server, earlier).await;
        server.write_all(OK).await.expect("first answer");
        assert_eq!(response(&mut client).await, OK);

        proxy.set(proxy::Mode::SilentAtDelete);
        let next = b"POST /heartbeat HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
        client.write_all(next).await.expect("next request");
        let mut upstream = vec![0; next.len()];
        tokio::select! {
            () = proxy.intercepted.notified() => {
                panic!("a completed request's DELETE silenced a later heartbeat; longer_tail={longer_tail}");
            }
            read = server.read_exact(&mut upstream) => {
                read.expect("upstream heartbeat");
                assert_eq!(upstream, next);
                server.write_all(OK).await.expect("heartbeat answer");
                assert_eq!(response(&mut client).await, OK);
            }
        }
    })
    .await
    .expect("the mode change check finishes");
}

async fn connection() -> (proxy::Proxy, TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let proxy = proxy::Proxy::new(listener.local_addr().expect("address")).await;
    let client = TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .expect("client");
    let (server, _) = listener.accept().await.expect("upstream");
    (proxy, client, server)
}

async fn forward(client: &mut TcpStream, server: &mut TcpStream, bytes: &[u8]) {
    client.write_all(bytes).await.expect("client writes");
    let mut received = vec![0; bytes.len()];
    server
        .read_exact(&mut received)
        .await
        .expect("upstream reads");
    assert_eq!(received, bytes);
}

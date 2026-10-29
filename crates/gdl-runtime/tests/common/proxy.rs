//! A TCP proxy in front of one of a hub's ports, for the tests that make a
//! hub fall silent, go away, or cut its connections under running
//! instances.

use std::sync::Arc;

/// How a [`Proxy`] treats its connections.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Forward,
    /// Accept and hold, answering nothing: a hub that stops answering
    /// without closing its connections.
    Silent,
    /// Close at once: a hub that is briefly gone.
    Refuse,
    /// Forward until a client sends a `DELETE`, then fall silent before
    /// passing it on: a hub that answers an announcement and then stops
    /// answering at the withdrawal that follows it.
    SilentAtDelete,
    /// Forward, but give back the state `draining` as `ready`: a pool's
    /// hub whose watch says nothing of the pool's drain to the hub that
    /// follows it through the proxy, while its other events still come.
    Undrained,
}

/// What [`Mode::Undrained`] replaces in what a hub sends, and with what:
/// as long, so that the response's framing holds, and still JSON, with
/// spaces after the string.
const DRAINING: &[u8] = b"\"draining\"";
const UNDRAINED: &[u8] = b"\"ready\"   ";

/// A TCP proxy in front of one of a hub's ports, so that a test can make
/// the hub fall silent or go away for a while under running instances.
pub struct Proxy {
    pub port: u16,
    mode: Arc<tokio::sync::watch::Sender<Mode>>,
    /// Signalled when `SilentAtDelete` held a `DELETE`.
    pub intercepted: Arc<tokio::sync::Notify>,
    tamper: Arc<Tamper>,
}

/// What a proxy changes of what passes through it, whatever its mode.
#[derive(Default)]
struct Tamper {
    /// What the next hub's bytes that hold the first are sent with in its
    /// place, once: as long, so that the response's framing holds.
    rewrite: std::sync::Mutex<Option<(Vec<u8>, Vec<u8>)>>,
    /// While set, a request that holds it is not passed on.
    hold: tokio::sync::watch::Sender<Option<Vec<u8>>>,
    /// Signalled when a request is held.
    held: tokio::sync::Notify,
    /// Signalled when the hub answers a request released after a hold.
    answered: tokio::sync::Notify,
    /// While set, a request that holds it is answered here with the hub's
    /// `unavailable`, and never passed on.
    refuse: std::sync::Mutex<Option<Vec<u8>>>,
}

impl Proxy {
    pub async fn new(target: std::net::SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("valid test fixture");
        let port = listener.local_addr().expect("valid test fixture").port();
        let (mode, modes) = tokio::sync::watch::channel(Mode::Forward);
        let mode = Arc::new(mode);
        let intercepted = Arc::new(tokio::sync::Notify::new());
        let tamper = Arc::new(Tamper::default());
        let (setter, seen, tampering) = (
            Arc::clone(&mode),
            Arc::clone(&intercepted),
            Arc::clone(&tamper),
        );
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let held = Held {
                    set: Arc::clone(&setter),
                    seen: Arc::clone(&seen),
                    tamper: Arc::clone(&tampering),
                };
                tokio::spawn(relay(client, target, modes.clone(), held));
            }
        });
        Self {
            port,
            mode,
            intercepted,
            tamper,
        }
    }

    /// Send the first bytes the hub sends next that are `from` as `to`,
    /// once, on whichever connection they come.
    pub fn rewrite_once(&self, from: &[u8], to: &[u8]) {
        assert_eq!(from.len(), to.len(), "a rewrite keeps the framing");
        *self.tamper.rewrite.lock().expect("lock") = Some((from.to_vec(), to.to_vec()));
    }

    /// Hold every request that holds `pattern` until [`Proxy::release`].
    pub fn hold(&self, pattern: &[u8]) {
        assert!(pattern.len() <= TAIL + 1, "a pattern the relay can find");
        self.tamper.hold.send_replace(Some(pattern.to_vec()));
    }

    /// Answer every request that holds `pattern` as a hub that refuses it
    /// does, with a 503 and the code `unavailable`, and close its
    /// connection: the hub never sees the request.
    pub fn refuse(&self, pattern: &[u8]) {
        assert!(pattern.len() <= TAIL + 1, "a pattern the relay can find");
        *self.tamper.refuse.lock().expect("lock") = Some(pattern.to_vec());
    }

    /// Pass the held requests on.
    pub fn release(&self) {
        self.tamper.hold.send_replace(None);
    }

    /// Resolves once a request is held.
    pub async fn holding(&self) {
        self.tamper.held.notified().await;
    }

    /// Resolves once the hub answers a request released after a hold.
    pub async fn answered_after_hold(&self) {
        self.tamper.answered.notified().await;
    }

    pub fn set(&self, mode: Mode) {
        let _ = self.mode.send(mode);
    }
}

/// What a relay needs to hold a `DELETE`: the proxy's mode, which it
/// turns silent, and the signal that it did.
struct Held {
    set: Arc<tokio::sync::watch::Sender<Mode>>,
    seen: Arc<tokio::sync::Notify>,
    tamper: Arc<Tamper>,
}

/// Relay one connection while the mode forwards; hold it while silent;
/// drop it once refusing.
async fn relay(
    mut client: tokio::net::TcpStream,
    target: std::net::SocketAddr,
    mut mode: tokio::sync::watch::Receiver<Mode>,
    held: Held,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // Settle the mode first: hold while silent, return when refusing.
    loop {
        let now = *mode.borrow_and_update();
        match now {
            Mode::Refuse => return,
            Mode::Silent => {
                if mode.changed().await.is_err() {
                    return;
                }
            }
            Mode::Forward | Mode::SilentAtDelete | Mode::Undrained => break,
        }
    }
    let Ok(mut server) = tokio::net::TcpStream::connect(target).await else {
        return;
    };
    let (mut from_client, mut to_client) = client.split();
    let (mut from_server, mut to_server) = server.split();
    let (mut up, mut down) = ([0u8; 8192], [0u8; 8192]);
    // The last bytes sent upstream, so that a pattern split across two
    // reads is still recognized, whenever the pattern was set.
    let mut tail: Vec<u8> = Vec::new();
    // What a hub sent that may be the start of a state to replace, held
    // until the bytes after it come.
    let mut held_down: Vec<u8> = Vec::new();
    let mut awaiting_held = false;
    // Cleared when the client stops sending while a released request
    // awaits the hub's answer: the relay stays, so that the hub still
    // answers it, and a client that only half-closed still gets the answer.
    let mut client_open = true;
    loop {
        let now = *mode.borrow_and_update();
        match now {
            Mode::Refuse => return,
            Mode::Silent => {
                if mode.changed().await.is_err() {
                    return;
                }
                continue;
            }
            Mode::Forward | Mode::SilentAtDelete | Mode::Undrained => {}
        }
        // Biased, the mode first: a test changes the mode before it makes
        // the hub send what the new mode treats, so bytes ready at the same
        // poll as a change are relayed under the new mode.
        tokio::select! {
            biased;
            changed = mode.changed() => if changed.is_err() {
                return;
            },
            read = from_client.read(&mut up), if client_open => match read {
                Ok(n) if n > 0 => {
                    let mut seen = tail.clone();
                    seen.extend_from_slice(&up[..n]);
                    let kept = tail.len();
                    let found = (now == Mode::SilentAtDelete)
                        .then(|| find(&seen, kept, b"DELETE "))
                        .flatten();
                    if let Some(at) = found {
                        // What came before the request goes on; the request
                        // is held here, with every other connection.
                        let before = at.saturating_sub(tail.len());
                        let _ = to_server.write_all(&up[..before]).await;
                        let _ = held.set.send(Mode::Silent);
                        held.seen.notify_one();
                        // Until the test changes the mode again, not at
                        // this change, which is the proxy's own.
                        mode.borrow_and_update();
                        let _ = mode.changed().await;
                        return;
                    }
                    let refused = held.tamper.refuse.lock().expect("lock").clone();
                    if let Some(pattern) = refused
                        && find(&seen, kept, &pattern).is_some()
                    {
                        let _ = to_client.write_all(&refusal()).await;
                        return;
                    }
                    let pattern = held.tamper.hold.borrow().clone();
                    if let Some(pattern) = pattern
                        && find(&seen, kept, &pattern).is_some()
                    {
                        awaiting_held = true;
                        held.tamper.held.notify_one();
                        let mut hold = held.tamper.hold.subscribe();
                        let _ = hold.wait_for(Option::is_none).await;
                    }
                    if to_server.write_all(&up[..n]).await.is_err() {
                        return;
                    }
                    let keep = seen.len().saturating_sub(TAIL);
                    tail = seen[keep..].to_vec();
                }
                // Retain the request after its client times out, as a
                // server that already buffered its body can. The relay
                // still reads after a release, since the read that was
                // held may have had only part of the request.
                _ if awaiting_held => client_open = false,
                _ => return,
            },
            read = from_server.read(&mut down) => match read {
                Ok(n) if n > 0 => {
                    if awaiting_held {
                        awaiting_held = false;
                        held.tamper.answered.notify_one();
                    }
                    let mut out = std::mem::take(&mut held_down);
                    out.extend_from_slice(&down[..n]);
                    if now == Mode::Undrained {
                        held_down = undrain(&mut out);
                    }
                    let rewrite = held.tamper.rewrite.lock().expect("lock").clone();
                    if let Some((from, to)) = rewrite {
                        let mut rest = std::mem::take(&mut held_down);
                        out.append(&mut rest);
                        held_down = rewrite_once(&held.tamper, &mut out, &from, &to);
                    }
                    if to_client.write_all(&out).await.is_err() {
                        return;
                    }
                }
                _ => return,
            },
        }
    }
}

/// How many bytes of what went upstream a relay keeps: more than any
/// pattern a test looks for, less one.
const TAIL: usize = 255;

/// Where `pattern` is in `seen`, whose first `kept` bytes came from earlier
/// reads. A match wholly in those is not one: it was there to be found
/// before, under the patterns and the mode of then.
fn find(seen: &[u8], kept: usize, pattern: &[u8]) -> Option<usize> {
    seen.windows(pattern.len())
        .enumerate()
        .position(|(at, window)| at + pattern.len() > kept && window == pattern)
}

/// What a hub answers a request it will not take now.
fn refusal() -> Vec<u8> {
    let body = br#"{"code":"unavailable","message":"refused by the test proxy"}"#;
    let mut answer = format!(
        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    answer.extend_from_slice(body);
    answer
}

/// Replace each [`DRAINING`] in `out` with [`UNDRAINED`], and take off the
/// end of it, to be sent with what comes next, a part that may be the
/// start of one.
fn undrain(out: &mut Vec<u8>) -> Vec<u8> {
    let mut at = 0;
    while let Some(found) = out[at..]
        .windows(DRAINING.len())
        .position(|window| window == DRAINING)
    {
        let start = at + found;
        out[start..start + DRAINING.len()].copy_from_slice(UNDRAINED);
        at = start + DRAINING.len();
    }
    let partial = (1..DRAINING.len())
        .rev()
        .find(|len| out.len() >= *len && out.ends_with(&DRAINING[..*len]))
        .unwrap_or(0);
    out.split_off(out.len() - partial)
}

/// Replace the first `from` in `out` with `to`, and end the rewrite, if
/// another connection has not already; take off the end of `out`, to be
/// sent with what comes next, a part that may be the start of `from`.
fn rewrite_once(tamper: &Tamper, out: &mut Vec<u8>, from: &[u8], to: &[u8]) -> Vec<u8> {
    let mut rewrite = tamper.rewrite.lock().expect("lock");
    if rewrite.is_none() {
        return Vec::new();
    }
    if let Some(start) = out.windows(from.len()).position(|window| window == from) {
        out[start..start + from.len()].copy_from_slice(to);
        *rewrite = None;
        return Vec::new();
    }
    let partial = (1..from.len())
        .rev()
        .find(|len| out.len() >= *len && out.ends_with(&from[..*len]))
        .unwrap_or(0);
    out.split_off(out.len() - partial)
}

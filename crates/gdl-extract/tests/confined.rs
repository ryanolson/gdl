//! A confined `cargo metadata` is held to its deadline: a dependency whose
//! host accepts the connection and never answers does not hold the
//! process, or cargo's package lock, for longer.

use std::time::Duration;

use gdl_extract::{Cargo, Workspace};

#[test]
fn a_dependency_that_never_answers_is_given_up_at_the_deadline() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a silent port");
    let port = listener.local_addr().expect("the port").port();
    // Accept and hold every connection; never answer. The streams are kept
    // so that the test can see each one closed afterwards.
    let held: std::sync::Arc<std::sync::Mutex<Vec<std::net::TcpStream>>> = Default::default();
    let accepted = std::sync::Arc::clone(&held);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            accepted.lock().expect("the held streams").push(stream);
        }
    });
    let temp = tempfile::Builder::new()
        .prefix("gdl-confined-")
        .tempdir()
        .expect("a scratch directory");
    let scratch = temp.path();
    let cargo = Cargo::confined(Duration::from_secs(3));
    let started = std::time::Instant::now();
    let Err(error) = Workspace::fetch_git(
        "silent",
        &format!("http://127.0.0.1:{port}/silent.git"),
        "0123456789abcdef0123456789abcdef01234567",
        scratch,
        &cargo,
    ) else {
        panic!("a silent host resolved");
    };
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "waited {:?}",
        started.elapsed()
    );
    assert!(
        error.to_string().contains("did not finish")
            || error.to_string().contains("cargo metadata failed"),
        "{error}"
    );
    // The git that cargo spawned died with it: every connection it held
    // is closed, rather than running on, reparented, for as long as the
    // host trickles.
    let streams = std::mem::take(&mut *held.lock().expect("the held streams"));
    assert!(!streams.is_empty(), "git never connected");
    for mut stream in streams {
        use std::io::Read;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("a read timeout");
        // The request git sent is still buffered on the socket; only end
        // of file says the other end is gone.
        let mut rest = Vec::new();
        if let Err(e) = stream.read_to_end(&mut rest) {
            panic!("a connection git held is still open: {e}");
        }
    }
}

//! What the tests that run the chat example's binaries share: where things
//! are, a build of the binaries, and processes that are reaped however a
//! test ends.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The limit, in seconds, on a nested `cargo` or `cargo gdl compose`. It
/// guards against a hang and claims nothing about speed: each run takes
/// Cargo's package cache lock, which every cargo on the host shares, so on
/// a busy host most of its time is a wait for other builds. A compose that
/// takes 3 s alone took over 30 s so.
pub const CARGO_LIMIT: &str = "300";

pub fn root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    dir.parent()
        .and_then(Path::parent)
        .expect("the crate sits two levels below the root")
        .to_path_buf()
}

/// A command that runs generated binary `path`, without the caller's
/// `RUST_LOG`: under a verbose filter a binary can fill a stderr pipe that
/// its test no longer reads, and stop until it is read.
pub fn generated(path: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut command = Command::new(path);
    command.env_remove("RUST_LOG");
    command
}

pub fn example() -> PathBuf {
    root().join("examples/chat")
}

/// Nested cargo runs get target directories of their own: the outer
/// `cargo test` may still hold the lock on its own.
pub fn target(purpose: &str) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root().join("target"))
        .join(purpose)
}

pub fn cargo() -> Command {
    Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
}

/// The chat example's binaries, built once for the test binary: the
/// directory that holds them.
pub fn built() -> &'static Path {
    &chat_build().bin
}

/// A build of the chat example by `cargo gdl build`, as a user runs it.
pub struct Built {
    /// The directory the binaries were copied to.
    pub bin: PathBuf,
    /// The generated workspace.
    pub workspace: PathBuf,
    /// What the build printed on stderr: Cargo's progress and warnings.
    pub stderr: String,
}

/// The chat example built once for the test binary, offline: the local
/// cache holds every crate. Test binaries that build it at once share its
/// target directory, where one build waits for the other.
pub fn chat_build() -> &'static Built {
    static BUILT: OnceLock<Built> = OnceLock::new();
    BUILT.get_or_init(|| {
        let target = target("examples");
        let bin = target.join("chat-bin");
        let build = Command::new(env!("CARGO_BIN_EXE_cargo-gdl"))
            .args(["gdl", "build", "--spec"])
            .arg(example().join("gdl.toml"))
            .arg("--target-dir")
            .arg(&target)
            .arg("--out")
            .arg(&bin)
            .env("CARGO_NET_OFFLINE", "true")
            .output()
            .expect("cargo gdl build");
        let stderr = String::from_utf8_lossy(&build.stderr).into_owned();
        assert!(build.status.success(), "{stderr}");
        Built {
            bin,
            workspace: target.join("gdl/chat/gdl/workspace"),
            stderr,
        }
    })
}

/// A child process killed when the test ends, however it ends.
pub struct Running(pub Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Running {
    /// Its exit status, within `within`.
    pub fn exits_within(&mut self, within: Duration) -> ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait().expect("the process's state") {
                return status;
            }
            assert!(
                started.elapsed() < within,
                "the process did not exit within {within:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Send it `signal`, such as `-TERM`.
    pub fn signal(&self, signal: &str) {
        let sent = Command::new("kill")
            .args([signal, &self.0.id().to_string()])
            .status()
            .expect("kill");
        assert!(sent.success());
    }

    /// Its standard output, line by line.
    pub fn stdout(&mut self) -> Lines {
        Lines::of(self.0.stdout.take().expect("piped stdout"))
    }

    /// Its standard error, line by line.
    pub fn stderr(&mut self) -> Lines {
        Lines::of(self.0.stderr.take().expect("piped stderr"))
    }
}

/// Lines a process writes, read on a thread of their own, so that a test
/// waits for one with a bound.
pub struct Lines(mpsc::Receiver<String>);

impl Lines {
    fn of(stream: impl Read + Send + 'static) -> Self {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self(rx)
    }

    /// The next line, within `within`.
    pub fn next(&self, within: Duration) -> String {
        self.0.recv_timeout(within).expect("a line within the wait")
    }

    /// The next line, within `within`; none once the stream has closed or
    /// the wait has passed.
    pub fn try_next(&self, within: Duration) -> Option<String> {
        self.0.recv_timeout(within).ok()
    }

    /// The first line that contains `text`, within `within`; the lines
    /// before it are dropped.
    pub fn find(&self, text: &str, within: Duration) -> String {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.0.recv_timeout(left) {
                Ok(line) if line.contains(text) => return line,
                Ok(_) => {}
                Err(_) => panic!("no line with {text:?} within {within:?}"),
            }
        }
    }

    /// Every line until the process closes the stream, within `within`.
    pub fn rest(&self, within: Duration) -> Vec<String> {
        let deadline = Instant::now() + within;
        let mut lines = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.0.recv_timeout(left) {
                Ok(line) => lines.push(line),
                Err(mpsc::RecvTimeoutError::Disconnected) => return lines,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    panic!("the stream stayed open past {within:?}: {lines:?}")
                }
            }
        }
    }
}

/// A deployment of the chat example in a scratch directory: a roster
/// directory, and each segment's settings beside it.
pub struct Chat {
    dir: tempfile::TempDir,
}

impl Chat {
    pub fn new(prefix: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir()
            .expect("scratch");
        std::fs::create_dir_all(dir.path().join("roster")).expect("roster");
        Self { dir }
    }

    /// Write settings for `name` meeting in the roster, with `extra` after
    /// them, and return their path.
    pub fn config(&self, name: &str, extra: &str) -> PathBuf {
        let path = self.dir.path().join(format!("{name}.toml"));
        let text = format!(
            "listen = \"127.0.0.1:0\"\nroster = {}\n{extra}",
            toml::Value::String(self.dir.path().join("roster").display().to_string())
        );
        std::fs::write(&path, text).expect("config");
        path
    }

    /// Start segment `segment`'s binary with `config`; its stdin and
    /// stdout piped when `piped`, else closed and dropped. Its stderr is
    /// piped.
    pub fn spawn(&self, segment: &str, config: &Path, piped: bool) -> Running {
        self.spawn_from(built(), segment, config, piped)
    }

    /// [`Chat::spawn`], with the binaries in `bin`.
    pub fn spawn_from(&self, bin: &Path, segment: &str, config: &Path, piped: bool) -> Running {
        let mut command = generated(bin.join(format!("chat-{segment}")));
        command.arg(config).stderr(Stdio::piped());
        if piped {
            command.stdin(Stdio::piped()).stdout(Stdio::piped());
        } else {
            command.stdin(Stdio::null()).stdout(Stdio::null());
        }
        Running(command.spawn().expect(segment))
    }
}

/// Write one line to a process's stdin.
pub fn ask(process: &mut Running, line: &str) {
    let stdin = process.0.stdin.as_mut().expect("piped stdin");
    writeln!(stdin, "{line}").expect("request");
}

/// The address in a "system listener at <address>" line.
pub fn listener(line: &str) -> SocketAddr {
    line.rsplit(' ')
        .next()
        .and_then(|address| address.parse().ok())
        .unwrap_or_else(|| panic!("no address in {line:?}"))
}

/// `GET path` from a system listener: the status code and the body.
pub fn get(address: SocketAddr, path: &str) -> (u16, String) {
    let mut stream =
        std::net::TcpStream::connect_timeout(&address, Duration::from_secs(2)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");
    write!(stream, "GET {path} HTTP/1.1\r\nHost: probe\r\n\r\n").expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    let code = response
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status code");
    (
        code,
        response.split("\r\n\r\n").nth(1).unwrap_or("").to_string(),
    )
}

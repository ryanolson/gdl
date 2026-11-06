//! One external command that `deploy` and `down` run, ended at its
//! deadline: `kubectl` for a Kubernetes cluster, and Slurm's commands for a
//! Slurm site.

use std::io::{Read as _, Write as _};
use std::process::{Command, Stdio};
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use super::kubernetes::Reply;
use crate::Error;

/// Run `command`, named `name` in what it says, with `input` on its stdin
/// when given, and kill it at `deadline` when given. A command that runs
/// past its deadline is a [`Reply`] that did not succeed, with `late` as
/// its stderr; an error is only a command that could not start.
pub(super) fn run(
    mut command: Command,
    name: &str,
    input: Option<&[u8]>,
    deadline: Option<Instant>,
    late: &str,
) -> Result<Reply, Error> {
    let running = |e: std::io::Error| Error::new(format!("running `{name}`: {e}"));
    let mut child = command
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(running)?;
    // Written from a thread of its own: a command that prints while it
    // reads would otherwise block on a full pipe.
    let writer = match (input, child.stdin.take()) {
        (Some(input), Some(mut stdin)) => {
            let input = input.to_vec();
            Some(std::thread::spawn(move || stdin.write_all(&input)))
        }
        _ => None,
    };
    // Each pipe is read on a thread of its own, which sends what it read
    // once the pipe closes: a process the command started can hold a pipe
    // open after the command exits, so the reads too are waited for only
    // until the deadline.
    let drain = |pipe: Option<Box<dyn std::io::Read + Send>>| {
        let (send, read) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut text = Vec::new();
            if let Some(mut pipe) = pipe {
                let _ = pipe.read_to_end(&mut text);
            }
            let _ = send.send(text);
        });
        read
    };
    let stdout = drain(child.stdout.take().map(|p| Box::new(p) as _));
    let stderr = drain(child.stderr.take().map(|p| Box::new(p) as _));
    let status = loop {
        if let Some(status) = child.try_wait().map_err(running)? {
            break status;
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(Reply::failed(late));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    // The writer is not waited for: a command that exits before it reads
    // all its input breaks the pipe, and its own status says what went
    // wrong.
    drop(writer);
    let text = |reader: std::sync::mpsc::Receiver<Vec<u8>>| {
        let read = match deadline {
            Some(deadline) => {
                reader.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None => reader.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        read.map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    };
    match (text(stdout), text(stderr)) {
        (Ok(stdout), Ok(stderr)) => Ok(Reply {
            success: status.success(),
            stdout,
            stderr,
        }),
        // The readers are left to end with the pipes.
        _ => Ok(Reply::failed(late)),
    }
}

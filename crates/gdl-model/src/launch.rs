//! The records that a launcher in a pool's job on a Slurm site and the
//! login side (`cargo gdl join`, `leave`, `down`) exchange, in the
//! launcher's directory in the pool's directory on the shared file system:
//! what each file is named and what it holds. Both sides read and write
//! them; this module holds only their names and forms, no I/O.
//!
//! Each file is written once, to a name of its own in the same directory,
//! flushed, and renamed into place, so that a reader on another client
//! that opens it by name sees all of it or nothing. A launcher handles its
//! requests strictly in order, `1`, `2`, ..., and opens the records of the
//! next by name, never through a directory's listing, which a network
//! file system's client may cache. See `docs/src/guides/slurm.md`.

use std::fmt;

/// The directory of the launcher of `segment` on node `node` of job
/// `job`, within the pool's directory.
pub fn launcher_directory(job: &str, node: &str, segment: &str) -> String {
    format!("launch.{job}/{node}/{segment}")
}

/// What the launcher writes once it is up: where its joiners listen, and
/// its own step.
pub const UP: &str = "up";

/// What the launcher writes once it has taken a `TERM`: it starts nothing
/// more.
pub const CLOSED: &str = "closed";

/// The record of request `n` that the login side writes to ask for a
/// joiner: it holds the launch's name.
pub fn run(n: u64) -> String {
    format!("{n}.run")
}

/// The record of request `n` that the login side writes to stop it: it
/// holds the launch's name.
pub fn stop(n: u64) -> String {
    format!("{n}.stop")
}

/// The record the launcher writes once it has started request `n`'s
/// joiner: its pid.
pub fn started(n: u64) -> String {
    format!("{n}.started")
}

/// The record the launcher writes once request `n` has ended: one
/// [`Done`].
pub fn done(n: u64) -> String {
    format!("{n}.done")
}

/// What `up` holds: the address the launcher's joiners listen on, and the
/// launcher's own step, `<job>.<step>`, read from Slurm's environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Up {
    /// The host its joiners listen on.
    pub address: String,
    /// Its own step, `<job>.<step>`.
    pub step: String,
}

impl Up {
    /// `up`'s text, as written.
    pub fn text(&self) -> String {
        format!("{} {}\n", self.address, self.step)
    }

    /// `up` read from `text`: `None` for anything but an address and a
    /// step `<job>.<step>`.
    pub fn read(text: &str) -> Option<Self> {
        let mut words = text.split_whitespace();
        let (address, step) = (words.next()?, words.next()?);
        let (job, id) = step.split_once('.')?;
        let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
        (words.next().is_none() && digits(job) && digits(id)).then(|| Self {
            address: address.to_string(),
            step: step.to_string(),
        })
    }
}

/// How a request ended: what `<n>.done` holds, the one terminal state of
/// each request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Done {
    /// Its joiner exited with this code.
    Exited(i32),
    /// Its joiner was ended by this signal.
    Killed(i32),
    /// It was stopped before its joiner started, or with no `run` at all:
    /// its joiner never started.
    Dropped,
    /// The launcher had closed, on a `TERM`: its joiner never started.
    Refused,
}

impl fmt::Display for Done {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exited(code) => write!(f, "exited {code}"),
            Self::Killed(signal) => write!(f, "killed {signal}"),
            Self::Dropped => f.write_str("dropped"),
            Self::Refused => f.write_str("refused closed"),
        }
    }
}

impl Done {
    /// `<n>.done`'s text, as written.
    pub fn text(&self) -> String {
        format!("{self}\n")
    }

    /// `<n>.done` read from `text`: `None` for anything else.
    pub fn read(text: &str) -> Option<Self> {
        let words: Vec<&str> = text.split_whitespace().collect();
        match words.as_slice() {
            ["exited", code] => code.parse().ok().map(Self::Exited),
            ["killed", signal] => signal.parse().ok().map(Self::Killed),
            ["dropped"] => Some(Self::Dropped),
            ["refused", "closed"] => Some(Self::Refused),
            _ => None,
        }
    }

    /// Whether its joiner ran.
    pub fn ran(&self) -> bool {
        matches!(self, Self::Exited(_) | Self::Killed(_))
    }
}

/// A launch's name, as `<n>.run` and `<n>.stop` hold it: `None` for
/// anything but one word of letters and digits.
pub fn read_launch(text: &str) -> Option<&str> {
    let mut words = text.split_whitespace();
    let launch = words.next()?;
    (words.next().is_none() && launch.bytes().all(|b| b.is_ascii_alphanumeric())).then_some(launch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_record_reads_back_what_it_wrote() {
        let up = Up {
            address: "10.0.0.7".into(),
            step: "42.9".into(),
        };
        assert_eq!(Up::read(&up.text()), Some(up));
        for not in [
            "",
            "10.0.0.7",
            "10.0.0.7 42",
            "10.0.0.7 42.x",
            "a 42.9 more",
        ] {
            assert_eq!(Up::read(not), None, "{not:?}");
        }
        for done in [
            Done::Exited(0),
            Done::Exited(3),
            Done::Killed(15),
            Done::Dropped,
            Done::Refused,
        ] {
            assert_eq!(Done::read(&done.text()), Some(done));
        }
        for not in ["", "exited", "exited x", "refused", "started 9"] {
            assert_eq!(Done::read(not), None, "{not:?}");
        }
        assert_eq!(read_launch("1a2bx3c\n"), Some("1a2bx3c"));
        for not in ["", "a b", "a.b", "../x"] {
            assert_eq!(read_launch(not), None, "{not:?}");
        }
        assert_eq!(
            launcher_directory("42", "n1", "engine"),
            "launch.42/n1/engine"
        );
        assert_eq!(
            (run(3), stop(3), started(3), done(3)),
            (
                "3.run".into(),
                "3.stop".into(),
                "3.started".into(),
                "3.done".into()
            )
        );
    }
}

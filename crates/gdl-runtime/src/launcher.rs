//! The launcher of a pool's job on a Slurm site: a mode of each segment's
//! own binary, `segment --launch ...`, that srt-slurm runs on each spare
//! node of the pool's job, one for each segment of its class. It starts
//! the segment as its own child when `cargo gdl join --in-job` asks, and
//! stops it with one `TERM` when `leave` or `down` asks, through records
//! in the pool's directory ([`gdl_model::launch`]). srt-slurm runs every
//! service in a container with no Slurm client, so a joiner cannot be a
//! step of its own: it is the launcher's child, in the launcher's step.
//! See `docs/src/guides/slurm.md`.
//!
//! Three modes, each only as the binary's first argument:
//!
//! - `--launch --root <dir> --node <node> --segment <segment> [--poll-ms
//!   <ms>] -- <command...>`: the launcher.
//! - `--launch-canary <launcher pid>`: the launcher's canary, a child that
//!   only waits for a signal, so that the launcher can tell whether Slurm
//!   signalled its children too.
//! - `--launched-by <launcher pid> <segment's arguments...>`: a joiner the
//!   launcher started. It is killed when the launcher dies, and runs as
//!   the segment otherwise. Arguments are not inherited, so only the
//!   launcher's own child carries the marker.
//!
//! The launcher needs Linux 5.4 or later (pidfd); elsewhere each mode is
//! refused in one line.

use std::ffi::{OsStr, OsString};
use std::process::ExitCode;

#[cfg(target_os = "linux")]
mod linux;

/// The launcher's mode.
const LAUNCH: &str = "--launch";
/// The canary's mode.
const CANARY: &str = "--launch-canary";
/// The marker of a joiner the launcher started.
const LAUNCHED_BY: &str = "--launched-by";

/// The launcher's modes, for a segment binary's `main`, after its start
/// check: `Some` with the exit code when the binary ran as the launcher or
/// its canary, or must not run at all; `None` when it runs as the segment,
/// a joiner the launcher started among them, whose death signal is then
/// set. A mode named anywhere but first is refused.
pub fn launched() -> Option<ExitCode> {
    let args: Vec<OsString> = std::env::args_os().collect();
    let program = args.first().map_or_else(
        || "segment".into(),
        |name| name.to_string_lossy().into_owned(),
    );
    let failed = |why: String| {
        eprintln!("{program}: {why}");
        Some(ExitCode::FAILURE)
    };
    match Mode::of(&args) {
        Mode::Segment => None,
        Mode::Misplaced(flag) => failed(format!("`{flag}` is taken only as the first argument")),
        #[cfg(not(target_os = "linux"))]
        Mode::Launch | Mode::Canary | Mode::LaunchedBy => failed(format!(
            "`{}` runs only on Linux, where a Slurm pool's job runs",
            args[1].to_string_lossy()
        )),
        #[cfg(target_os = "linux")]
        Mode::Launch => Some(linux::launch_main(&args[2..]).unwrap_or_else(|why| {
            eprintln!("{program} {LAUNCH}: {why}");
            ExitCode::FAILURE
        })),
        #[cfg(target_os = "linux")]
        Mode::Canary => Some(linux::canary_main(&args[2..]).unwrap_or_else(|why| {
            eprintln!("{program} {CANARY}: {why}");
            ExitCode::FAILURE
        })),
        #[cfg(target_os = "linux")]
        Mode::LaunchedBy => match linux::launched_by(args.get(2).map(OsString::as_os_str)) {
            Ok(()) => None,
            Err(why) => failed(why),
        },
    }
}

/// What a segment binary's arguments ask it to be.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    /// The segment, as ever.
    Segment,
    Launch,
    Canary,
    /// A joiner the launcher started.
    LaunchedBy,
    /// A mode named other than first: refused.
    Misplaced(String),
}

impl Mode {
    /// The mode of `args`, the binary's name first. Only the first
    /// argument names a mode; after `--launch`, the joiner's command may
    /// hold anything.
    fn of(args: &[OsString]) -> Self {
        let first = args.get(1).and_then(|arg| arg.to_str());
        if first == Some(LAUNCH) {
            return Self::Launch;
        }
        if let Some(misplaced) = args
            .iter()
            .skip(2)
            .filter_map(|arg| arg.to_str())
            .find(|arg| [LAUNCH, CANARY, LAUNCHED_BY].contains(arg))
        {
            return Self::Misplaced(misplaced.to_string());
        }
        match first {
            Some(CANARY) => Self::Canary,
            Some(LAUNCHED_BY) => Self::LaunchedBy,
            _ => Self::Segment,
        }
    }
}

/// The segment's own arguments: the process's, without the binary's name,
/// and without a joiner's marker, which [`launched`] has read.
pub(crate) fn segment_args() -> impl Iterator<Item = OsString> {
    let mut args = std::env::args_os().skip(1).peekable();
    if args.peek().map(OsString::as_os_str) == Some(OsStr::new(LAUNCHED_BY)) {
        args.next();
        args.next();
    }
    args
}

/// Run the launcher on `args`, the arguments after `--launch`, with
/// `canary` as its canary's command, to which its pid is added: what
/// `--launch` runs, for a test that runs the launcher from its own binary.
#[cfg(target_os = "linux")]
pub fn run_launcher(args: &[OsString], canary: Vec<OsString>) -> ExitCode {
    match linux::Options::read(args).and_then(|options| linux::launch(options, canary)) {
        Ok(code) => code,
        Err(why) => {
            eprintln!("launcher: {why}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<OsString> {
        std::iter::once("segment")
            .chain(words.iter().copied())
            .map(OsString::from)
            .collect()
    }

    /// A mode is named only first; anywhere else it is refused, but after
    /// `--launch` the joiner's command may hold any word.
    #[test]
    fn a_mode_is_named_only_first() {
        assert_eq!(Mode::of(&args(&["config.toml"])), Mode::Segment);
        assert_eq!(Mode::of(&args(&[])), Mode::Segment);
        assert_eq!(
            Mode::of(&args(&[
                "--launch",
                "--root",
                "/gdl",
                "--",
                "x",
                "--launched-by"
            ])),
            Mode::Launch
        );
        assert_eq!(Mode::of(&args(&["--launch-canary", "9"])), Mode::Canary);
        assert_eq!(
            Mode::of(&args(&["--launched-by", "9", "config.toml"])),
            Mode::LaunchedBy
        );
        for misplaced in [
            &["config.toml", "--launch"][..],
            &["config.toml", "--launched-by", "9"],
            &["--launched-by", "9", "--launch-canary"],
        ] {
            assert!(
                matches!(Mode::of(&args(misplaced)), Mode::Misplaced(_)),
                "{misplaced:?}"
            );
        }
    }
}

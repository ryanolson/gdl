//! `gdl-gen --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]`
//!
//! Generates one crate per segment into `--out`. With `--check`, writes
//! nothing and fails if the files there differ from what would be
//! generated. The contract is compiled in `--target-dir`, else
//! `$CARGO_TARGET_DIR`, else `target` beside the spec.

use std::path::PathBuf;
use std::process::ExitCode;

struct Args {
    spec: PathBuf,
    out: PathBuf,
    check: bool,
    target: Option<PathBuf>,
}

const USAGE: &str = "usage: gdl-gen --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]";

fn args() -> Result<Args, String> {
    let mut spec = None;
    let mut out = None;
    let mut check = false;
    let mut target = None;
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--spec") => spec = args.next().map(PathBuf::from),
            Some("--out") => out = args.next().map(PathBuf::from),
            Some("--target-dir") => target = args.next().map(PathBuf::from),
            Some("--check") => check = true,
            _ => return Err(format!("unexpected argument {arg:?}\n{USAGE}")),
        }
    }
    Ok(Args {
        spec: spec.ok_or(USAGE)?,
        out: out.ok_or(USAGE)?,
        check,
        target,
    })
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("gdl-gen: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let args = args()?;
    let spec = gdl_gen::Spec::load(&args.spec)?;
    let target = args
        .target
        .or_else(|| std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from))
        .unwrap_or_else(|| spec.dir.join("target"));
    let files = gdl_gen::render(&spec, &args.out, &target)?;
    if args.check {
        let drift = gdl_gen::drift(&args.out, &files)?;
        if drift.is_empty() {
            return Ok(ExitCode::SUCCESS);
        }
        for line in drift {
            eprintln!("{line}");
        }
        eprintln!("gdl-gen: the generated crates are out of date; regenerate them");
        return Ok(ExitCode::FAILURE);
    }
    for removed in gdl_gen::write(&args.out, &files)? {
        eprintln!("removed stale {}", removed.display());
    }
    Ok(ExitCode::SUCCESS)
}

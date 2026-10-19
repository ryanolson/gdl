//! `gdl-gen --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]`
//! `gdl-gen --spec <gdl.toml> --diagram dot|mermaid [--target-dir <dir>]`
//! `gdl-gen --spec <gdl.toml> --resolve [--target-dir <dir>]`
//!
//! Generates one crate per segment into `--out`. With `--check`, writes
//! nothing and fails if the files there differ from what would be
//! generated. With `--diagram`, writes nothing and prints the placed graph
//! as Graphviz DOT or a Mermaid flowchart. With `--resolve`, writes
//! nothing and prints the type each message name of the spec's graph file
//! resolves to. The contract is compiled in
//! `--target-dir`, else `$CARGO_TARGET_DIR`, else `target` beside the spec.

use std::path::PathBuf;
use std::process::ExitCode;

use gdl_gen::Format;

enum Mode {
    Generate { out: PathBuf, check: bool },
    Diagram(Format),
    Resolve,
}

struct Args {
    spec: PathBuf,
    mode: Mode,
    target: Option<PathBuf>,
}

const USAGE: &str = "usage: gdl-gen --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]\n       gdl-gen --spec <gdl.toml> --diagram dot|mermaid [--target-dir <dir>]\n       gdl-gen --spec <gdl.toml> --resolve [--target-dir <dir>]";

fn args() -> Result<Args, String> {
    let mut spec = None;
    let mut out = None;
    let mut check = false;
    let mut diagram = None;
    let mut resolve = false;
    let mut target = None;
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--spec") => spec = args.next().map(PathBuf::from),
            Some("--out") => out = args.next().map(PathBuf::from),
            Some("--target-dir") => target = args.next().map(PathBuf::from),
            Some("--check") => check = true,
            Some("--resolve") => resolve = true,
            Some("--diagram") => {
                diagram = match args.next().as_ref().and_then(|f| f.to_str()) {
                    Some("dot") => Some(Format::Dot),
                    Some("mermaid") => Some(Format::Mermaid),
                    other => {
                        return Err(format!(
                            "--diagram takes dot or mermaid, not {other:?}\n{USAGE}"
                        ));
                    }
                }
            }
            _ => return Err(format!("unexpected argument {arg:?}\n{USAGE}")),
        }
    }
    let mode = match (diagram, out, resolve) {
        (Some(format), None, false) if !check => Mode::Diagram(format),
        (None, None, true) if !check => Mode::Resolve,
        (None, Some(out), false) => Mode::Generate { out, check },
        _ => return Err(USAGE.to_string()),
    };
    Ok(Args {
        spec: spec.ok_or(USAGE)?,
        mode,
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
        .unwrap_or_else(|| spec.dir().join("target"));
    let (out, check) = match args.mode {
        Mode::Diagram(format) => {
            print!("{}", gdl_gen::diagram(&spec, &target, format)?);
            return Ok(ExitCode::SUCCESS);
        }
        Mode::Resolve => {
            print!("{}", gdl_gen::resolve(&spec)?);
            return Ok(ExitCode::SUCCESS);
        }
        Mode::Generate { out, check } => (out, check),
    };
    let files = gdl_gen::render(&spec, &out, &target)?;
    if check {
        let drift = gdl_gen::drift(&out, &files)?;
        if drift.is_empty() {
            return Ok(ExitCode::SUCCESS);
        }
        for line in drift {
            eprintln!("{line}");
        }
        eprintln!("gdl-gen: the generated crates are out of date; regenerate them");
        return Ok(ExitCode::FAILURE);
    }
    for removed in gdl_gen::write(&out, &files)? {
        eprintln!("removed stale {}", removed.display());
    }
    Ok(ExitCode::SUCCESS)
}

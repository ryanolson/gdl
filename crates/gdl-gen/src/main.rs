//! `cargo gdl`: compose gdl graphs from crates.
//!
//! ```text
//! cargo gdl extract <dir | name@version> [--catalog <dir>] [--verify] [--target-dir <dir>]
//! cargo gdl catalog <dir>
//! cargo gdl resolve --spec <gdl.toml> [--target-dir <dir>]
//! cargo gdl generate --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]
//! cargo gdl check --spec <gdl.toml> [--fleet <fleet.toml>] [--target-dir <dir>]
//! cargo gdl diagram --spec <gdl.toml> --format dot|mermaid [--target-dir <dir>]
//! cargo gdl compose --spec <gdl.toml> --out <directory> [--fleet <fleet.toml>] [--local] [--target-dir <dir>]
//! cargo gdl export --spec <gdl.toml> --out <file.tar.gz> [--target-dir <dir>]
//! ```
//!
//! `extract` reads what a crate offers (messages, exports, nodes) from its
//! source, building nothing, and prints it as a working-catalogue entry, or
//! adds it to the working catalogue in `--catalog`, as
//! `<name>/<version>.toml` with its checksum and the time it was added.
//! `catalog` prints a working catalogue's index. `--verify`
//! builds the crate as well and fails if what its markers register differs
//! from what was read. `resolve` prints the message each name of a spec's
//! graph file resolves to. `generate` writes one crate per segment, and the
//! contract crate of a graph file, into `--out`, each layout's segments in
//! a directory of their own when the spec has several; with `--check` it writes
//! nothing and fails if the files there differ. `check` checks the spec
//! as `generate` does, and with `--fleet` the fleet file against it,
//! writing nothing; it prints every problem in both files, each with its
//! file, table, and key, and fails if there is any. `diagram` prints the
//! placed graph. `export` and `diagram` take a spec of one layout.
//! `compose` writes a Docker Compose bundle; a spec with classes or several
//! layouts it deploys only by cell, with `--fleet`: one hub per cell, and
//! each cell's segments. With `--fleet` it runs the same check as `check`
//! first, and writes nothing if it fails. With `--local` its images build
//! the crates the spec names by path, and gdl and its hub from `[gdl]
//! path`, from a build context that holds them and the bundle, so the
//! bundle builds only on this machine. `export` writes the generated crates as a `.tar.gz` with
//! `build-all.sh`, to build each segment on its own. Programs are built in `--target-dir`, else `$CARGO_TARGET_DIR`,
//! else `target` beside the spec or the crate.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use gdl_gen::Format;

const USAGE: &str = "usage: cargo gdl extract <dir | name@version> [--catalog <dir>] [--verify] [--target-dir <dir>]
       cargo gdl catalog <dir>
       cargo gdl resolve --spec <gdl.toml> [--target-dir <dir>]
       cargo gdl generate --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]
       cargo gdl check --spec <gdl.toml> [--fleet <fleet.toml>] [--target-dir <dir>]
       cargo gdl diagram --spec <gdl.toml> --format dot|mermaid [--target-dir <dir>]
       cargo gdl compose --spec <gdl.toml> --out <directory> [--fleet <fleet.toml>] [--local] [--target-dir <dir>]
       cargo gdl export --spec <gdl.toml> --out <file.tar.gz> [--target-dir <dir>]";

enum Command {
    Compose {
        out: PathBuf,
        fleet: Option<PathBuf>,
        local: bool,
    },
    Check {
        fleet: Option<PathBuf>,
    },
    Extract {
        krate: gdl_gen::Crate,
        catalog: Option<PathBuf>,
        verify: bool,
    },
    Catalog(PathBuf),
    Resolve,
    Generate {
        out: PathBuf,
        check: bool,
    },
    Diagram(Format),
    Export {
        out: PathBuf,
    },
}

struct Args {
    command: Command,
    spec: Option<PathBuf>,
    target: Option<PathBuf>,
}

fn args() -> Result<Args, String> {
    let mut args = std::env::args_os().skip(1).peekable();
    // Cargo runs `cargo-gdl gdl <command> ..` for `cargo gdl <command> ..`.
    if args.peek().and_then(|a| a.to_str()) == Some("gdl") {
        args.next();
    }
    let command = args
        .next()
        .and_then(|a| a.into_string().ok())
        .ok_or_else(|| USAGE.to_string())?;
    let (mut spec, mut out, mut catalog, mut target, mut format, mut krate) =
        (None, None, None, None, None, None);
    let mut fleet = None;
    let (mut check, mut verify, mut local) = (false, false, false);
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .map(PathBuf::from)
                .ok_or_else(|| USAGE.to_string())
        };
        match arg.to_str() {
            Some("--spec") => spec = Some(value()?),
            Some("--out") => out = Some(value()?),
            Some("--catalog") => catalog = Some(value()?),
            Some("--target-dir") => target = Some(value()?),
            Some("--format") => format = Some(value()?),
            Some("--fleet") => fleet = Some(value()?),
            Some("--check") => check = true,
            Some("--verify") => verify = true,
            Some("--local") => local = true,
            Some(other) if !other.starts_with("--") && krate.is_none() => {
                krate = Some(other.to_string());
            }
            _ => return Err(format!("unexpected argument {arg:?}\n{USAGE}")),
        }
    }
    let command = match command.as_str() {
        "extract" => Command::Extract {
            krate: gdl_gen::Crate::parse(&krate.ok_or_else(|| USAGE.to_string())?)
                .map_err(|e| e.to_string())?,
            catalog,
            verify,
        },
        "catalog" => Command::Catalog(PathBuf::from(krate.ok_or_else(|| USAGE.to_string())?)),
        "resolve" => Command::Resolve,
        "generate" => Command::Generate {
            out: out.ok_or_else(|| USAGE.to_string())?,
            check,
        },
        "compose" => Command::Compose {
            out: out.ok_or_else(|| USAGE.to_string())?,
            fleet: fleet.take(),
            local: std::mem::take(&mut local),
        },
        "check" => Command::Check {
            fleet: fleet.take(),
        },
        "export" => Command::Export {
            out: out.ok_or_else(|| USAGE.to_string())?,
        },
        "diagram" => match format.as_ref().and_then(|f| f.to_str()) {
            Some("dot") => Command::Diagram(Format::Dot),
            Some("mermaid") => Command::Diagram(Format::Mermaid),
            other => {
                return Err(format!(
                    "--format takes dot or mermaid, not {other:?}\n{USAGE}"
                ));
            }
        },
        _ => return Err(USAGE.to_string()),
    };
    let needs_spec = !matches!(command, Command::Extract { .. } | Command::Catalog(_));
    // `--fleet` and `--local` are for the commands that took them above.
    if (needs_spec && spec.is_none()) || fleet.is_some() || local {
        return Err(USAGE.to_string());
    }
    Ok(Args {
        command,
        spec,
        target,
    })
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("cargo gdl: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let args = args()?;
    let target = |beside: &std::path::Path| {
        args.target
            .clone()
            .or_else(|| std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from))
            .unwrap_or_else(|| beside.join("target"))
    };
    // A problem of the spec's that names no file is the spec's own.
    let located = |error: gdl_gen::Error| match &args.spec {
        Some(file) => error.in_file(file.display()),
        None => gdl_model::Problems::from(gdl_model::Problem::from(error.to_string())),
    };
    let spec = || -> Result<gdl_gen::Spec, Box<dyn std::error::Error>> {
        let file = args.spec.as_ref().ok_or(USAGE)?;
        Ok(gdl_gen::Spec::load(file).map_err(|e| e.in_file(file.display()))?)
    };
    match &args.command {
        Command::Extract {
            krate,
            catalog,
            verify,
        } => {
            let beside = match krate {
                gdl_gen::Crate::Path(dir) => dir.clone(),
                gdl_gen::Crate::Registry { .. } | gdl_gen::Crate::Git { .. } => PathBuf::from("."),
            };
            let target = target(&beside);
            let extracted = gdl_gen::extract(krate, &target, &gdl_extract::Cargo::trusted())?;
            if *verify {
                for note in gdl_gen::verify(&extracted, &target)? {
                    eprintln!("cargo gdl: {note}");
                }
            }
            match catalog {
                Some(catalog) => {
                    let now = std::time::SystemTime::now();
                    let file = gdl_gen::add_to_catalog(catalog, &extracted, now)?;
                    println!("{}", file.display());
                }
                None => print!("{}", extracted.offer.to_toml()),
            }
        }
        Command::Catalog(dir) => print!("{}", gdl_gen::Catalog::load(dir)?),
        Command::Resolve => {
            let spec = spec()?;
            let target = target(spec.dir());
            print!("{}", gdl_gen::resolve(&spec, &target).map_err(located)?);
        }
        Command::Diagram(format) => {
            let spec = spec()?;
            let target = target(spec.dir());
            print!(
                "{}",
                gdl_gen::diagram(&spec, &target, *format).map_err(located)?
            );
        }
        Command::Check { fleet } => {
            let file = args.spec.as_ref().ok_or(USAGE)?;
            let problems = gdl_gen::check_files(file, fleet.as_deref(), &target(beside(file)));
            return Ok(report(&problems));
        }
        Command::Compose { out, fleet, local } => {
            let fleet = match fleet {
                None => None,
                Some(path) => {
                    let file = args.spec.as_ref().ok_or(USAGE)?;
                    let problems = gdl_gen::check_files(file, Some(path), &target(beside(file)));
                    if !problems.is_empty() {
                        return Ok(report(&problems));
                    }
                    // The check read it.
                    let text = std::fs::read_to_string(path)
                        .map_err(|e| format!("{}: {e}", path.display()))?;
                    Some(
                        gdl_model::Fleet::from_toml(&text)
                            .map_err(|problems| problems.in_file(path.display()).to_string())?,
                    )
                }
            };
            let spec = spec()?;
            let target = target(spec.dir());
            let sources = match local {
                true => gdl_gen::Sources::Local,
                false => gdl_gen::Sources::Fetched,
            };
            if let Some(root) = gdl_gen::compose(&spec, fleet.as_ref(), out, &target, sources)? {
                eprintln!(
                    "cargo gdl: --local: the images build from {}, so this bundle builds only on this machine",
                    root.display()
                );
            }
        }
        Command::Export { out } => {
            let spec = spec()?;
            let target = target(spec.dir());
            let bytes = gdl_gen::export(&spec, &target, gdl_gen::Offers::Read).map_err(located)?;
            std::fs::write(out, bytes).map_err(|e| format!("{}: {e}", out.display()))?;
        }
        Command::Generate { out, check } => {
            let spec = spec()?;
            let target = target(spec.dir());
            let files = gdl_gen::render(&spec, out, &target).map_err(located)?;
            if *check {
                let drift = gdl_gen::drift(out, &files)?;
                if drift.is_empty() {
                    return Ok(ExitCode::SUCCESS);
                }
                for line in drift {
                    eprintln!("{line}");
                }
                eprintln!("cargo gdl: the generated crates are out of date; regenerate them");
                return Ok(ExitCode::FAILURE);
            }
            for removed in gdl_gen::write(out, &files)? {
                eprintln!("removed stale {}", removed.display());
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// The directory of the spec `file`, which its relative paths are read
/// from: a bare file name's is the current directory.
fn beside(file: &Path) -> &Path {
    match file.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Print each problem on its own line and their count, and fail; or say
/// that there are none.
fn report(problems: &gdl_model::Problems) -> ExitCode {
    if problems.is_empty() {
        println!("cargo gdl: no problems");
        return ExitCode::SUCCESS;
    }
    for problem in &problems.0 {
        eprintln!("{problem}");
    }
    let count = problems.0.len();
    let noun = if count == 1 { "problem" } else { "problems" };
    eprintln!("cargo gdl: {count} {noun}");
    ExitCode::FAILURE
}

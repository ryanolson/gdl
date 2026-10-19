//! `cargo gdl`: compose gdl graphs from crates.
//!
//! ```text
//! cargo gdl extract <dir | name@version> [--catalog <dir>] [--verify] [--target-dir <dir>]
//! cargo gdl catalog <dir>
//! cargo gdl resolve --spec <gdl.toml> [--target-dir <dir>]
//! cargo gdl generate --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]
//! cargo gdl diagram --spec <gdl.toml> --format dot|mermaid [--target-dir <dir>]
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
//! contract crate of a graph file, into `--out`; with `--check` it writes
//! nothing and fails if the files there differ. `diagram` prints the placed
//! graph. `export` writes the generated crates as a `.tar.gz` with
//! `build-all.sh`, to build each segment on its own. Programs are built in `--target-dir`, else `$CARGO_TARGET_DIR`,
//! else `target` beside the spec or the crate.

use std::path::PathBuf;
use std::process::ExitCode;

use gdl_gen::Format;

const USAGE: &str = "usage: cargo gdl extract <dir | name@version> [--catalog <dir>] [--verify] [--target-dir <dir>]
       cargo gdl catalog <dir>
       cargo gdl resolve --spec <gdl.toml> [--target-dir <dir>]
       cargo gdl generate --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]
       cargo gdl diagram --spec <gdl.toml> --format dot|mermaid [--target-dir <dir>]
       cargo gdl export --spec <gdl.toml> --out <file.tar.gz> [--target-dir <dir>]";

enum Command {
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
    let (mut check, mut verify) = (false, false);
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
            Some("--check") => check = true,
            Some("--verify") => verify = true,
            Some(other) if !other.starts_with("--") && krate.is_none() => {
                krate = Some(other.to_string());
            }
            _ => return Err(format!("unexpected argument {arg:?}\n{USAGE}")),
        }
    }
    let command = match command.as_str() {
        "extract" => Command::Extract {
            krate: gdl_gen::Crate::parse(&krate.ok_or_else(|| USAGE.to_string())?),
            catalog,
            verify,
        },
        "catalog" => Command::Catalog(PathBuf::from(krate.ok_or_else(|| USAGE.to_string())?)),
        "resolve" => Command::Resolve,
        "generate" => Command::Generate {
            out: out.ok_or_else(|| USAGE.to_string())?,
            check,
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
    if needs_spec && spec.is_none() {
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
    let spec = || -> Result<gdl_gen::Spec, Box<dyn std::error::Error>> {
        let file = args.spec.as_ref().ok_or(USAGE)?;
        Ok(gdl_gen::Spec::load(file)?)
    };
    match &args.command {
        Command::Extract {
            krate,
            catalog,
            verify,
        } => {
            let beside = match krate {
                gdl_gen::Crate::Path(dir) => dir.clone(),
                gdl_gen::Crate::Registry { .. } => PathBuf::from("."),
            };
            let target = target(&beside);
            let extracted = gdl_gen::extract(krate, &target)?;
            if *verify {
                gdl_gen::verify(&extracted, &target)?;
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
            print!("{}", gdl_gen::resolve(&spec, &target)?);
        }
        Command::Diagram(format) => {
            let spec = spec()?;
            let target = target(spec.dir());
            print!("{}", gdl_gen::diagram(&spec, &target, *format)?);
        }
        Command::Export { out } => {
            let spec = spec()?;
            let target = target(spec.dir());
            let bytes = gdl_gen::export(&spec, &target, gdl_gen::Offers::Read)?;
            std::fs::write(out, bytes).map_err(|e| format!("{}: {e}", out.display()))?;
        }
        Command::Generate { out, check } => {
            let spec = spec()?;
            let target = target(spec.dir());
            let files = gdl_gen::render(&spec, out, &target)?;
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

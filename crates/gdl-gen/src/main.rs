//! `cargo gdl`: compose gdl graphs from crates.
//!
//! ```text
//! cargo gdl extract <dir | name@version> [--catalog <dir>] [--verify] [--probe] [--target-dir <dir>]
//! cargo gdl catalog <dir>
//! cargo gdl resolve --spec <gdl.toml> [--target-dir <dir>]
//! cargo gdl generate --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]
//! cargo gdl build --spec <gdl.toml> [--out <dir>] [--release] [--target-dir <dir>]
//! cargo gdl run --spec <gdl.toml> [--release] [--target-dir <dir>]
//! cargo gdl check --spec <gdl.toml> [--fleet <fleet.toml>] [--target-dir <dir>]
//! cargo gdl diagram --spec <gdl.toml> --format dot|mermaid [--direction lr|tb] [--target-dir <dir>]
//! cargo gdl compose --spec <gdl.toml> --out <directory> [--fleet <fleet.toml>] [--local] [--target-dir <dir>]
//! cargo gdl k8s --spec <gdl.toml> --fleet <fleet.toml> --out <directory> [--tag <tag>] [--images <images.json>] [--local] [--target-dir <dir>]
//! cargo gdl images --spec <gdl.toml> --fleet <fleet.toml> [--push] [--out <images.json>] [--local] [--target-dir <dir>]
//! cargo gdl deploy --spec <gdl.toml> --fleet <fleet.toml> [--out <directory>] [--local] [--allow-pool-restart] [--target-dir <dir>]
//! cargo gdl down --fleet <fleet.toml> [--spec <gdl.toml>] [--target-dir <dir>]
//! cargo gdl export --spec <gdl.toml> --out <file.tar.gz> [--target-dir <dir>]
//! ```
//!
//! `extract` reads what a crate offers (messages, exports, nodes) from its
//! source, building nothing, and prints it as a working-catalogue entry, or
//! adds it to the working catalogue in `--catalog`, as
//! `<name>/<version>.toml` with its checksum and the time it was added.
//! `catalog` prints a working catalogue's index. `--verify`
//! builds the crate as well and fails if what its markers register differs
//! from what was read. `--probe` builds it the same way and records, on
//! each instantiation of a generic message, whether the build finds it a
//! wire type; that reading takes precedence over the reader's. A published
//! version whose entry holds a probe is kept, and nothing is built.
//! `resolve` prints the message each name of a spec's
//! graph file resolves to. `generate` writes one crate per segment, and the
//! contract crate of a graph file, into `--out`, each layout's segments in
//! a directory of their own when the spec has several; with `--check` it writes
//! nothing and fails if the files there differ. `build` generates the same
//! crates as one workspace in `<target-dir>/gdl/<graph>/<spec stem>`,
//! never in the source tree, builds every segment, copies each binary to
//! `--out`, else to `bin/<profile>` beside that workspace, and prints each
//! copy's path. It writes only the files whose text changed, so Cargo
//! rebuilds only what a change reaches. `run` builds as `build` does, then runs every
//! segment on this machine, each with settings of its own in a temporary
//! directory: a listener on a free loopback port, one shared roster
//! directory, and its `[settings.<segment>]` table. The
//! segment that hosts the source reads JSON requests from stdin; when it
//! ends, each other segment gets one SIGTERM and drains. A SIGINT reaches
//! every segment from the terminal, and `run` sends none. Unix only.
//! `check` checks the spec
//! as `generate` does, and with `--fleet` the fleet file against it,
//! writing nothing; it prints every problem in both files, each with its
//! file, table, and key, and fails if there is any. `diagram` prints the
//! placed graph, left to right unless `--direction tb`. `export` and `diagram` take a spec of one layout.
//! `compose` writes a Docker Compose bundle; a spec with classes or several
//! layouts it deploys only by pool, with `--fleet`: one hub per pool, and
//! each pool's segments. With `--fleet` it runs the same check as `check`
//! first, and writes nothing if it fails. With `--local` its images build
//! the crates the spec names by path, and gdl and its hub from `[gdl]
//! path`, from the tree it writes in the bundle's `context/`: those
//! sources as Git holds them, and the bundle among them. `k8s` writes a fleet's Kubernetes
//! objects, one directory per cluster, after the same check; each image
//! is tagged `--tag`, else with the sha256 of what it is built from, and
//! with `--images`, the file `images` writes, also named by its digest. It
//! needs no Docker and no cluster. `images` builds the hub's image and one
//! per layout and segment that the fleet's pools run, for each cluster's
//! platform, with `docker buildx`; with `--push` it pushes each to each
//! cluster's registry, unless the registry already holds its tag, and
//! writes each image's digest to `--out`. It prints each image by
//! reference and digest. With `--local`, `k8s`, `images`, and `deploy`
//! build gdl and its hub from `[gdl] path`, and the crates the spec names
//! by path, as Git holds them on this machine (each commit, and each change
//! to a tracked file; `git add` a new file to build it), with no fetch of
//! gdl; each tag covers those sources. `deploy` applies a fleet to its clusters with
//! `kubectl` and each cluster's kube context, and stops at the first step
//! that fails: `check`, and each cluster's context; `images --push`; `k8s`,
//! by the pushed digests, also written to `--out` when given; each
//! cluster's base objects; its pools, applied server-side; a prune of the
//! objects labelled with the fleet that it did not render, after a drain
//! of each pool it removes; and a wait until each pool's health answers
//! 200. In a cluster the gdl operator runs (`[cluster.<name>] operator`),
//! its pools are `GdlPool`s, which it deletes with background propagation
//! so that the operator stops each in order, and a pool is ready when its
//! status says so; `--allow-pool-restart` lets the operator apply a
//! change that restarts a pool's hub. `down` drains every pool that has a
//! parent, deletes every object labelled with the fleet, children's pools
//! first across the fleet, and removes the label from each namespace,
//! which it keeps; with `--spec`, it first runs the same check as `check`.
//! `export` writes the generated crates as a `.tar.gz` with
//! `build-all.sh`, to build each segment on its own. Programs are built in `--target-dir`, else `$CARGO_TARGET_DIR`,
//! else `target` beside the spec or the crate.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use gdl_gen::{Direction, Format};

const USAGE: &str = "usage: cargo gdl extract <dir | name@version> [--catalog <dir>] [--verify] [--probe] [--target-dir <dir>]
       cargo gdl catalog <dir>
       cargo gdl resolve --spec <gdl.toml> [--target-dir <dir>]
       cargo gdl generate --spec <gdl.toml> --out <dir> [--check] [--target-dir <dir>]
       cargo gdl build --spec <gdl.toml> [--out <dir>] [--release] [--target-dir <dir>]
       cargo gdl run --spec <gdl.toml> [--release] [--target-dir <dir>]
       cargo gdl check --spec <gdl.toml> [--fleet <fleet.toml>] [--target-dir <dir>]
       cargo gdl diagram --spec <gdl.toml> --format dot|mermaid [--direction lr|tb] [--target-dir <dir>]
       cargo gdl compose --spec <gdl.toml> --out <directory> [--fleet <fleet.toml>] [--local] [--target-dir <dir>]
       cargo gdl k8s --spec <gdl.toml> --fleet <fleet.toml> --out <directory> [--tag <tag>] [--images <images.json>] [--local] [--target-dir <dir>]
       cargo gdl images --spec <gdl.toml> --fleet <fleet.toml> [--push] [--out <images.json>] [--local] [--target-dir <dir>]
       cargo gdl deploy --spec <gdl.toml> --fleet <fleet.toml> [--out <directory>] [--local] [--allow-pool-restart] [--target-dir <dir>]
       cargo gdl down --fleet <fleet.toml> [--spec <gdl.toml>] [--target-dir <dir>]
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
    K8s {
        out: PathBuf,
        fleet: PathBuf,
        tag: Option<String>,
        images: Option<PathBuf>,
        local: bool,
    },
    Images {
        fleet: PathBuf,
        push: bool,
        out: Option<PathBuf>,
        local: bool,
    },
    Deploy {
        fleet: PathBuf,
        out: Option<PathBuf>,
        local: bool,
        allow_pool_restart: bool,
    },
    Down {
        fleet: PathBuf,
    },
    Extract {
        krate: gdl_gen::Crate,
        catalog: Option<PathBuf>,
        verify: bool,
        probe: bool,
    },
    Catalog(PathBuf),
    Resolve,
    Generate {
        out: PathBuf,
        check: bool,
    },
    Build {
        out: Option<PathBuf>,
        release: bool,
    },
    Run {
        release: bool,
    },
    Diagram(Format, Direction),
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
    let (mut spec, mut out, mut catalog, mut target, mut format, mut direction, mut krate) =
        (None, None, None, None, None, None, None);
    let (mut fleet, mut tag, mut images) = (None, None, None);
    let (mut check, mut verify, mut probe, mut local) = (false, false, false, false);
    let mut push = false;
    let mut release = false;
    let mut allow_pool_restart = false;
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
            Some("--direction") => direction = Some(value()?),
            Some("--fleet") => fleet = Some(value()?),
            Some("--images") => images = Some(value()?),
            Some("--push") => push = true,
            Some("--tag") => {
                let given = value()?;
                tag = Some(given.into_os_string().into_string().map_err(|given| {
                    format!("--tag takes an image tag, not {given:?}\n{USAGE}")
                })?);
            }
            Some("--check") => check = true,
            Some("--verify") => verify = true,
            Some("--probe") => probe = true,
            Some("--local") => local = true,
            Some("--allow-pool-restart") => allow_pool_restart = true,
            Some("--release") => release = true,
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
            probe,
        },
        "catalog" => Command::Catalog(PathBuf::from(krate.ok_or_else(|| USAGE.to_string())?)),
        "resolve" => Command::Resolve,
        "generate" => Command::Generate {
            out: out.ok_or_else(|| USAGE.to_string())?,
            check,
        },
        "build" => Command::Build {
            out: out.take(),
            release: std::mem::take(&mut release),
        },
        "run" => Command::Run {
            release: std::mem::take(&mut release),
        },
        "compose" => Command::Compose {
            out: out.ok_or_else(|| USAGE.to_string())?,
            fleet: fleet.take(),
            local: std::mem::take(&mut local),
        },
        "check" => Command::Check {
            fleet: fleet.take(),
        },
        "k8s" => Command::K8s {
            out: out.ok_or_else(|| USAGE.to_string())?,
            fleet: fleet.take().ok_or_else(|| USAGE.to_string())?,
            tag: tag.take(),
            images: images.take(),
            local: std::mem::take(&mut local),
        },
        "images" => {
            // The digests file names images in their registries, which a
            // run without `--push` does not reach.
            if out.is_some() && !push {
                return Err(format!(
                    "images: --out writes the digests of pushed images: give --push\n{USAGE}"
                ));
            }
            Command::Images {
                fleet: fleet.take().ok_or_else(|| USAGE.to_string())?,
                push: std::mem::take(&mut push),
                out: out.take(),
                local: std::mem::take(&mut local),
            }
        }
        "deploy" => Command::Deploy {
            fleet: fleet.take().ok_or_else(|| USAGE.to_string())?,
            out: out.take(),
            local: std::mem::take(&mut local),
            allow_pool_restart: std::mem::take(&mut allow_pool_restart),
        },
        "down" => Command::Down {
            fleet: fleet.take().ok_or_else(|| USAGE.to_string())?,
        },
        "export" => Command::Export {
            out: out.ok_or_else(|| USAGE.to_string())?,
        },
        "diagram" => {
            let format = match format.take().as_ref().and_then(|f| f.to_str()) {
                Some("dot") => Format::Dot,
                Some("mermaid") => Format::Mermaid,
                other => {
                    return Err(format!(
                        "--format takes dot or mermaid, not {other:?}\n{USAGE}"
                    ));
                }
            };
            let direction = match direction.take().as_ref().and_then(|d| d.to_str()) {
                None | Some("lr") => Direction::LeftToRight,
                Some("tb") => Direction::TopToBottom,
                Some(other) => {
                    return Err(format!(
                        "--direction takes lr or tb, not {other:?}\n{USAGE}"
                    ));
                }
            };
            Command::Diagram(format, direction)
        }
        _ => return Err(USAGE.to_string()),
    };
    let needs_spec = !matches!(
        command,
        Command::Extract { .. } | Command::Catalog(_) | Command::Down { .. }
    );
    // `--fleet`, `--tag`, `--images`, `--push`, `--local`, `--release`, and
    // `--direction` are for the commands that took them above.
    if (needs_spec && spec.is_none())
        || direction.is_some()
        || fleet.is_some()
        || tag.is_some()
        || images.is_some()
        || push
        || local
        || release
    {
        return Err(USAGE.to_string());
    }
    Ok(Args {
        command,
        spec,
        target,
    })
}

/// Where the images take gdl and the crates they build: `--local`, from
/// this machine's sources.
fn sources(local: bool) -> gdl_gen::Sources {
    match local {
        true => gdl_gen::Sources::Local,
        false => gdl_gen::Sources::Fetched,
    }
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
            probe,
        } => {
            let beside = match krate {
                gdl_gen::Crate::Path(dir) => dir.clone(),
                gdl_gen::Crate::Registry { .. } | gdl_gen::Crate::Git { .. } => PathBuf::from("."),
            };
            let target = target(&beside);
            let mut extracted = gdl_gen::extract(krate, &target, &gdl_extract::Cargo::trusted())?;
            if *verify {
                for note in gdl_gen::verify(&extracted, &target)? {
                    eprintln!("cargo gdl: {note}");
                }
            }
            if *probe {
                let kept = match catalog {
                    Some(catalog) => gdl_gen::Catalog::load(catalog)?.probed(&extracted.offer),
                    None => false,
                };
                if !kept {
                    for note in gdl_gen::probe(&mut extracted, &target)? {
                        eprintln!("cargo gdl: {note}");
                    }
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
        Command::Diagram(format, direction) => {
            let spec = spec()?;
            let target = target(spec.dir());
            print!(
                "{}",
                gdl_gen::diagram(&spec, &target, *format, *direction).map_err(located)?
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
            if let Some(tree) = gdl_gen::compose(&spec, fleet.as_ref(), out, &target, sources)? {
                eprintln!(
                    "cargo gdl: --local: the images build from {}, which holds the sources as Git holds them now: compose again to build a later change",
                    tree.display()
                );
            }
        }
        Command::K8s {
            out,
            fleet,
            tag,
            images,
            local,
        } => {
            let Some(Deployed {
                spec,
                fleet,
                target,
            }) = fleet_and_spec(&args, fleet, &target)?
            else {
                return Ok(ExitCode::FAILURE);
            };
            let digests = match images {
                None => None,
                Some(path) => {
                    let text = std::fs::read_to_string(path)
                        .map_err(|e| format!("{}: {e}", path.display()))?;
                    Some(
                        gdl_gen::Digests::from_json(&text)
                            .map_err(|e| format!("{}: {e}", path.display()))?,
                    )
                }
            };
            gdl_gen::k8s(
                &spec,
                &fleet,
                out,
                &target,
                tag.as_deref(),
                digests.as_ref(),
                sources(*local),
            )?;
        }
        Command::Images {
            fleet,
            push,
            out,
            local,
        } => {
            let Some(Deployed {
                spec,
                fleet,
                target,
            }) = fleet_and_spec(&args, fleet, &target)?
            else {
                return Ok(ExitCode::FAILURE);
            };
            let built = gdl_gen::images(
                &spec,
                &fleet,
                &target,
                *push,
                sources(*local),
                &mut gdl_gen::Buildx,
            )?;
            for image in &built {
                let what = match image.outcome {
                    gdl_gen::Outcome::Pushed => "pushed",
                    gdl_gen::Outcome::Present => "already in its registry",
                    gdl_gen::Outcome::Loaded => "built into the local image store",
                };
                eprintln!("cargo gdl: {}: {what}", image.reference);
                println!("{}@{}", image.reference, image.digest);
            }
            if let Some(out) = out {
                std::fs::write(out, gdl_gen::Digests::of(&built).to_json())
                    .map_err(|e| format!("{}: {e}", out.display()))?;
            }
        }
        Command::Deploy {
            fleet,
            out,
            local,
            allow_pool_restart,
        } => {
            let file = args.spec.as_ref().ok_or(USAGE)?;
            let how = gdl_gen::Deploy {
                spec: file.clone(),
                fleet: fleet.clone(),
                target: target(beside(file)),
                out: out.clone(),
                waits: gdl_gen::Waits::default(),
                sources: sources(*local),
                allow_pool_restart: *allow_pool_restart,
            };
            let mut say = |step: &str| eprintln!("cargo gdl: {step}");
            if let Err(error) =
                gdl_gen::deploy(&how, &mut gdl_gen::Buildx, &mut gdl_gen::Kubectl, &mut say)
            {
                eprintln!("{error}");
                eprintln!("cargo gdl: deploy stopped");
                return Ok(ExitCode::FAILURE);
            }
        }
        Command::Down { fleet } => {
            if let Some(file) = &args.spec {
                let problems = gdl_gen::check_files(file, Some(fleet), &target(beside(file)));
                if !problems.is_empty() {
                    return Ok(report(&problems));
                }
            }
            let mut say = |step: &str| eprintln!("cargo gdl: {step}");
            gdl_gen::down(
                fleet,
                &gdl_gen::Waits::default(),
                &mut gdl_gen::Kubectl,
                &mut say,
            )?;
        }
        Command::Export { out } => {
            let spec = spec()?;
            let target = target(spec.dir());
            let bytes = gdl_gen::export(&spec, &target, gdl_gen::Offers::Read).map_err(located)?;
            std::fs::write(out, bytes).map_err(|e| format!("{}: {e}", out.display()))?;
        }
        Command::Build { out, release } => {
            let spec = spec()?;
            let target = target(spec.dir());
            let how = gdl_gen::Build {
                out: out.clone(),
                release: *release,
            };
            for binary in gdl_gen::build(&spec, &target, &how).map_err(located)? {
                println!("{}", binary.display());
            }
        }
        Command::Run { release } => {
            let spec = spec()?;
            let target = target(spec.dir());
            let how = gdl_gen::Build {
                out: None,
                release: *release,
            };
            // A segment that fails is no problem of the spec.
            gdl_gen::run(&spec, &target, &how)?;
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

/// A spec, the fleet that deploys it, and the target directory to build in.
struct Deployed {
    spec: gdl_gen::Spec,
    fleet: gdl_model::Fleet,
    target: PathBuf,
}

/// The spec, the fleet at `path`, and the target directory, once `check`
/// finds no problem in either file; `None`, with the problems printed,
/// when it finds some.
fn fleet_and_spec(
    args: &Args,
    path: &Path,
    target: &dyn Fn(&Path) -> PathBuf,
) -> Result<Option<Deployed>, Box<dyn std::error::Error>> {
    let file = args.spec.as_ref().ok_or(USAGE)?;
    let problems = gdl_gen::check_files(file, Some(path), &target(beside(file)));
    if !problems.is_empty() {
        report(&problems);
        return Ok(None);
    }
    // The check read it.
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let fleet = gdl_model::Fleet::from_toml(&text)
        .map_err(|problems| problems.in_file(path.display()).to_string())?;
    let spec = gdl_gen::Spec::load(file).map_err(|e| e.in_file(file.display()))?;
    let target = target(spec.dir());
    Ok(Some(Deployed {
        spec,
        fleet,
        target,
    }))
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

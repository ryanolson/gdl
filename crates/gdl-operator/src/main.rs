//! `gdl-operator`: `run` runs the operator in a namespace, and
//! `manifests` prints what a cluster's owner applies to run it there.

use std::process::ExitCode;

const USAGE: &str = "usage: gdl-operator run --namespace <namespace>\n       gdl-operator manifests --namespace <namespace> --image <image>";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gdl-operator: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("run") => {
            let namespace = flag(args, "--namespace")?;
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .init();
            let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
            runtime
                .block_on(gdl_operator::run(
                    &namespace,
                    gdl_operator::Victims::default(),
                ))
                .map_err(|e| e.to_string())
        }
        Some("manifests") => {
            let namespace = flag(args, "--namespace")?;
            let image = flag(args, "--image")?;
            let mut objects = gdl_operator::crds();
            objects.extend(gdl_operator::namespaced(&namespace, &image));
            print!("{}", gdl_operator::stream(&objects));
            Ok(())
        }
        _ => Err(USAGE.to_string()),
    }
}

/// The value after `name` in `args`.
fn flag(args: &[String], name: &str) -> Result<String, String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|at| args.get(at + 1))
        .cloned()
        .ok_or_else(|| format!("{name} is missing; {USAGE}"))
}

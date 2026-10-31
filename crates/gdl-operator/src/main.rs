//! `gdl-operator`: `manifests` prints what a cluster's owner applies to
//! run the operator in a namespace.

use std::process::ExitCode;

const USAGE: &str = "usage: gdl-operator manifests --namespace <namespace> --image <image>";

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

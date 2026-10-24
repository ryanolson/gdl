//! A fleet's Compose bundle: for each cell, one hub, named for the cell,
//! and a service for each segment of the cell's class in the cell's
//! layout, each configured as that cell. Each image is built once per
//! layout and segment, and every cell of that layout and class runs it.

use std::collections::{BTreeMap, BTreeSet};

use gdl_model::{CellSettings, DirectoryBackend, Fleet, HUB_PUBLIC_PORT, Manifest};
use serde_json::{Map, Value, json};

use super::variables::{cell_variables, string};
use super::{Bundle, check_segment_name, hub_healthcheck, io, segment_healthcheck};
use crate::{Error, Files, Spec, emit, export::image_problem, validate::Layouts};

/// Where every hub of the bundle reads and writes the cell directory: the
/// one volume they share.
const CELLS: &str = "/var/lib/gdl/cells";

/// The volume that holds the cell directory.
const VOLUME: &str = "cells";

/// Why `spec` deploys only by cell, if it does: its placement has classes,
/// or it has several layouts, each of which only a cell names.
pub(super) fn by_cell(spec: &Spec) -> Option<String> {
    let layouts: Vec<_> = spec.layouts().collect();
    if layouts.len() > 1 {
        let names: Vec<String> = layouts
            .iter()
            .map(|laid| format!("`{}`", laid.name))
            .collect();
        let names = match names.split_last() {
            Some((last, [only])) => format!("{only} and {last}"),
            Some((last, rest)) => format!("{}, and {last}", rest.join(", ")),
            None => String::new(),
        };
        return Some(format!("of several layouts, {names},"));
    }
    layouts
        .iter()
        .any(|laid| laid.placement.classes().next().is_some())
        .then(|| "with classes".into())
}

/// The bundle of `fleet`'s cells, which [`Fleet::check`] accepted against
/// `spec`'s `layouts`.
pub(super) fn bundle(
    spec: &Spec,
    fleet: &Fleet,
    mut bundle: Bundle,
    manifest: &Manifest,
    layouts: &Layouts,
) -> Result<Files, Error> {
    let cells: Vec<CellSettings> = fleet.cells().collect();
    if cells.is_empty() {
        return Err(Error::new(
            "the fleet has no cells, so there is nothing to deploy",
        ));
    }
    bundle.validate_images()?;
    let graph = bundle.graph.clone();
    let mut services = Map::new();
    // The cells that run each segment, by its name, which its deployment
    // settings, and so its ports, are keyed by in every layout.
    let mut runs: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for laid in spec.layouts() {
        let ours: Vec<&CellSettings> = cells
            .iter()
            .filter(|cell| cell.layout.as_deref().unwrap_or(laid.name) == laid.name)
            .collect();
        if ours.is_empty() {
            continue;
        }
        let layout = layouts
            .get(laid.name)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::new(laid.named("the placement does not fit the graph".into())))?;
        // A spec of one layout keeps the names of a bundle without cells.
        let dir = laid.dir();
        let deployment = dir.join("deployment.json");
        let spec_of =
            gdl_model::DeploymentSpec::new(manifest.clone(), laid.placement.clone()).map_err(io)?;
        bundle.files.insert(
            deployment.clone(),
            serde_json::to_string_pretty(&spec_of).map_err(io)?,
        );
        let hub_dockerfile = dir.join("hub/Dockerfile");
        let text = bundle.hub_dockerfile(&deployment.display().to_string());
        bundle.files.insert(hub_dockerfile.clone(), text);
        let hub = Image {
            name: match laid.among_several() {
                true => format!("{}:{}", emit::package_name(&graph, "hub"), laid.name),
                false => emit::package_name(&graph, "hub"),
            },
            dockerfile: hub_dockerfile.display().to_string(),
        };
        // Each segment that a cell of the layout runs, built once.
        let mut images = BTreeMap::new();
        for (segment, _) in laid.placement.segments() {
            check_segment_name(segment)?;
            let class = layout.class_of(segment);
            if !ours.iter().any(|cell| class == Some(cell.class.as_str())) {
                continue;
            }
            let package = laid.package(&graph, segment);
            if let Some(problem) = image_problem(segment, &package) {
                return Err(Error::new(problem));
            }
            let grace = bundle.segment_image(&dir, segment, &package, None)?;
            let image = Image {
                name: package,
                dockerfile: dir.join(segment).join("Dockerfile").display().to_string(),
            };
            images.insert(segment, (image, grace));
        }
        let mut built = BTreeSet::new();
        for cell in ours {
            let variables = variables(cell)?;
            let mut service = json!({
                "image": hub.name,
                // Compose refuses to scale a service whose container is
                // named, and a cell's record has one writer.
                "container_name": emit::package_name(&graph, &cell.name),
                "environment": variables,
                "volumes": [format!("{VOLUME}:{CELLS}")],
                "healthcheck": hub_healthcheck(),
            });
            hub.build(&mut service, &mut built);
            services.insert(cell.name.clone(), service);
            for (segment, (image, grace)) in &images {
                if layout.class_of(segment) != Some(cell.class.as_str()) {
                    continue;
                }
                runs.entry(segment).or_default().insert(&cell.name);
                let mut variables = variables.clone();
                variables.insert("GDL_HUB__HOST".into(), string(&cell.name).into());
                // A segment waits only for its own cell's hub to be live:
                // never for another cell, nor for a cell's health, which a
                // parent answers 503 until its children are ready.
                let mut service = json!({
                    "image": image.name,
                    "depends_on": {&cell.name: {"condition": "service_healthy"}},
                    "environment": variables,
                    "init": true,
                    "stop_grace_period": format!("{grace}s"),
                    "healthcheck": segment_healthcheck(),
                    "ports": bundle.ports(segment),
                });
                image.build(&mut service, &mut built);
                services.insert(format!("{}_{segment}", cell.name), service);
            }
        }
    }
    for (segment, cells) in &runs {
        if cells.len() > 1 && !bundle.ports(segment).is_empty() {
            let cells: Vec<String> = cells.iter().map(|cell| format!("`{cell}`")).collect();
            return Err(Error::new(format!(
                "segment `{segment}` publishes ports, and runs in cells {}: two containers on one host cannot bind one port; publish ports only from a segment that runs in one cell",
                cells.join(", ")
            )));
        }
    }
    let mut files = bundle.files;
    files.insert(
        "compose.yaml".into(),
        serde_json::to_string_pretty(&json!({"services": services, "volumes": {VOLUME: {}}}))
            .map_err(io)?,
    );
    files.insert("README.md".into(), README.into());
    Ok(files)
}

/// An image of the bundle: its name, and the Dockerfile that builds it.
struct Image {
    name: String,
    dockerfile: String,
}

impl Image {
    /// Give `service` the build of this image if no service in `built` has
    /// it yet: one service builds an image, and the others of its cells
    /// run it by its name, which Compose does not pull while a service of
    /// the bundle builds it.
    fn build(&self, service: &mut Value, built: &mut BTreeSet<String>) {
        if built.insert(self.name.clone()) {
            service["build"] = json!({"context": ".", "dockerfile": self.dockerfile});
        }
    }
}

/// The `GDL_CELL__*` variables of `cell`'s hub and segments: its settings
/// as its fleet gives them, publishing to the bundle's shared directory,
/// with its hub where other cells' hubs reach it, at the service named for
/// the cell.
fn variables(cell: &CellSettings) -> Result<Map<String, Value>, Error> {
    let mut cell = cell.clone();
    cell.directory = DirectoryBackend::Dir { path: CELLS.into() };
    cell.hub = Some(format!("{}:{HUB_PUBLIC_PORT}", cell.name));
    let toml::Value::Table(table) = toml::Value::try_from(&cell).map_err(io)? else {
        return Err(Error::new("a cell's settings are a table"));
    };
    Ok(cell_variables(&table))
}

/// The README of a fleet's bundle.
const README: &str = "# Docker Compose deployment of a fleet\n\n\
Run `docker compose up --build` from this directory, on one host. Each cell of the fleet has one hub, the service named for the cell, and a service for each segment of its class in its layout, named `<cell>_<segment>`. A cell's hub and segments take the cell's settings from `GDL_CELL__*` variables, and a segment finds its own cell's hub through `GDL_HUB__HOST`.\n\n\
A segment starts once its own cell's hub is healthy, and waits for nothing else. A hub's health check is its own liveness. Nothing waits for a cell's health, or for another cell: a parent cell's health answers 503 until its children are ready, so a wait on it would deadlock a fleet that starts together.\n\n\
A cell's hub must never run as two at once: a cell's record has one writer, its hub. Each hub's service names its container, so Compose refuses to scale it. Container names are global on the Docker host, so a host runs one copy of this bundle at a time. Compose's default recreate stops the old container before it starts the new one, so `docker compose up` after a change replaces a hub without two running at once.\n\n\
Every hub publishes its cell's record to the `dir` cell directory, on the volume `cells`, which every hub of the bundle shares. So the bundle is for one host: cells that run on several machines wait for the `relay` directory backend. Use a fresh directory for each deployment: `docker compose down -v` removes the volume with the containers.\n\n\
Each image is built once, from one Dockerfile per layout and segment, and every cell of that layout and class runs it. A segment's image is named for its package (`<graph>-<segment>`, or `<graph>-<layout>-<segment>` for a spec of several layouts), and a hub's is `<graph>-hub`, tagged with its layout for a spec of several.\n\n\
Each segment takes its roster from its cell's hub when it starts: the loaded instances of every segment of its class, at least `min_replicas` of each (one by default). An instance that starts later joins, so `docker compose up --scale <cell>_<segment>=N` adds instances of a segment that publishes no host port. A segment that publishes host ports runs in one cell. An instance that is stopped retires: the others stop routing to it, and it leaves once it has drained. Kubernetes and srt-slurm are not generated.\n";

#[cfg(test)]
mod tests {
    use super::*;

    /// A cell's settings become one variable each, nested keys joined by
    /// `__`, and none for a setting that is unsaid.
    #[test]
    fn a_cells_settings_become_its_variables() {
        let fleet = Fleet::from_toml(
            "[class.model_x]\nready = { engine = 2 }\n\
             [cell.model-x-01]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n",
        )
        .expect("a fleet");
        let cell = fleet.cell("model-x-01").expect("a cell");
        let variables = variables(&cell).expect("variables");
        let expected = [
            ("GDL_CELL__CLASS", "model_x"),
            ("GDL_CELL__DIR", CELLS),
            ("GDL_CELL__DIRECTORY", "dir"),
            ("GDL_CELL__HOME", "global-us"),
            ("GDL_CELL__HUB", "model-x-01:1337"),
            ("GDL_CELL__NAME", "model-x-01"),
            ("GDL_CELL__READY__ENGINE", "2"),
            ("GDL_CELL__REGION", "us-east-1"),
            ("GDL_CELL__SUMMARY_MS", "10000"),
        ];
        let found: Vec<(&str, &str)> = variables
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str().expect("a string")))
            .collect();
        assert_eq!(found, expected);
    }
}

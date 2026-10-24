//! What the hub binary runs: admission for its deployment, and, for a
//! cell's hub, the cell's extension with the directory its settings name.

use figment::Figment;
use figment::providers::Env;
use figment::value::{Dict, Tag, Value};
use gdl_cells::DirDirectory;
use gdl_model::{CellSettings, DeploymentSpec, DirectoryBackend, Origin, Problems, cell_variable};
use velo_hub::{Hub, HubBuilder, HubConfig};

use crate::{CellError, CellReadiness, CellTiming, GraphAdmission};

/// A cell's settings for its hub, from the environment: `GDL_CELL__NAME`,
/// `GDL_CELL__READY__<SEGMENT>`, and so on, as an instance reads its
/// `[cell]` table, with keys matched without regard to case and `__`
/// between nested keys. `None` when no `GDL_CELL__` variable is set: the
/// hub of a deployment without cells. A variable that names no setting,
/// or whose value does not fit its setting or `spec`'s layout, is refused,
/// and every refusal names its variable.
///
/// The hub binary takes its deployment as its only argument and has no
/// settings file, so the environment is the only source.
pub fn cell_from_env(spec: &DeploymentSpec) -> Result<Option<CellSettings>, CellError> {
    let figment = Figment::from(Env::prefixed("GDL_CELL__").split("__"));
    let given = figment
        .extract::<Dict>()
        .map_err(|e| CellError::new(format!("GDL_CELL__*: {e}")))?;
    if given.is_empty() {
        return Ok(None);
    }
    // Every key came from the variable that spells it.
    let named = |problems: Problems| {
        let named = problems.0.into_iter().map(|mut problem| {
            if let Some(key) = problem
                .path()
                .as_deref()
                .and_then(|p| p.strip_prefix("cell."))
            {
                problem.origin = Some(Origin::Variable(cell_variable(key)));
            }
            problem
        });
        CellError::new(Problems(named.collect()).to_string())
    };
    let cell = CellSettings::read(&Value::Dict(Tag::Default, given)).map_err(named)?;
    let layout = gdl_model::layout(&spec.graph, &spec.placement)
        .map_err(|e| CellError::new(e.to_string()))?;
    cell.check_hub(&layout).map_err(named)?;
    Ok(Some(cell))
}

/// The hub of `spec`: [`GraphAdmission`], and with `cell`, the cell's
/// [`CellReadiness`] publishing to the directory its settings name, with
/// the default [`CellTiming`]. Only the `dir` backend runs in a hub
/// process: `memory` is one directory inside one process, which no other
/// hub could read, and `relay` is not built yet.
pub fn hub_builder(
    config: HubConfig,
    spec: DeploymentSpec,
    cell: Option<CellSettings>,
) -> Result<HubBuilder, CellError> {
    let Some(cell) = cell else {
        return Ok(Hub::builder(config).extension(GraphAdmission::new(spec)));
    };
    let directory = match &cell.directory {
        DirectoryBackend::Dir { path } => DirDirectory::new(path.clone()),
        DirectoryBackend::Memory => {
            return Err(CellError::new(
                "[cell] directory = \"memory\" is a directory inside one process, which no other hub can read: a hub process publishes with GDL_CELL__DIRECTORY=dir and GDL_CELL__DIR=<a directory every hub on the host shares>",
            ));
        }
        DirectoryBackend::Relay => {
            return Err(CellError::new(
                "[cell] directory = \"relay\", the default, is not built yet: a hub process publishes with GDL_CELL__DIRECTORY=dir and GDL_CELL__DIR=<a directory every hub on the host shares>",
            ));
        }
    };
    let readiness = CellReadiness::new(&spec, cell, directory, CellTiming::default())?;
    Ok(Hub::builder(config)
        .extension(GraphAdmission::new(spec))
        .extension(readiness))
}

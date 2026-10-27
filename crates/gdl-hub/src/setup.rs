//! What the hub binary runs: admission for its deployment, and, for a
//! cell's hub, the cell's extension with the directory its settings name.

use std::ffi::OsString;

use figment::value::{Dict, Map, Tag, Value};
use figment::{Figment, Metadata, Profile, Provider, Source};
use gdl_cells::{CellDirectory, DirDirectory};
use gdl_model::{CellSettings, DeploymentSpec, DirectoryBackend, Origin, Problems, cell_variable};
use velo_hub::{Hub, HubBuilder, HubConfig};

use crate::{CellError, CellReadiness, CellTiming, GraphAdmission, HubDirectory, KeyedRecords};

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
    cell_from_vars(spec, std::env::vars_os())
}

/// [`cell_from_env`] from `vars`, names and values as the environment
/// gives them, in its order, rather than from the process's environment:
/// so that what a generator writes for a hub is read as the hub reads it.
pub fn cell_from_vars(
    spec: &DeploymentSpec,
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Option<CellSettings>, CellError> {
    // One provider to a variable, so that figment's provenance of a value
    // names the variable that gave it, whichever of several set it last.
    let figment = variables(vars).fold(Figment::new(), Figment::merge);
    let given = figment
        .extract::<Dict>()
        .map_err(|e| CellError::new(format!("GDL_CELL__*: {e}")))?;
    if given.is_empty() {
        return Ok(None);
    }
    // A key that is set names the variable that gave its value; one that
    // is missing, the variable that would set it.
    let named = |problems: Problems| {
        let named = problems.0.into_iter().map(|mut problem| {
            if let Some(key) = problem
                .path()
                .as_deref()
                .and_then(|p| p.strip_prefix("cell."))
            {
                let variable = figment
                    .find_metadata(key)
                    .map_or_else(|| cell_variable(key), |given| given.name.to_string());
                problem.origin = Some(Origin::Variable(variable));
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
/// keyed record service ([`KeyedRecords`], of `[cell] keyed_capacity`
/// records) and its [`CellReadiness`] publishing to the directory its settings name, at the
/// pace its settings give ([`CellTiming::from_settings`]): `relay`, reporting to its parent's hub
/// ([`HubDirectory`]), or `dir`. `memory` is one directory inside one
/// process, which no other hub could read, so a hub process refuses it.
/// Settings made in code are checked as reading them checks them
/// ([`gdl_model::check_hub_timing`]): a timing the hub cannot run at is
/// refused, naming its key. [`hub_builder_with`] takes a directory that
/// the settings cannot name.
pub fn hub_builder(
    config: HubConfig,
    spec: DeploymentSpec,
    cell: Option<CellSettings>,
) -> Result<HubBuilder, CellError> {
    let Some(cell) = cell else {
        return Ok(Hub::builder(config).extension(GraphAdmission::new(spec)));
    };
    match &cell.directory {
        DirectoryBackend::Relay { parent_hub } => {
            let directory = HubDirectory::new(parent_hub.clone())?;
            let (builder, timing) = cell_hub(config, &spec, &cell)?;
            let readiness = CellReadiness::new(&spec, cell, directory.clone(), timing)?
                .with_reports(&directory)?;
            Ok(builder.extension(readiness))
        }
        DirectoryBackend::Dir { path } => {
            let directory = DirDirectory::new(path.clone());
            let cell = CellSettings {
                directory: DirectoryBackend::DEFAULT,
                ..cell
            };
            hub_builder_with(config, spec, cell, directory)
        }
        DirectoryBackend::Memory => Err(CellError::new(
            "[cell] directory = \"memory\" is a directory inside one process, which no other hub can read: a hub process publishes with GDL_CELL__DIRECTORY=relay, the default, and GDL_CELL__PARENT_HUB=<its parent cell's hub> for a class that another sends to",
        )),
    }
}

/// The hub of `spec` with the cell `cell`: [`GraphAdmission`], the cell's
/// keyed record service ([`KeyedRecords`]), and the cell's
/// [`CellReadiness`] publishing its record to `directory`, and
/// watching it for its children, at the pace its settings give. Any
/// backend fits, from any crate: making the cell visible is
/// [`CellDirectory::publish`], and where the record goes is the backend's
/// business. A backend that needs routes of its own on the hub, as a
/// parent's hub that takes its children's records does, adds its own
/// extension to the builder this returns.
///
/// `cell` names no directory of its own: settings whose `directory`,
/// `parent_hub`, or `dir` key is set are refused, so that nothing they say
/// is left unread. The rest is checked as [`hub_builder`] checks it.
/// [`cell_from_env`] reads the settings of the `gdl-hub` binary, whose
/// default backend is `relay`, so it asks a child cell for `parent_hub`:
/// a program that gives its hub a directory makes its settings itself.
pub fn hub_builder_with<D: CellDirectory>(
    config: HubConfig,
    spec: DeploymentSpec,
    cell: CellSettings,
    directory: D,
) -> Result<HubBuilder, CellError> {
    if cell.directory != DirectoryBackend::DEFAULT {
        return Err(CellError::new(format!(
            "[cell] directory: the hub's program gives it its directory, so the settings name none, and these name {:?}",
            cell.directory
        )));
    }
    let (builder, timing) = cell_hub(config, &spec, &cell)?;
    let readiness = CellReadiness::new(&spec, cell, directory, timing)?;
    Ok(builder.extension(readiness))
}

/// The hub of `spec` with admission, and the timing `cell`'s settings
/// give, once it is checked: settings made in code skip the reader's
/// checks.
fn cell_hub(
    config: HubConfig,
    spec: &DeploymentSpec,
    cell: &CellSettings,
) -> Result<(HubBuilder, CellTiming), CellError> {
    gdl_model::check_hub_timing(&cell.timing)
        .map_err(|problem| CellError::new(problem.to_string()))?;
    let builder = Hub::builder(config)
        .extension(GraphAdmission::new(spec.clone()))
        .extension(KeyedRecords::new(cell.timing.keyed_capacity)?);
    Ok((builder, CellTiming::from_settings(&cell.timing)))
}

/// The `GDL_CELL__` variables of `vars`, one provider to each, in their
/// order, so that a later variable overrides an earlier one as within one
/// provider: the keys after the prefix, matched without regard to case,
/// with `__` between nested keys.
fn variables(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> impl Iterator<Item = Variable> {
    const PREFIX: &str = "GDL_CELL__";
    vars.into_iter().filter_map(|(name, value)| {
        let name = name.to_string_lossy().trim().to_string();
        let key = name
            .get(..PREFIX.len())
            .filter(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
            .map(|_| name[PREFIX.len()..].replace("__", ".").to_ascii_lowercase())?;
        (!key.split('.').any(str::is_empty)).then(|| Variable {
            key,
            value: value.to_string_lossy().to_string(),
            name,
        })
    })
}

/// One environment variable, as a provider named for it.
struct Variable {
    name: String,
    key: String,
    value: String,
}

impl Provider for Variable {
    fn metadata(&self) -> Metadata {
        Metadata::from(
            self.name.clone(),
            Source::Custom("environment variable".into()),
        )
    }

    fn data(&self) -> Result<Map<Profile, Dict>, figment::Error> {
        let Ok(value) = self.value.parse::<Value>();
        let dict = figment::util::nest(&self.key, value).into_dict();
        Ok(Profile::Default.collect(dict.unwrap_or_default()))
    }
}

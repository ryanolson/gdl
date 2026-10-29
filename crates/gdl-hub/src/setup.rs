//! What the hub binary runs: admission for its deployment, and, for a
//! pool's hub, the pool's extension with the directory its settings name.

use std::ffi::OsString;

use figment::value::{Dict, Map, Tag, Value};
use figment::{Figment, Metadata, Profile, Provider, Source};
use gdl_model::{DeploymentSpec, DirectoryBackend, Origin, PoolSettings, Problems, pool_variable};
use gdl_pools::{DirDirectory, PoolDirectory};
use velo_hub::{Hub, HubBuilder, HubConfig};

use crate::{GraphAdmission, HubDirectory, KeyedRecords, PoolError, PoolReadiness, PoolTiming};

/// A pool's settings for its hub, from the environment: `GDL_POOL__NAME`,
/// `GDL_POOL__READY__<SEGMENT>`, and so on, as an instance reads its
/// `[pool]` table, with keys matched without regard to case and `__`
/// between nested keys. `None` when no `GDL_POOL__` variable is set: the
/// hub of a deployment without pools. A variable that names no setting,
/// or whose value does not fit its setting or `spec`'s layout, is refused,
/// and every refusal names its variable.
///
/// The hub binary takes its deployment as its only argument and has no
/// settings file, so the environment is the only source.
pub fn pool_from_env(spec: &DeploymentSpec) -> Result<Option<PoolSettings>, PoolError> {
    pool_from_vars(spec, std::env::vars_os())
}

/// [`pool_from_env`] from `vars`, names and values as the environment
/// gives them, in its order, rather than from the process's environment:
/// so that what a generator writes for a hub is read as the hub reads it.
pub fn pool_from_vars(
    spec: &DeploymentSpec,
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Option<PoolSettings>, PoolError> {
    // One provider to a variable, so that figment's provenance of a value
    // names the variable that gave it, whichever of several set it last.
    let figment = variables(vars).fold(Figment::new(), Figment::merge);
    let given = figment
        .extract::<Dict>()
        .map_err(|e| PoolError::new(format!("GDL_POOL__*: {e}")))?;
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
                .and_then(|p| p.strip_prefix("pool."))
            {
                let variable = figment
                    .find_metadata(key)
                    .map_or_else(|| pool_variable(key), |given| given.name.to_string());
                problem.origin = Some(Origin::Variable(variable));
            }
            problem
        });
        PoolError::new(Problems(named.collect()).to_string())
    };
    let pool = PoolSettings::read(&Value::Dict(Tag::Default, given)).map_err(named)?;
    let layout = gdl_model::layout(&spec.graph, &spec.placement)
        .map_err(|e| PoolError::new(e.to_string()))?;
    pool.check_hub(&layout).map_err(named)?;
    Ok(Some(pool))
}

/// The hub of `spec`: [`GraphAdmission`], and with `pool`, the pool's
/// keyed record service ([`KeyedRecords`], of `[pool] keyed_capacity`
/// records) and its [`PoolReadiness`] publishing to the directory its settings name, at the
/// pace its settings give ([`PoolTiming::from_settings`]): `relay`, reporting to its parent's hub
/// and keeping its children's records in its state directory
/// ([`HubDirectory`]), or `dir`. `memory` is one directory inside one
/// process, which no other hub could read, so a hub process refuses it.
/// Settings made in code are checked as reading them checks them
/// ([`gdl_model::check_hub_timing`]): a timing the hub cannot run at is
/// refused, naming its key. [`hub_builder_with`] takes a directory that
/// the settings cannot name.
pub fn hub_builder(
    config: HubConfig,
    spec: DeploymentSpec,
    pool: Option<PoolSettings>,
) -> Result<HubBuilder, PoolError> {
    let Some(pool) = pool else {
        return Ok(Hub::builder(config).extension(GraphAdmission::new(spec)));
    };
    match &pool.directory {
        DirectoryBackend::Relay {
            parent_hub,
            state_dir,
        } => {
            let directory = HubDirectory::new(parent_hub.clone(), state_dir.clone())?;
            let (builder, timing) = pool_hub(config, &spec, &pool)?;
            let readiness = PoolReadiness::new(&spec, pool, directory.clone(), timing)?
                .with_reports(&directory)?;
            Ok(builder.extension(readiness))
        }
        DirectoryBackend::Dir { path } => {
            let directory = DirDirectory::new(path.clone());
            let pool = PoolSettings {
                directory: DirectoryBackend::DEFAULT,
                ..pool
            };
            hub_builder_with(config, spec, pool, directory)
        }
        DirectoryBackend::Memory => Err(PoolError::new(
            "[pool] directory = \"memory\" is a directory inside one process, which no other hub can read: a hub process publishes with GDL_POOL__DIRECTORY=relay, the default, and GDL_POOL__PARENT_HUB=<its parent pool's hub> for a class that another sends to",
        )),
    }
}

/// The hub of `spec` with the pool `pool`: [`GraphAdmission`], the pool's
/// keyed record service ([`KeyedRecords`]), and the pool's
/// [`PoolReadiness`] publishing its record to `directory`, and
/// watching it for its children, at the pace its settings give. Any
/// backend fits, from any crate: making the pool visible is
/// [`PoolDirectory::publish`], and where the record goes is the backend's
/// business. A backend that needs routes of its own on the hub, as a
/// parent's hub that takes its children's records does, adds its own
/// extension to the builder this returns.
///
/// `pool` names no directory of its own: settings whose `directory`,
/// `parent_hub`, `state_dir`, or `dir` key is set are refused, so that nothing they say
/// is left unread. The rest is checked as [`hub_builder`] checks it.
/// [`pool_from_env`] reads the settings of the `gdl-hub` binary, whose
/// default backend is `relay`, so it asks a child pool for `parent_hub`:
/// a program that gives its hub a directory makes its settings itself.
pub fn hub_builder_with<D: PoolDirectory>(
    config: HubConfig,
    spec: DeploymentSpec,
    pool: PoolSettings,
    directory: D,
) -> Result<HubBuilder, PoolError> {
    if pool.directory != DirectoryBackend::DEFAULT {
        return Err(PoolError::new(format!(
            "[pool] directory: the hub's program gives it its directory, so the settings name none, and these name {:?}",
            pool.directory
        )));
    }
    let (builder, timing) = pool_hub(config, &spec, &pool)?;
    let readiness = PoolReadiness::new(&spec, pool, directory, timing)?;
    Ok(builder.extension(readiness))
}

/// The hub of `spec` with admission, and the timing `pool`'s settings
/// give, once it is checked: settings made in code skip the reader's
/// checks.
fn pool_hub(
    config: HubConfig,
    spec: &DeploymentSpec,
    pool: &PoolSettings,
) -> Result<(HubBuilder, PoolTiming), PoolError> {
    gdl_model::check_hub_timing(&pool.timing)
        .map_err(|problem| PoolError::new(problem.to_string()))?;
    let builder = Hub::builder(config)
        .extension(GraphAdmission::new(spec.clone()))
        .extension(KeyedRecords::new(pool.timing.keyed_capacity)?);
    Ok((builder, PoolTiming::from_settings(&pool.timing)))
}

/// The `GDL_POOL__` variables of `vars`, one provider to each, in their
/// order, so that a later variable overrides an earlier one as within one
/// provider: the keys after the prefix, matched without regard to case,
/// with `__` between nested keys.
fn variables(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> impl Iterator<Item = Variable> {
    const PREFIX: &str = "GDL_POOL__";
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

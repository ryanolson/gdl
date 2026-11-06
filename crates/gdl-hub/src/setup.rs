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
/// as is a variable that sets the table in a form the hub does not read,
/// `GDL_POOL` alone or `GDL_POOL.<KEY>`, which a segment refuses too
/// ([`gdl_model::whole_table_variable`]); every refusal names its
/// variable.
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
    let figment = variables(POOL_PREFIX, vars)?
        .into_iter()
        .fold(Figment::new(), Figment::merge);
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

/// The hub binary's server settings, from the environment: its ports,
/// `GDL_HUB__PUBLIC_PORT` and `GDL_HUB__CONTROL_PORT` (1337 and 8337 by
/// default, 0 lets the OS choose), and its instance lease,
/// `GDL_HUB__INSTANCE_LEASE_MS` and `GDL_HUB__PRUNE_MS`
/// ([`gdl_model::InstanceLease`], 30 s and 10 s by default). A segment
/// reads the same variables as its `[hub]` table.
pub fn hub_config_from_env() -> Result<HubConfig, PoolError> {
    hub_config_from_vars(std::env::vars_os())
}

/// [`hub_config_from_env`] from `vars`, rather than from the process's
/// environment. The variables are read as a segment reads its `[hub]`
/// table from them ([`variables`]), so the hub and its segments take one
/// variable as one value: a name in any case and with surrounding space,
/// the last of two spellings winning, and a value as figment parses it.
/// The segment's own keys, such as `host` and `heartbeat_ms`, are left to
/// it. A value that does not fit its key, a lease or prune period of 0,
/// a lease not longer than the prune period, and a variable that sets the
/// table in a form the hub does not read, `GDL_HUB` alone or
/// `GDL_HUB.<KEY>`, which a segment refuses too
/// ([`gdl_model::whole_table_variable`]), are refused, naming the
/// variable.
pub fn hub_config_from_vars(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<HubConfig, PoolError> {
    /// The keys of `[hub]` that the hub reads.
    #[derive(serde::Deserialize)]
    struct Server {
        public_port: Option<u16>,
        control_port: Option<u16>,
        instance_lease_ms: Option<u64>,
        prune_ms: Option<u64>,
    }
    // One provider to a variable, so that an error names the variable
    // that gave the value.
    let figment = variables(HUB_PREFIX, vars)?
        .into_iter()
        .fold(Figment::new(), Figment::merge);
    let server: Server = figment
        .extract()
        .map_err(|e| PoolError::new(format!("{HUB_PREFIX}*: {e}")))?;
    let millis =
        |value: Option<u64>, default| value.map_or(default, std::time::Duration::from_millis);
    let lease = gdl_model::InstanceLease::new(
        millis(
            server.instance_lease_ms,
            gdl_model::InstanceLease::DEFAULT_LEASE,
        ),
        millis(server.prune_ms, gdl_model::InstanceLease::DEFAULT_PRUNE),
    )
    .map_err(|(key, why)| {
        let name = figment.find_metadata(key).map_or_else(
            || format!("{HUB_PREFIX}{}", key.to_ascii_uppercase()),
            |given| given.name.to_string(),
        );
        PoolError::new(format!("{name}: {why}"))
    })?;
    let mut config = HubConfig::default();
    config.server.public_port = server.public_port.unwrap_or(gdl_model::HUB_PUBLIC_PORT);
    config.server.control_port = server.control_port.unwrap_or(gdl_model::HUB_CONTROL_PORT);
    config.server.ttl = lease.lease();
    config.server.prune_interval = lease.prune();
    Ok(config)
}

/// The hub of `spec`: [`GraphAdmission`], and with `pool`, the pool's
/// keyed record service ([`KeyedRecords`], of `[pool] keyed_capacity`
/// records) and its [`PoolReadiness`] publishing to the directory its settings name, at the
/// pace its settings give ([`PoolTiming::from_settings`]): `relay`, reporting to its parent's hub
/// and keeping its children's records in its state directory
/// ([`HubDirectory`]), `dir`, or, with the `dynamodb` feature, `dynamodb`,
/// whose client it builds from the settings: with an `endpoint`, a
/// DynamoDB Local's, it signs with fixed test credentials, and without
/// one it takes its credentials from the process's environment, such as a
/// Kubernetes service account's role. With the `etcd` feature, `etcd`, at
/// the settings' `endpoints`, under their `prefix`. `memory` is one directory inside one
/// process, which no other hub could read, so a hub process refuses it.
/// Settings made in code are checked as reading them checks them
/// ([`gdl_model::check_hub_timing`], and under `dir` and `dynamodb`
/// [`gdl_model::check_polled_lease`]): a timing the hub cannot run at, or
/// one whose lease lapses between its readers' polls, is refused, naming
/// its key. [`hub_builder_with`] takes a directory that
/// the settings cannot name.
pub async fn hub_builder(
    config: HubConfig,
    spec: DeploymentSpec,
    pool: Option<PoolSettings>,
) -> Result<HubBuilder, PoolError> {
    let Some(pool) = pool else {
        return Ok(Hub::builder(config).extension(GraphAdmission::new(spec)));
    };
    gdl_model::check_polled_lease(&pool.directory, &pool.timing).map_err(|why| {
        PoolError::new(
            gdl_model::Problem::at(Some("pool".into()), Some("lease_ms".into()), why).to_string(),
        )
    })?;
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
        #[cfg(feature = "dynamodb")]
        DirectoryBackend::DynamoDb {
            records_table,
            lists_table,
            aws_region,
            endpoint,
        } => {
            let directory = crate::dynamodb::directory(
                records_table,
                lists_table,
                aws_region,
                endpoint.as_deref(),
            )
            .await;
            let pool = PoolSettings {
                directory: DirectoryBackend::DEFAULT,
                ..pool
            };
            hub_builder_with(config, spec, pool, directory)
        }
        #[cfg(not(feature = "dynamodb"))]
        DirectoryBackend::DynamoDb { .. } => Err(PoolError::new(
            "[pool] directory = \"dynamodb\": this gdl-hub is built without its `dynamodb` feature, which links the AWS SDK; install it with `cargo install gdl-hub --features dynamodb`",
        )),
        #[cfg(feature = "etcd")]
        DirectoryBackend::Etcd { endpoints, prefix } => {
            // Reads nothing yet: the hub's first write makes the store's
            // identity, so a hub starts while etcd is still coming up.
            let directory =
                gdl_pools_etcd::EtcdDirectory::connect(endpoints.clone(), prefix.clone())
                    .await
                    .map_err(|error| PoolError::new(format!("[pool] endpoints: {error}")))?;
            let pool = PoolSettings {
                directory: DirectoryBackend::DEFAULT,
                ..pool
            };
            hub_builder_with(config, spec, pool, directory)
        }
        #[cfg(not(feature = "etcd"))]
        DirectoryBackend::Etcd { .. } => Err(PoolError::new(
            "[pool] directory = \"etcd\": this gdl-hub is built without its `etcd` feature, which links the etcd client; install it with `cargo install gdl-hub --features etcd`",
        )),
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
/// `pool` names no directory of its own: settings whose `directory` key,
/// or any key of a backend, is set are refused, so that nothing they say
/// is left unread. The rest is checked as [`hub_builder`] checks it, but
/// for the lease against the directory's poll period, which only the
/// program that made `directory` knows: it gives a lease longer than the
/// heartbeat and that period together.
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

/// The `dynamodb` backend that the tables step (`gdl-hub --tables`) reads
/// from `vars`: `GDL_POOL__RECORDS_TABLE`, `GDL_POOL__LISTS_TABLE`,
/// `GDL_POOL__AWS_REGION`, and, for DynamoDB Local, `GDL_POOL__ENDPOINT`,
/// read and checked as a pool's hub reads and checks them. Any other `GDL_POOL__` variable is
/// ignored, so the step can share a pool's variables. It needs no AWS
/// SDK, so that a generator's test can read what it writes for the step.
///
/// # Errors
///
/// A variable that is missing or does not fit, naming it.
pub fn directory_from_vars(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<DirectoryBackend, PoolError> {
    // Each value as the hub reads it: through figment, which decodes a
    // value that a generator quoted so that it reads as a string, such as
    // a table named `"123"`.
    let mut given = std::collections::BTreeMap::new();
    for variable in variables(POOL_PREFIX, vars)? {
        given.insert(variable.key.clone(), variable);
    }
    let mut take = |key: &str| -> Result<Option<String>, PoolError> {
        let Some(variable) = given.remove(key) else {
            return Ok(None);
        };
        let Ok(value) = variable.value.parse::<Value>();
        value
            .deserialize::<String>()
            .map(Some)
            .map_err(|error| PoolError::new(format!("{}: {error}", variable.name)))
    };
    let required = |key: &str, value: Option<String>| {
        value.ok_or_else(|| PoolError::new(format!("{}: missing", pool_variable(key))))
    };
    let records = required("records_table", take("records_table")?)?;
    let lists = required("lists_table", take("lists_table")?)?;
    let region = required("aws_region", take("aws_region")?)?;
    let endpoint = take("endpoint")?;
    DirectoryBackend::dynamodb(records, lists, region, endpoint).map_err(by_variable)
}

/// `problems` with a `[pool]` key, as the `GDL_POOL__` variable that set
/// it, for a step that reads its settings from variables alone.
pub(crate) fn by_variable(problems: Problems) -> PoolError {
    let named = problems.0.into_iter().map(|mut problem| {
        if let Some(key) = problem
            .path()
            .as_deref()
            .and_then(|path| path.strip_prefix("pool."))
        {
            problem.origin = Some(Origin::Variable(pool_variable(key)));
        }
        problem
    });
    PoolError::new(Problems(named.collect()).to_string())
}

/// The prefix of a pool's variables, which its hub and its segments read.
pub(crate) const POOL_PREFIX: &str = "GDL_POOL__";

/// The prefix of the hub's server variables, which a segment reads as its
/// `[hub]` table.
const HUB_PREFIX: &str = "GDL_HUB__";

/// The variables of `vars` under `prefix`, one provider to each, in their
/// order, so that a later variable overrides an earlier one as within one
/// provider: the keys after the prefix, matched without regard to case,
/// with `__` between nested keys. A segment reads its variables the same
/// way (`gdl-runtime`'s `process/origins.rs`), so a variable that both
/// read gives both one value.
///
/// # Errors
///
/// A variable that sets the prefix's table in a form only a segment would
/// read, `GDL_POOL` alone or `GDL_POOL.<KEY>`, which a segment refuses too
/// ([`gdl_model::whole_table_variable`]): skipped here, it would leave the
/// hub's value different from its segments'.
pub(crate) fn variables(
    prefix: &'static str,
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Vec<Variable>, PoolError> {
    let table = prefix.trim_end_matches('_');
    let mut read = Vec::new();
    for (name, value) in vars {
        let name = name.to_string_lossy().trim().to_string();
        let ours = name
            .get(..table.len())
            .is_some_and(|given| given.eq_ignore_ascii_case(table));
        if ours && let Some(refused) = gdl_model::whole_table_variable(&name) {
            return Err(PoolError::new(refused));
        }
        let Some(key) = name
            .get(..prefix.len())
            .filter(|given| given.eq_ignore_ascii_case(prefix))
            .map(|_| name[prefix.len()..].replace("__", ".").to_ascii_lowercase())
        else {
            continue;
        };
        if !key.split('.').any(str::is_empty) {
            read.push(Variable {
                key,
                value: value.to_string_lossy().to_string(),
                name,
            });
        }
    }
    Ok(read)
}

/// One environment variable, as a provider named for it.
pub(crate) struct Variable {
    pub(crate) name: String,
    pub(crate) key: String,
    pub(crate) value: String,
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

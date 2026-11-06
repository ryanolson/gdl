//! A pool's DynamoGraphDeployment (DGD): dynamo's deployment unit, which
//! runs the segments placed in its components ([`Segment::component`])
//! beside dynamo's own workers. The pool owns it, so that its stop
//! orders it and garbage collection removes it, and dynamo's operator
//! runs its pods. See `docs/src/guides/operator.md`.

use serde_json::{Map, Value};

use crate::pool::Pool;

/// The API version of the DGDs a pool renders: the one whose components
/// are a list, which dynamo's operator stores.
pub const DGD_API_VERSION: &str = "nvidia.com/v1beta1";

/// The kind of a DGD.
pub const DGD_KIND: &str = "DynamoGraphDeployment";

/// The label dynamo's operator puts on each pod of a DGD: its name.
pub const DGD_LABEL: &str = "nvidia.com/dynamo-graph-deployment-name";

/// The label dynamo's operator puts on each pod of a DGD component: its
/// name.
pub const COMPONENT_LABEL: &str = "nvidia.com/dynamo-component";

/// The annotation of a pod template that holds the checksum of what its
/// pods read from the pool's ConfigMap.
const CONFIG: &str = "gdl.dev/config";

/// The pod templates of DGD component `component`, as dynamo takes them:
/// each role's when it is an LPX component or a role has one, since
/// dynamo then refuses a shared template beside them; else the shared one.
fn templates(component: &mut Value) -> Vec<&mut Value> {
    if by_role(component) {
        let roles = component["roles"].as_array_mut().into_iter().flatten();
        return roles
            .filter_map(|role| role.as_object_mut())
            .map(|role| role.entry("podTemplate").or_insert(Value::Null))
            .collect();
    }
    match component.as_object_mut() {
        Some(fields) => vec![fields.entry("podTemplate").or_insert(Value::Null)],
        None => Vec::new(),
    }
}

/// Whether dynamo takes DGD component `component`'s pods from its roles'
/// templates ([`templates`]).
fn by_role(component: &Value) -> bool {
    let roled = component["type"] == "lpx"
        || component["roles"]
            .as_array()
            .is_some_and(|roles| roles.iter().any(|role| !role["podTemplate"].is_null()));
    roled && component["roles"].is_array()
}

/// Set the image of DGD component `component`'s container `main` to
/// `image`: the image of the segment it runs, which `cargo gdl` builds.
/// Refused unless the component's pod templates, as dynamo takes them
/// ([`templates`]), hold exactly one container of that name, since
/// otherwise no one place holds the segment's image. Nothing else of
/// the DGD changes.
pub fn set_image(dgd: &mut Value, component: &str, image: &str) -> Result<(), String> {
    let found = dgd
        .get_mut("spec")
        .and_then(|spec| spec.get_mut("components"))
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
        .find(|c| c["name"] == component)
        .ok_or_else(|| format!("the pool's DGD has no component `{component}`"))?;
    // Read through `get_mut` alone: `templates` makes the templates it
    // does not find, and an image is set in none of them.
    let shared = !by_role(found);
    let templates: Vec<&mut Value> = match shared {
        true => found.get_mut("podTemplate").into_iter().collect(),
        false => found
            .get_mut("roles")
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
            .filter_map(|role| role.get_mut("podTemplate"))
            .collect(),
    };
    let mut mains: Vec<&mut Value> = templates
        .into_iter()
        .filter_map(|template| template.pointer_mut("/spec/containers"))
        .filter_map(Value::as_array_mut)
        .flatten()
        .filter(|container| container["name"] == "main")
        .collect();
    match mains.as_mut_slice() {
        [main] => {
            if let Some(fields) = main.as_object_mut() {
                fields.insert("image".into(), image.into());
            }
            Ok(())
        }
        others => Err(format!(
            "component `{component}` of the pool's DGD has {} containers named `main` in its pod templates ({}): the segment's image is set in exactly one",
            others.len(),
            match shared {
                true => "`podTemplate.spec.containers`",
                false => "each role's `podTemplate.spec.containers`",
            }
        )),
    }
}

/// Refused unless `dgd`, as JSON, is a DGD that pool `name` in namespace
/// `namespace` can own with each `(segment, component)` of `placed`:
/// what [`crate::children`] refuses of it, and a named component without
/// `scalingAdapter`, or whose name dynamo would hash into its adapter's,
/// or without exactly one container `main` ([`set_image`]), or of a type
/// in [`VERSIONED`] without `runtimeVersionOverride`, or with one that
/// dynamo's schema refuses ([`runtime_version`]): dynamo's webhook reads
/// such a component's image tag as its version, and refuses one that is
/// not a semantic version, as `cargo gdl`'s tags never are, unless the
/// component names it. `cargo gdl
/// check` asks it of a fleet's DGD file, so that the file is refused
/// before anything is built.
pub fn check_dgd(
    name: &str,
    namespace: &str,
    dgd: &Value,
    placed: &[(&str, &str)],
) -> Result<(), String> {
    let mut dgd = dgd.clone();
    fit(name, namespace, &mut dgd, placed, "")?;
    for (_, component) in placed {
        adapter_name(&dgd, name, component)?;
        set_image(&mut dgd, component, "")?;
        let found = dgd["spec"]["components"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|c| c["name"] == *component);
        let version = found
            .map(|c| &c["runtimeVersionOverride"])
            .filter(|v| !v.is_null());
        let typed = found
            .and_then(|c| c["type"].as_str())
            .filter(|t| VERSIONED.contains(t));
        match (version, typed) {
            (None, Some(kind)) => {
                return Err(format!(
                    "component `{component}` of the pool's DGD is of type `{kind}` and runs a segment, whose image tag is not a semantic version: give it `runtimeVersionOverride`, the dynamo version the segment links, since dynamo's webhook refuses a typed component without it"
                ));
            }
            (None, None) => continue,
            (Some(_), _) => {}
        }
        if let Some(version) = version
            && !version.as_str().is_some_and(runtime_version)
        {
            return Err(format!(
                "component `{component}` of the pool's DGD has `runtimeVersionOverride` {version}, and dynamo takes only a string MAJOR.MINOR.PATCH, each part 0 or 1 to 4 digits without a leading 0, such as \"1.7.0\""
            ));
        }
    }
    Ok(())
}

/// The component types whose missing `runtimeVersionOverride` dynamo's
/// webhook refuses when the image tag is not a version
/// (`toleratesMissingRuntimeVersionOverride` in its
/// `internal/webhook/validation/shared_helpers.go`): `epp` always, and the
/// others unless the webhook's `allowMissingRuntimeVersionOverride`
/// setting, off for a DGD, says otherwise. Any other type, or none, may
/// omit it.
const VERSIONED: [&str; 6] = ["epp", "frontend", "worker", "prefill", "decode", "planner"];

/// Whether `version` is a `runtimeVersionOverride` that dynamo's DGD
/// schema takes: `^(0|[1-9][0-9]{0,3})\.(0|[1-9][0-9]{0,3})\.(0|[1-9][0-9]{0,3})$`
/// (`deploy/operator/api/v1beta1/dynamocomponentdeployment_types.go`).
fn runtime_version(version: &str) -> bool {
    let part = |part: &str| {
        part == "0"
            || (!part.starts_with('0')
                && (1..=4).contains(&part.len())
                && part.bytes().all(|b| b.is_ascii_digit()))
    };
    let parts: Vec<&str> = version.split('.').collect();
    parts.len() == 3 && parts.iter().all(|p| part(p))
}

/// Put `checksum` in `template`'s [`CONFIG`] annotation, making the
/// template, its metadata and its annotations where they are absent.
/// Refused where one of them is not an object, as dynamo would refuse
/// the DGD: indexing a field of another JSON value would panic.
fn annotate(template: &mut Value, component: &str, checksum: &str) -> Result<(), String> {
    let template = object(template, "pod template", component)?;
    let metadata = template.entry("metadata").or_insert(Value::Null);
    let metadata = object(metadata, "pod template's metadata", component)?;
    let annotations = metadata.entry("annotations").or_insert(Value::Null);
    object(annotations, "pod template's annotations", component)?
        .insert(CONFIG.into(), checksum.into());
    Ok(())
}

/// Give each container `main` of `template` `GLIBC_TUNABLES` when its
/// `env` has none, as every launcher of a segment does
/// ([`crate::render::default_tunables`]). Here, and not where `cargo gdl`
/// sets the image, since a `GdlPool` can name a DGD that `cargo gdl` never
/// read. Refused where a `main` container's `env` is not a list.
fn tune(template: &mut Value, component: &str) -> Result<(), String> {
    let containers = template
        .pointer_mut("/spec/containers")
        .and_then(Value::as_array_mut);
    for main in containers.into_iter().flatten() {
        if main["name"] != "main" {
            continue;
        }
        let Some(fields) = main.as_object_mut() else {
            continue;
        };
        let env = fields.entry("env").or_insert(Value::Null);
        if env.is_null() {
            *env = Value::Array(Vec::new());
        }
        let env = env.as_array_mut().ok_or_else(|| {
            format!("component `{component}` of the pool's DGD has a container `main` whose `env` is not a list")
        })?;
        crate::render::default_tunables(env);
    }
    Ok(())
}

/// `value` as an object, made one where it is absent; refused where it is
/// another JSON value, as `what` of component `component`.
fn object<'v>(
    value: &'v mut Value,
    what: &str,
    component: &str,
) -> Result<&'v mut Map<String, Value>, String> {
    if value.is_null() {
        *value = Value::Object(Map::new());
    }
    value.as_object_mut().ok_or_else(|| {
        format!("component `{component}` of the pool's DGD has a {what} that is not an object")
    })
}

/// The components of `dgd`, as JSON, that run a segment: those whose pod
/// templates carry the pool's [`CONFIG`] checksum, as the render gives
/// them. A DGD as applied says it, whatever the pool's spec says now.
pub fn segment_components(dgd: &Value) -> Vec<String> {
    let mut found = Vec::new();
    for component in dgd["spec"]["components"].as_array().into_iter().flatten() {
        let mut component = component.clone();
        let marked = templates(&mut component)
            .iter()
            .any(|template| !template["metadata"]["annotations"][CONFIG].is_null());
        if marked && let Some(name) = component["name"].as_str() {
            found.push(name.to_string());
        }
    }
    found
}

/// The API version of dynamo's DGD scaling adapter.
pub const ADAPTER_API_VERSION: &str = "nvidia.com/v1beta1";

/// The kind of dynamo's DGD scaling adapter.
pub const ADAPTER_KIND: &str = "DynamoGraphDeploymentScalingAdapter";

/// The name of the scaling adapter that dynamo's operator makes for
/// component `component` of the rendered DGD `dgd`, named `name`, which a
/// segment's `GdlScaler` writes. dynamo names it `<dgd>-<component in lower
/// case>` when that is a DNS subdomain, and hashes it otherwise
/// (`generateAdapterName` in its operator), so a component whose name
/// gives no plain one is refused, as is one without `scalingAdapter`:
/// without an adapter, dynamo's webhook lets no one but its operator
/// write the count.
pub(crate) fn adapter_name(dgd: &Value, name: &str, component: &str) -> Result<String, String> {
    let found = dgd["spec"]["components"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|c| c["name"] == component)
        .ok_or_else(|| format!("the pool's DGD has no component `{component}`"))?;
    if found["scalingAdapter"].is_null() {
        return Err(format!(
            "component `{component}` of the pool's DGD runs a segment, and the operator scales it through its scaling adapter: give it `scalingAdapter: {{}}`"
        ));
    }
    let adapter = format!("{name}-{}", component.to_lowercase());
    if !dns_subdomain(&adapter) {
        return Err(format!(
            "component `{component}` of the pool's DGD gives its scaling adapter the name `{adapter}`, which dynamo would hash: name the component with letters, digits and `-`"
        ));
    }
    Ok(adapter)
}

/// Whether `name` is a DNS subdomain, as Kubernetes names an object.
fn dns_subdomain(name: &str) -> bool {
    let edge = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    name.len() <= 253
        && edge(name.chars().next())
        && edge(name.chars().last())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

/// Refused unless `dgd` is a DGD that pool `name` in `namespace` can own
/// as written, with each `(segment, component)` of `placed`; each named
/// component's pod templates then carry `checksum` in [`CONFIG`].
fn fit(
    name: &str,
    namespace: &str,
    dgd: &mut Value,
    placed: &[(&str, &str)],
    checksum: &str,
) -> Result<(), String> {
    // dynamo labels each pod of the DGD with its name, the pool's, and the
    // operator finds the pods by that label: it must fit a label's value.
    if name.len() > crate::labels::LABEL_LIMIT {
        return Err(format!(
            "pool `{name}` has a `dgd`, and its name is longer than {} characters, which a label's value holds: dynamo labels the DGD's pods with it",
            crate::labels::LABEL_LIMIT
        ));
    }
    if dgd["apiVersion"] != DGD_API_VERSION || dgd["kind"] != DGD_KIND {
        return Err(format!(
            "the pool's `dgd` is a {} of {}: a pool owns a {DGD_KIND} of {DGD_API_VERSION}",
            dgd["kind"], dgd["apiVersion"]
        ));
    }
    // The operator finds the DGD's pods by its name, so it is the pool's,
    // and in the pool's namespace: a DGD that says otherwise is refused,
    // never renamed.
    for (field, wanted) in [("name", name), ("namespace", namespace)] {
        if let Some(given) = dgd["metadata"][field].as_str()
            && given != wanted
        {
            return Err(format!(
                "the pool's `dgd` has {field} `{given}`, and it must be `{wanted}`, the pool's"
            ));
        }
    }
    // Checked lookups: indexing a field of a JSON value that is not an
    // object would panic, and a pool's `dgd` is text anyone can write.
    let components = dgd
        .get_mut("spec")
        .and_then(Value::as_object_mut)
        .and_then(|spec| spec.get_mut("components"))
        .and_then(Value::as_array_mut)
        .ok_or("the pool's `dgd` has no `spec.components` list")?;
    for (at, (segment, component)) in placed.iter().enumerate() {
        if let Some((other, _)) = placed[..at].iter().find(|(_, c)| c == component) {
            return Err(format!(
                "segments `{other}` and `{segment}` both run in component `{component}` of the pool's DGD"
            ));
        }
        let found = components
            .iter_mut()
            .find(|c| c["name"] == *component)
            .ok_or_else(|| {
                format!(
                    "segment `{segment}` runs in component `{component}`, which the pool's DGD does not have"
                )
            })?;
        if found["roles"]
            .as_array()
            .is_some_and(|roles| roles.iter().any(|role| !role.is_object()))
        {
            return Err(format!(
                "component `{component}` of the pool's DGD has a role that is not an object"
            ));
        }
        for template in templates(found) {
            annotate(template, component, checksum)?;
            tune(template, component)?;
        }
    }
    Ok(())
}

/// The DGD of `pool`, named `name` in `namespace`, with `labels` and
/// `annotations` added to its own. A component that runs a segment
/// carries `checksum` in its pods' `gdl.dev/config`, so that a change to
/// the pool's ConfigMap rolls them. A component with a scaling adapter
/// carries no `replicas`: the adapter owns the count, and dynamo's
/// webhook refuses any other writer. `None` for a pool without a DGD.
/// Refused when the DGD is not one the pool can own as written, or a
/// segment names a component it does not have.
pub(crate) fn render(
    name: &str,
    namespace: &str,
    pool: &Pool,
    labels: Map<String, Value>,
    annotations: Map<String, Value>,
    checksum: &str,
) -> Result<Option<Value>, String> {
    let placed: Vec<(&str, &str)> = pool
        .segments
        .iter()
        .filter_map(|segment| Some((segment.name.as_str(), segment.component.as_deref()?)))
        .collect();
    let Some(text) = &pool.dgd else {
        return match placed.first() {
            Some((segment, component)) => Err(format!(
                "segment `{segment}` runs in component `{component}` of the pool's DGD, and the pool has no `dgd`"
            )),
            None => Ok(None),
        };
    };
    let mut dgd: Value =
        serde_json::from_str(text).map_err(|e| format!("the pool's `dgd` is not JSON: {e}"))?;
    fit(name, namespace, &mut dgd, &placed, checksum)?;
    let components = dgd
        .pointer_mut("/spec/components")
        .and_then(Value::as_array_mut);
    for component in components.into_iter().flatten() {
        if !component["scalingAdapter"].is_null()
            && let Some(fields) = component.as_object_mut()
        {
            fields.remove("replicas");
        }
    }
    // Only what a pool's apply writes: what the API server sets, such as
    // a uid or a status, would be refused by a server-side apply.
    let mut own_labels = dgd["metadata"]["labels"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    own_labels.extend(labels);
    let mut own_annotations = dgd["metadata"]["annotations"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    own_annotations.extend(annotations);
    let mut metadata = Map::new();
    metadata.insert("name".into(), name.into());
    metadata.insert("namespace".into(), namespace.into());
    metadata.insert("labels".into(), own_labels.into());
    if !own_annotations.is_empty() {
        metadata.insert("annotations".into(), own_annotations.into());
    }
    dgd["metadata"] = metadata.into();
    if let Some(fields) = dgd.as_object_mut() {
        fields.remove("status");
    }
    Ok(Some(dgd))
}

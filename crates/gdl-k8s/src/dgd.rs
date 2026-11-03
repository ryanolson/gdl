//! A pool's DynamoGraphDeployment (DGD): dynamo's deployment unit, which
//! runs the segments placed in its components ([`Segment::component`])
//! beside dynamo's own workers. The pool owns it, so that its stop
//! orders it and garbage collection removes it, and dynamo's operator
//! runs its pods. See `docs/src/guides/operator.md`.

use serde_json::{Map, Value};

use crate::pool::{Pool, Segment};

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
    let by_role = component["type"] == "lpx"
        || component["roles"]
            .as_array()
            .is_some_and(|roles| roles.iter().any(|role| !role["podTemplate"].is_null()));
    if by_role && component["roles"].is_array() {
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
    let placed: Vec<(&Segment, &str)> = pool
        .segments
        .iter()
        .filter_map(|segment| Some((segment, segment.component.as_deref()?)))
        .collect();
    let Some(text) = &pool.dgd else {
        return match placed.first() {
            Some((segment, component)) => Err(format!(
                "segment `{}` runs in component `{component}` of the pool's DGD, and the pool has no `dgd`",
                segment.name
            )),
            None => Ok(None),
        };
    };
    let mut dgd: Value =
        serde_json::from_str(text).map_err(|e| format!("the pool's `dgd` is not JSON: {e}"))?;
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
    let components = dgd["spec"]["components"]
        .as_array_mut()
        .ok_or("the pool's `dgd` has no `spec.components` list")?;
    for (at, (segment, component)) in placed.iter().enumerate() {
        if let Some((other, _)) = placed[..at].iter().find(|(_, c)| c == component) {
            return Err(format!(
                "segments `{}` and `{}` both run in component `{component}` of the pool's DGD",
                other.name, segment.name
            ));
        }
        let found = components
            .iter_mut()
            .find(|c| c["name"] == *component)
            .ok_or_else(|| {
                format!(
                    "segment `{}` runs in component `{component}`, which the pool's DGD does not have",
                    segment.name
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
        }
    }
    for component in components.iter_mut() {
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

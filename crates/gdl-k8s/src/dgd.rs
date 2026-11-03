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
        let template = &mut found["podTemplate"];
        if template.is_null() {
            *template = Value::Object(Map::new());
        }
        template["metadata"]["annotations"]["gdl.dev/config"] = checksum.into();
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

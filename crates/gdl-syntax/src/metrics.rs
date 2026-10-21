use gdl_model::{MetricField, MetricKind, RUNTIME_LABELS, RUNTIME_METRICS};
use syn::ext::IdentExt;
use syn::spanned::Spanned;

/// Read the metrics of a struct that derives `Metrics`, one per field in
/// field order, and refuse a name, label, or bucket list that breaks
/// dynamo's conventions or collides with what the runtime adds. The derive
/// and the source reader both read a metrics type through this, so they
/// cannot accept different definitions.
pub fn metric_fields(fields: &syn::Fields) -> syn::Result<Vec<MetricField>> {
    let syn::Fields::Named(fields) = fields else {
        return Err(syn::Error::new(
            fields.span(),
            "Metrics requires a struct with named fields",
        ));
    };
    fields.named.iter().map(metric_field).collect()
}

fn metric_field(field: &syn::Field) -> syn::Result<MetricField> {
    let ident = field.ident.as_ref().expect("named field");
    let name = ident.unraw().to_string();
    let (kind, vector) = kind(&field.ty)?;
    let mut buckets = None;
    let mut labels = None;
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("gdl")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("buckets") {
                if buckets.is_some() {
                    return Err(meta.error("buckets declared twice"));
                }
                buckets = Some(bounds(&meta.value()?.parse::<syn::ExprArray>()?)?);
                Ok(())
            } else if meta.path.is_ident("labels") {
                if labels.is_some() {
                    return Err(meta.error("labels declared twice"));
                }
                labels = Some(label_names(&meta.value()?.parse::<syn::ExprArray>()?)?);
                Ok(())
            } else {
                Err(meta.error("expected buckets or labels"))
            }
        })?;
    }
    let refuse =
        |message: String| syn::Error::new(ident.span(), format!("metric `{name}`: {message}"));
    if let Some(problem) = name_problem(&name, kind) {
        return Err(refuse(problem));
    }
    let buckets = match (kind, buckets) {
        (MetricKind::Histogram, Some(buckets)) => buckets,
        (MetricKind::Histogram, None) => {
            return Err(refuse("a histogram needs `#[gdl(buckets = [...])]`".into()));
        }
        (_, Some(_)) => return Err(refuse("only a histogram has buckets".into())),
        (_, None) => Vec::new(),
    };
    let labels = match (vector, labels) {
        (true, Some(labels)) => labels,
        (true, None) => return Err(refuse("a vector needs `#[gdl(labels = [...])]`".into())),
        (false, Some(_)) => {
            return Err(refuse(
                "only a vector has labels; use the type's `Vec` form".into(),
            ));
        }
        (false, None) => Vec::new(),
    };
    for (index, label) in labels.iter().enumerate() {
        if let Some(problem) = label_problem(label, kind) {
            return Err(refuse(format!("label `{label}`: {problem}")));
        }
        if labels[..index].contains(label) {
            return Err(refuse(format!("label `{label}` is named twice")));
        }
    }
    let help = doc(&field.attrs);
    if help.is_empty() {
        return Err(refuse(
            "document the field: its doc comment is the metric's help".into(),
        ));
    }
    Ok(MetricField {
        name,
        help,
        kind,
        labels,
        buckets,
    })
}

/// The kind of a metric field's type, and whether it is a vector, from the
/// type's last path segment: the derive sees the type's name, not what it
/// resolves to.
fn kind(ty: &syn::Type) -> syn::Result<(MetricKind, bool)> {
    let unsupported = || {
        syn::Error::new(
            ty.span(),
            "a metric is a prometheus `IntCounter`, `Counter`, `IntGauge`, `Gauge`, `Histogram`, or the `Vec` form of one",
        )
    };
    let syn::Type::Path(path) = ty else {
        return Err(unsupported());
    };
    let last = path.path.segments.last().ok_or_else(unsupported)?;
    if !last.arguments.is_none() {
        return Err(unsupported());
    }
    Ok(match last.ident.to_string().as_str() {
        "IntCounter" | "Counter" => (MetricKind::Counter, false),
        "IntCounterVec" | "CounterVec" => (MetricKind::Counter, true),
        "IntGauge" | "Gauge" => (MetricKind::Gauge, false),
        "IntGaugeVec" | "GaugeVec" => (MetricKind::Gauge, true),
        "Histogram" => (MetricKind::Histogram, false),
        "HistogramVec" => (MetricKind::Histogram, true),
        _ => return Err(unsupported()),
    })
}

/// Why `name` breaks dynamo's naming rules for a metric of `kind`
/// (`lib/runtime/src/metrics/prometheus_names.rs` in dynamo), if it does.
fn name_problem(name: &str, kind: MetricKind) -> Option<String> {
    let words: Vec<&str> = name.split('_').collect();
    let lowercase = name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !lowercase || words.iter().any(|w| w.is_empty()) {
        return Some(
            "a name is lowercase letters, digits, and single underscores, starting with a letter"
                .into(),
        );
    }
    if RUNTIME_METRICS.contains(&name) {
        return Some("the runtime records a metric of this name for every node".into());
    }
    let counter = kind == MetricKind::Counter;
    let total = words.last() == Some(&"total");
    if counter && !total {
        return Some("a counter's name ends in `_total`".into());
    }
    if !counter && total {
        return Some(format!(
            "only a counter's name ends in `_total`; this is a {}",
            kind.name()
        ));
    }
    // The unit is the last word, or the word before `total`. dynamo calls
    // these words vague there (`prometheus_names.rs`), though it uses them
    // inside names (`worker_last_time_to_first_token_seconds`).
    let unit = if counter && words.len() > 1 {
        words[words.len() - 2]
    } else {
        words[words.len() - 1]
    };
    if ["counter", "gauge", "size", "time"].contains(&unit) {
        return Some(format!(
            "`_{unit}` is too vague for a name; say what is counted, or the unit (`_total`, `_seconds`, `_bytes`)"
        ));
    }
    // A histogram `wait_seconds` serves `wait_seconds_count`; a gauge of
    // that name beside it would collide in the scrape.
    if ["sum", "count", "bucket"].contains(&unit) {
        return Some(format!(
            "`_{unit}` is a histogram's own series; name the metric for what it measures"
        ));
    }
    if words.iter().any(|w| *w == "duration" || *w == "latency") && unit != "seconds" {
        return Some(if counter {
            "a duration is in seconds: end the name in `_seconds_total`".into()
        } else {
            "a duration is in seconds: end the name in `_seconds`".into()
        });
    }
    None
}

/// Why `label` cannot name a label of a metric of `kind`, if it cannot.
fn label_problem(label: &str, kind: MetricKind) -> Option<&'static str> {
    let valid = label.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Some("a label is letters, digits, and underscores, not starting with a digit");
    }
    if label.starts_with("__") {
        return Some("labels starting with `__` are reserved to Prometheus");
    }
    if RUNTIME_LABELS.contains(&label) {
        return Some("the runtime adds this label to every series");
    }
    if kind == MetricKind::Histogram && label == "le" {
        return Some("a histogram's buckets take `le`");
    }
    None
}

/// The bucket bounds of `#[gdl(buckets = [...])]`: numeric literals,
/// finite and strictly increasing.
fn bounds(array: &syn::ExprArray) -> syn::Result<Vec<f64>> {
    let mut bounds = Vec::with_capacity(array.elems.len());
    for elem in &array.elems {
        let (negative, lit) = match elem {
            syn::Expr::Unary(syn::ExprUnary {
                op: syn::UnOp::Neg(_),
                expr,
                ..
            }) => (true, expr.as_ref()),
            other => (false, other),
        };
        let value = match lit {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Float(f),
                ..
            }) => f.base10_parse::<f64>()?,
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(i),
                ..
            }) => i.base10_parse::<f64>()?,
            _ => return Err(syn::Error::new(elem.span(), "a bucket bound is a number")),
        };
        let value = if negative { -value } else { value };
        if !value.is_finite() {
            return Err(syn::Error::new(elem.span(), "a bucket bound is finite"));
        }
        if bounds.last().is_some_and(|last| *last >= value) {
            return Err(syn::Error::new(
                elem.span(),
                "bucket bounds are strictly increasing",
            ));
        }
        bounds.push(value);
    }
    if bounds.is_empty() {
        return Err(syn::Error::new(
            array.span(),
            "a histogram has at least one bucket",
        ));
    }
    Ok(bounds)
}

/// The label names of `#[gdl(labels = [...])]`: string literals.
fn label_names(array: &syn::ExprArray) -> syn::Result<Vec<String>> {
    let labels = array
        .elems
        .iter()
        .map(|elem| match elem {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) => Ok(s.value()),
            _ => Err(syn::Error::new(elem.span(), "a label name is a string")),
        })
        .collect::<syn::Result<Vec<_>>>()?;
    if labels.is_empty() {
        return Err(syn::Error::new(
            array.span(),
            "a vector has at least one label",
        ));
    }
    Ok(labels)
}

/// A field's doc comment, its lines trimmed and joined by spaces.
fn doc(attrs: &[syn::Attribute]) -> String {
    attrs
        .iter()
        .filter_map(|a| {
            if !a.path().is_ident("doc") {
                return None;
            }
            if let syn::Meta::NameValue(n) = &a.meta
                && let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) = &n.value
            {
                return Some(s.value().trim().to_string());
            }
            None
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(input: syn::DeriveInput) -> syn::Result<Vec<MetricField>> {
        let syn::Data::Struct(data) = input.data else {
            panic!("struct")
        };
        metric_fields(&data.fields)
    }

    /// The message a one-field metrics struct is refused with.
    fn refused(field: proc_macro2::TokenStream) -> String {
        let input: syn::DeriveInput = syn::parse_quote!(struct M { #field });
        match read(input) {
            Ok(fields) => panic!("accepted: {fields:?}"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn each_kind_reads_with_its_help_labels_and_buckets() {
        let input: syn::DeriveInput = syn::parse_quote! {
            struct EngineMetrics {
                /// Tokens generated.
                tokens_total: prometheus::IntCounter,
                /// Requests refused,
                /// by reason.
                #[gdl(labels = ["reason"])]
                refused_total: gdl::prometheus::IntCounterVec,
                /// Sequences in the batch.
                active_sequences: IntGauge,
                /// Time to the first token.
                #[gdl(buckets = [0.005, 1, 2.5])]
                time_to_first_token_seconds: Histogram,
                /// Queue wait, by queue.
                #[gdl(buckets = [-1.0, 0.0], labels = ["queue"])]
                queue_wait_seconds: HistogramVec,
            }
        };
        let fields = read(input).expect("valid metrics");
        let summary: Vec<_> = fields
            .iter()
            .map(|f| (f.name.as_str(), f.kind, f.labels.clone(), f.buckets.clone()))
            .collect();
        assert_eq!(
            summary,
            [
                ("tokens_total", MetricKind::Counter, vec![], vec![]),
                (
                    "refused_total",
                    MetricKind::Counter,
                    vec!["reason".to_string()],
                    vec![]
                ),
                ("active_sequences", MetricKind::Gauge, vec![], vec![]),
                (
                    "time_to_first_token_seconds",
                    MetricKind::Histogram,
                    vec![],
                    vec![0.005, 1.0, 2.5]
                ),
                (
                    "queue_wait_seconds",
                    MetricKind::Histogram,
                    vec!["queue".to_string()],
                    vec![-1.0, 0.0]
                ),
            ]
        );
        assert_eq!(fields[1].help, "Requests refused, by reason.");
    }

    #[test]
    fn names_follow_dynamos_conventions() {
        for (field, why) in [
            (
                quote::quote!(/** x */ tokens: IntCounter),
                "ends in `_total`",
            ),
            (
                quote::quote!(/** x */ active_total: IntGauge),
                "only a counter's name ends in `_total`",
            ),
            (
                quote::quote!(/** x */ request_counter_total: IntCounter),
                "`_counter` is too vague",
            ),
            (
                quote::quote!(/** x */ queue_gauge: IntGauge),
                "`_gauge` is too vague",
            ),
            (
                quote::quote!(/** x */ batch_size: IntGauge),
                "`_size` is too vague",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1.0])] request_time: Histogram),
                "`_time` is too vague",
            ),
            (
                quote::quote!(/** x */ wait_time_total: Counter),
                "`_time` is too vague",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1.0])] wait_sum: Histogram),
                "`_sum` is a histogram's own series",
            ),
            (
                quote::quote!(/** x */ queue_count: IntGauge),
                "`_count` is a histogram's own series",
            ),
            (
                quote::quote!(/** x */ queue_bucket: IntGauge),
                "`_bucket` is a histogram's own series",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1.0])] step_duration_ms: Histogram),
                "end the name in `_seconds`",
            ),
            (
                quote::quote!(/** x */ queue_latency_total: Counter),
                "end the name in `_seconds_total`",
            ),
            (
                quote::quote!(/** x */ Tokens_total: IntCounter),
                "lowercase letters",
            ),
            (
                quote::quote!(/** x */ tokens__total: IntCounter),
                "single underscores",
            ),
            (
                quote::quote!(/** x */ requests_total: IntCounter),
                "the runtime records",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1.0])] request_duration_seconds: Histogram),
                "the runtime records",
            ),
        ] {
            let message = refused(field.clone());
            assert!(message.contains(why), "{field}: {message}");
        }
        // The vague words are refused as the unit, not inside a name, as
        // dynamo's own names use them.
        let input: syn::DeriveInput = syn::parse_quote!(
            struct M {
                /// x
                #[gdl(buckets = [1.0])]
                time_to_first_token_seconds: Histogram,
                /// x
                #[gdl(buckets = [1.0])]
                worker_last_time_to_first_token_seconds: Histogram,
                /// x
                kv_cache_block_size_bytes: IntGauge,
                /// x
                tokens_counted_total: IntCounter,
                /// x
                busy_seconds_total: Counter,
                /// x
                last_update_timestamp_seconds: Gauge,
            }
        );
        read(input).expect("valid names");
    }

    #[test]
    fn labels_and_buckets_belong_to_their_kinds() {
        for (field, why) in [
            (
                quote::quote!(/** x */ refused_total: IntCounterVec),
                "a vector needs",
            ),
            (
                quote::quote!(/** x */ #[gdl(labels = ["a"])] refused_total: IntCounter),
                "only a vector has labels",
            ),
            (
                quote::quote!(/** x */ #[gdl(labels = [])] refused_total: IntCounterVec),
                "at least one label",
            ),
            (
                quote::quote!(/** x */ #[gdl(labels = ["a", "a"])] refused_total: IntCounterVec),
                "named twice",
            ),
            (
                quote::quote!(/** x */ #[gdl(labels = ["worker_id"])] refused_total: IntCounterVec),
                "the runtime adds this label",
            ),
            (
                quote::quote!(/** x */ #[gdl(labels = ["dynamo_endpoint"])] refused_total: IntCounterVec),
                "the runtime adds this label",
            ),
            (
                quote::quote!(/** x */ #[gdl(labels = ["__name"])] refused_total: IntCounterVec),
                "reserved to Prometheus",
            ),
            (
                quote::quote!(/** x */ #[gdl(labels = ["1a"])] refused_total: IntCounterVec),
                "not starting with a digit",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1.0], labels = ["le"])] wait_seconds: HistogramVec),
                "take `le`",
            ),
            (
                quote::quote!(/** x */ wait_seconds: Histogram),
                "needs `#[gdl(buckets",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1.0])] tokens_total: IntCounter),
                "only a histogram has buckets",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [])] wait_seconds: Histogram),
                "at least one bucket",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1.0, 1.0])] wait_seconds: Histogram),
                "strictly increasing",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [2.0, 1.0])] wait_seconds: Histogram),
                "strictly increasing",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = ["1"])] wait_seconds: Histogram),
                "a bucket bound is a number",
            ),
            (
                quote::quote!(/** x */ #[gdl(buckets = [1e400])] wait_seconds: Histogram),
                "finite",
            ),
            (
                quote::quote!(/** x */ #[gdl(bucket = [1.0])] wait_seconds: Histogram),
                "expected buckets or labels",
            ),
        ] {
            let message = refused(field.clone());
            assert!(message.contains(why), "{field}: {message}");
        }
    }

    #[test]
    fn a_metric_has_help_and_a_prometheus_type() {
        let message = refused(quote::quote!(tokens_total: IntCounter));
        assert!(message.contains("doc comment"), "{message}");
        for ty in [
            quote::quote!(u64),
            quote::quote!(GenericCounter<AtomicU64>),
            quote::quote!(&'static IntCounter),
        ] {
            let message = refused(quote::quote!(/** x */ tokens_total: #ty));
            assert!(
                message.contains("a metric is a prometheus"),
                "{ty}: {message}"
            );
        }
        let input: syn::DeriveInput = syn::parse_quote!(
            struct M(IntCounter);
        );
        let message = read(input).expect_err("a tuple struct").to_string();
        assert!(message.contains("named fields"), "{message}");
    }
}

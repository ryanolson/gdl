# Define node metrics

A node defines its metrics as it defines its configuration. The segment registers them and hands the built struct to the node's `build`. This guide shows the steps. The runtime records its own per-segment figures without any work from the node. [Health and metrics](../operations/observability.md) covers those.

## 1. Declare the metrics

Derive `gdl::Metrics` on a struct of prometheus metric fields. Name the types through `gdl::prometheus`, the version the runtime registers and serves.

```rust
#[derive(gdl::Metrics)]
pub struct EngineMetrics {
    /// Tokens generated.
    tokens_total: gdl::prometheus::IntCounter,
    /// Time to the first token.
    #[gdl(buckets = [0.005, 0.05, 0.5, 5.0])]
    time_to_first_token_seconds: gdl::prometheus::Histogram,
}
```

- The field's name is the metric's name. The registry adds the prefix (see [Metrics reference](../appendix/metrics.md)).
- The doc comment is the help text. A field without one is refused.
- A field's type is one of `IntCounter`, `Counter`, `IntGauge`, `Gauge`, `Histogram`, or the `Vec` form of each. The derive reads the type's last path segment.
- A histogram needs `#[gdl(buckets = [...])]`: at least one bound, finite, strictly increasing. A `Vec` form needs `#[gdl(labels = [...])]`, with at least one label, as in `#[gdl(labels = ["reason"])] refused_total: IntCounterVec`. No other field takes either.

## 2. Follow the naming rules

The derive refuses a name that breaks dynamo's conventions:

- A name is lowercase letters, digits, and single `_`s, and starts with a letter.
- A counter's name ends in `_total`, and no other kind's name does.
- The unit is the last word, or the word before `total`. It cannot be `counter`, `gauge`, `size`, or `time`. It cannot be `sum`, `count`, or `bucket`, which a histogram's own series use. So `batch_size` and `request_counter_total` are refused, and `worker_last_time_to_first_token_seconds` passes.
- A name with the word `duration` or `latency` has the unit `seconds` (`_seconds`, or `_seconds_total` for a counter). This is stricter than dynamo, which has `tokenizer_latency_ms`.
- The runtime's names are reserved, and so are the labels it adds. Both lists are in [Metrics reference](../appendix/metrics.md#reserved-names).
- A label is letters, digits, and `_`, does not start with a digit or `__`, appears once, and is not `le` on a histogram.

## 3. Take the metrics in `build`

```rust
#[derive(gdl::Node)]
#[gdl(metrics = EngineMetrics)]
pub struct Engine { metrics: EngineMetrics }

impl gdl::Build for Engine {
    type Config = Settings;
    type Metrics = EngineMetrics;
    async fn build(config: Settings, metrics: EngineMetrics) -> gdl::Result<Self> {
        Ok(Self { metrics })
    }
}
```

`#[gdl(metrics = T)]` names the type for the catalogue, and must be the node's `Build::Metrics`. A node without metrics writes `type Metrics = ();` and takes `(): ()`. To count items, clone a metric into the response stream, as `examples/chat/engine-mock` does. Only the derive implements `gdl::Metrics`, because the source reader takes the definitions from its attributes ([The catalogue and the source reader](../concepts/catalogue.md)).

The segment registers each node's metrics with `dynamo_endpoint` set to the node's name. A frontend's or service's metrics use the source's name, and an entry adapter's use its entry's name. A segment fails to build when two of its nodes define one name with a different kind, help, label set, or buckets. The error names both nodes. Scrape `/metrics` on the system listener to see the series.

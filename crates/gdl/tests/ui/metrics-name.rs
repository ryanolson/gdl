// A counter's name ends in `_total`, as dynamo's conventions say.
#[derive(gdl::Metrics)]
struct EngineMetrics {
    /// Tokens generated.
    tokens: gdl::prometheus::IntCounter,
}

fn main() {}

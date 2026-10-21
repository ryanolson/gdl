use gdl::Node;

/// The engine: its role impl is in another module.
#[derive(Node)]
#[gdl(metrics = EngineMetrics)]
pub struct Engine;

/// What the engine reports.
#[derive(gdl::Metrics)]
pub struct EngineMetrics {
    /// Tokens generated.
    tokens_total: gdl::prometheus::IntCounter,
    /// Time to the first token.
    #[gdl(buckets = [0.01, 1.0])]
    time_to_first_token_seconds: gdl::prometheus::Histogram,
}

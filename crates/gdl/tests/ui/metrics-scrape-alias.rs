// A type named `ScrapeGaugeVec` that is not gdl's: the catalogue reads the
// field as a gauge by its name, so the build must refuse a counter under it.
mod other {
    pub type ScrapeGaugeVec = gdl::prometheus::IntCounterVec;
}

#[derive(gdl::Metrics)]
struct GateMetrics {
    /// Each model's status.
    #[gdl(labels = ["model"])]
    model_status: other::ScrapeGaugeVec,
}

fn main() {}

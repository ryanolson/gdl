// The source reader takes a metrics type's definitions from
// `derive(gdl::Metrics)` and cannot run a hand-written impl, so the build
// refuses one too.
struct EngineMetrics;

impl gdl::Metrics for EngineMetrics {
    fn definitions() -> Vec<gdl::MetricField> {
        Vec::new()
    }

    fn register(_: &gdl::prometheus::Registry, _: &str) -> gdl::prometheus::Result<Self> {
        Ok(Self)
    }
}

fn main() {}

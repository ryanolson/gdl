// The metrics type a node names for the catalogue is the one its build
// registers: naming another fails to compile.

#[derive(gdl::Metrics)]
pub struct EngineMetrics {
    /// Tokens generated.
    tokens_total: gdl::prometheus::IntCounter,
}

#[derive(gdl::Node)]
#[gdl(metrics = EngineMetrics)]
pub struct Engine;

impl gdl::Build for Engine {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> gdl::Result<Self> {
        Ok(Self)
    }
}

impl gdl::Sink for Engine {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _cx: gdl::Cx, req: u32) -> gdl::Result<gdl::Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(req)])))
    }
}

fn main() {}

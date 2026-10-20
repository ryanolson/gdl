// The source reader takes a config type's fields from `derive(gdl::Config)`
// and cannot run a hand-written `graph_fields`, so the build refuses one too.
struct Settings;

impl gdl::ConfigSchema for Settings {
    fn graph_fields() -> Vec<gdl::ConfigField> {
        Vec::new()
    }
}

fn main() {}

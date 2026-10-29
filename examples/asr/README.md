# The ASR example

A speech recognition pipeline with a mock recognizer. Its request streams: a session's settings, then audio chunk by chunk. Transcripts stream back while the audio still streams in.

| Path | Holds |
|---|---|
| `asr.gdl` | The graph |
| `gdl.toml` | The spec: the graph, the crates, and three segments |
| `protocols` | The messages |
| `front-nodes`, `dsp-nodes`, `engine-mock` | The nodes, each marked with `derive(gdl::Node)` |
| `demo` | A program that runs the three segments in one process through their libraries |

Build the segments from the repository root:

```bash
cargo run -p gdl-gen -- build --spec examples/asr/gdl.toml
```

The demo links the generated segment libraries, so it is not a member of this workspace. `crates/gdl-gen/tests/asr.rs` generates the segment crates in the target directory and builds the demo beside them:

```bash
cargo nextest run -p gdl-gen --test asr --no-run
timeout 300 cargo nextest run -p gdl-gen --test asr
```

See [Run the examples](../../docs/src/guides/examples.md#asr).

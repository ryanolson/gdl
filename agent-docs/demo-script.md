# Composer demo: script (draft)

A screencast of the composer, from crates to a running graph, on one machine. It uses the chat example: seven nodes in four crates (`examples/chat`).

```
crates on disk --add--> catalogue --compose--> logical graph --cut--> 3 segments
   --download--> chat.tar.gz --build-all.sh--> 3 binaries --run--> answers
```

## Before recording (off camera)

1. Start from a clean `main`.
2. Warm the build cache, so the build on camera takes seconds, not minutes. Build `gdl-web`, and build the chat example's segments once into the target directory the demo uses:

   ```bash
   cargo build --release -p gdl-web
   export DEMO=/tmp/gdl-demo && rm -rf $DEMO && mkdir -p $DEMO
   CARGO_TARGET_DIR=$DEMO/build cargo build --release --manifest-path examples/chat/Cargo.toml --bins
   ```

3. Use a terminal with large type, and a browser window beside it.

## Scene 1: start the composer (about 20 s)

```bash
target/release/gdl-web --catalog $DEMO/catalogue --target-dir $DEMO/target \
  --gdl-path crates/gdl --local-root "$PWD" --listen 127.0.0.1:8080
```

Open `http://127.0.0.1:8080/`. Say that the catalogue is empty, and that the server builds nothing: it reads crates' source.

## Scene 2: import crates (about 45 s)

In **Catalog**, choose **Add from local** for each crate. Browse to its directory and press **Add package**:

- `examples/chat/protocols`: the messages.
- `examples/chat/front-nodes`: Preprocess, Truncate, Number.
- `examples/chat/engine-mock`: Engine. The real one would need a GPU; reading its source does not.
- `examples/chat/post-nodes`: Detokenize, Format.

Expand `chat-protocols` to show its messages, even though it has no nodes. Expand a message to show its Rust path and codec availability.

Point out that each crate marks its nodes with `#[derive(gdl::Node)]` and its messages with `#[derive(gdl_message::Message)]`, and that nothing else is written by hand.

## Scene 3: compose the logical graph (about 90 s)

The request flows down the page to the inner end, and the response flows back up to the source.

1. Use the pencil beside Graph Name to name the graph `chat`. Open Source settings and name the source `Frontend`. Select **Single** input with `Prompt` and **Stream** output with `Chunk`.
2. Choose the inner end: only nodes whose request is `Prompt` are offered. Choose the operator `Preprocess`.
3. In Preprocess's own hop:
   - add the forward node `Truncate`;
   - choose the inner end `Engine` (only a sink that takes `Words` is offered);
   - add the backward nodes `Detokenize`, then `Format`.
4. Back in the outer hop, add the backward node `Number`.
5. Press **Check graph**. It reports one segment, no crossings, and shows the graph file and the spec that the drawing became.

## Scene 4: carve it into segments (about 45 s)

After the complete graph passes validation, open the **Segments** tab.

1. Beside Engine, choose **New segment…**.
2. Beside Detokenize, choose **New segment…**. Place Format in that same segment.
3. Expand **Rename segments**. Name the source group `front`, the Engine group `engine`, and the Detokenize/Format group `post`.

The summary shows three segments and three network crossings. Click `Truncate → Engine` to show its placement dialog. Request and response boundaries update together when a node moves. Press **Check graph** to inspect the crossing report. A crossing requires a codec for its message.

## Scene 5: download (about 15 s)

Press **Download segments**: `chat.tar.gz`.

## Scene 6: build it (about 60 s)

```bash
cd $DEMO && tar xzf ~/Downloads/chat.tar.gz && ls chat
cat chat/engine/Cargo.toml        # only the engine segment depends on engine-mock
CARGO_TARGET_DIR=$DEMO/build sh chat/build-all.sh
```

Say that each segment is its own Cargo workspace, so a segment links only the nodes it hosts, and that `build-all.sh` builds them one by one into one shared cache.

## Scene 7: run the graph (about 60 s)

One config file per segment, sharing a fresh roster directory:

```bash
for s in front engine post; do
  printf 'listen = "127.0.0.1:0"\nroster = "%s/roster"\n' $DEMO > $DEMO/$s.toml
done
$DEMO/build/release/chat-engine $DEMO/engine.toml &
$DEMO/build/release/chat-post   $DEMO/post.toml &
echo '{"text": "hello big wide world", "max_words": 3}' | $DEMO/build/release/chat-front $DEMO/front.toml
```

The front segment reads one JSON request per line and writes the response items as JSON lines, after crossing to the engine and to post and back. It prints:

```text
front: ready
{"item":{"index":0,"text":"[wide]"}}
{"item":{"index":1,"text":"[big]"}}
{"item":{"index":2,"text":"[hello]"}}
{"done":null}
```

## Before recording, once

- **Rehearsed 2026-10-19:** scenes 6 and 7 ran as written, from an archive the server exported for this draft (with `GDL_PROFILE=dev` and a warm cache: 19 s to build), and printed the output above.
- **velo 0.21 (2026-10-19):** gdl now takes velo from its `main` (`515941c`), which builds on macOS. Scene 7 printed the output above again on Linux. On a Mac, drop any local `[patch.crates-io]` for velo before you build.
- **Rehearse.** `the_chat_example_composes_from_its_directories` (`gdl-web/tests/api.rs`) runs scenes 2 to 5 against the server without the page. A headless Chromium probe drove the page through scenes 3 and 4 and got the expected three crossings. Still, run the whole script once by hand.
- **Adding by git URL** is not built: the demo adds crates by their directories on this machine.

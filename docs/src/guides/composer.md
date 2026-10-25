# Compose in the browser

`gdl-web` serves the composer: a page that builds a graph from the nodes and messages of a working catalogue, cuts it into segments, and exports the generated crates or a Docker Compose bundle. The server reads crates' source and builds nothing to check or export a graph. This guide walks through the chat example (`examples/chat`): import its crates, draw the graph, cut it into three segments, download, build, and run.

## Start the server

```bash
cargo build --release -p gdl-web
target/release/gdl-web --catalog /tmp/gdl-demo/catalogue --target-dir /tmp/gdl-demo/target \
  --gdl-path crates/gdl-runtime --local-root "$PWD"
# gdl-web: serving http://127.0.0.1:8080/
```

| Flag | Default | What it does |
|---|---|---|
| `--catalog <dir>` | required | the working catalogue that imports add to |
| `--gdl-path <dir>` or `--gdl-git <url> --gdl-rev <rev>` | required | where generated crates take `gdl-runtime` from |
| `--listen <address>` | `127.0.0.1:8080` | the address to serve |
| `--target-dir <dir>` | `target` | where crates are fetched |
| `--local-root <dir>` | off | allow local imports, from this directory and below only |
| `--host <name>` | none | another name the server answers to; repeatable |
| `--docker` | off | allow **Build and apply** in the Deploy tab |

The server answers only to `localhost`, IP addresses, and each `--host` name. It refuses a request that changes state when its `Origin` or `Sec-Fetch-Site` is not its own. [Catalogue design](../development/catalogue-design.md#the-composers-server) explains these checks. Without `--local-root`, the page and the API offer only crates.io and Git. The page has no build step: the server embeds its HTML, script, style, and Mermaid 12.2.0 (`crates/gdl-web/assets/vendor/mermaid.js`, with its license beside it), so diagrams render without a CDN. `timeout 60 node --test crates/gdl-web/tests/editor.cjs` tests the page.

## Import crates

Open the page. In **Catalog**, the package, Git-branch, and folder icons open three import dialogs.

- **Add from crates.io** searches after a 350 ms pause in typing. Select a result, then take its current version or enter an exact one.
- **Add from Git** takes an HTTP(S) URL, a branch or commit, and an optional package path in the repository. The import pins the commit it resolved. Generated manifests keep that URL and commit.
- **Add from local** browses the `--local-root` directory. Choose a package directory and press **Add package**.

For the walkthrough, add four directories: `examples/chat/protocols` (the messages), `examples/chat/front-nodes` (Preprocess, Truncate, Number), `examples/chat/engine-mock` (Engine), and `examples/chat/post-nodes` (Detokenize, Format). A real engine would need a GPU to build. Reading its source does not.

The server reads each import in a child process with limited memory and time. A crate the reader cannot read is refused with its problems, and nothing is added. Expand a package to see its messages, with their Rust paths, visibility, and codec, and its nodes, with their roles and ports. Search filters package, message, and node names and Rust paths.

An empty catalogue offers **Load examples**, which imports `examples/api` from Git when the server takes gdl from Git. **Clear catalog** asks first, then moves the catalogue directory aside and clears the graph. It deletes no package sources.

## Draw the graph

Requests flow down the page to each hop's inner end. Responses flow back up through the backward nodes. Operators and sinks span both columns, and an operator's own hop is drawn beneath it.

1. Use the pencil beside **Graph Name** to name the graph `chat`. In the source card, choose **Single** input with `Prompt` and **Stream** output with `Chunk`. To host the graph instead, choose a catalogue frontend, whose ports then fix these messages.
2. At the inner end, choose `Preprocess`. Each slot offers only nodes whose port carries the arriving message.
3. In Preprocess's hop, add the forward node `Truncate`, the sink `Engine`, and the backward nodes `Detokenize` and `Format`.
4. In the outer hop, add the backward node `Number`.

You can also drag nodes and messages from the catalogue onto slots and cards. The server checks each complete graph and shows the result under **Validation & generated files**: a Mermaid diagram, problems, crossings, and the graph file and spec the drawing became. Every verdict is the server's. **Undo** and **Redo** (Ctrl/Command+Z, Ctrl/Command+Shift+Z) cover graph, placement, and router edits in the session.

For a `Service` frontend, each enabled endpoint gets its own arm. A dangling **+** offers **Add node**, **Connect existing**, and **Identity**. **Connect existing** shares a sink across arms. The canvas draws a shared sink once. A sink must match both the request and the response types. While you insert a conversion chain, an incomplete type match stays editable, and deployment stays disabled until the types align. [Hosted frontends](frontends.md) explains roots, shared sinks, and identity nodes.

## Cut it into segments

**Segments** opens once the graph passes.

1. Beside `Engine`, and then beside `Detokenize`, choose **New segment…**. Move `Format` into Detokenize's segment.
2. Expand **Rename segments**. Name the groups `front`, `engine`, and `post`.

The summary shows three segments and three network crossings. A move changes a node's request and response boundaries together. Clicking an edge opens a placement dialog for a connected node. On a scissors edge, choose **Only this node** or **Downstream chain**, which moves the chain down to the next existing boundary. An edge whose message has no codec stays local, and a cut on it is refused. Blue arrows are requests, and purple dashed arrows are responses.

A cut request edge has a router control. It offers **Round robin**, **Random**, and gdl's selectors, `affinity` and `least_loaded`. A choice writes `[routing]` in the spec. A selector from another crate is written in the spec by hand. [Write a selector](write-a-selector.md) and [Routing](../concepts/routing.md) cover both.

## Download and build

Press **Download segments**. In Graph mode the archive holds one segment. In Segments mode it uses your placement.

```bash
cd /tmp/gdl-demo && tar xzf ~/Downloads/chat.tar.gz
cat chat/engine/Cargo.toml     # only the engine segment depends on engine-mock
CARGO_TARGET_DIR=/tmp/gdl-demo/build sh chat/build-all.sh
```

Each segment is its own Cargo workspace, so it links only the nodes it hosts. `build-all.sh` builds them one at a time into one shared target directory. `GDL_PROFILE=dev` builds the dev profile. [Split, generate, and draw](split-and-generate.md) describes the archive.

## Run it

Give each segment a config that shares one fresh roster directory:

```bash
for s in front engine post; do
  printf 'listen = "127.0.0.1:0"\nroster = "/tmp/gdl-demo/roster"\n' > /tmp/gdl-demo/$s.toml
done
/tmp/gdl-demo/build/release/chat-engine /tmp/gdl-demo/engine.toml &
/tmp/gdl-demo/build/release/chat-post   /tmp/gdl-demo/post.toml &
echo '{"text": "hello big wide world", "max_words": 3}' \
  | /tmp/gdl-demo/build/release/chat-front /tmp/gdl-demo/front.toml
```

The front segment reads one JSON request per line, crosses to `engine` and `post` and back, and prints:

```text
front: ready
{"item":{"index":0,"text":"[wide]"}}
{"item":{"index":1,"text":"[big]"}}
{"item":{"index":2,"text":"[hello]"}}
{"done":null}
```

`the_chat_example_composes_from_its_directories` in `crates/gdl-web/tests/api.rs` runs the import, draw, cut, and download steps against the server.

## Deploy with Docker Compose

**Deploy** follows **Graph** and **Segments**. It checks the placement, then previews `compose.yaml`, the hub's Dockerfile, and each segment's Dockerfile and runtime settings.

Set the default runtime image and the build image, and override either per segment. Set published ports and runtime TOML per segment. Each change regenerates the preview. **Download Compose bundle** stays off until generation succeeds, and downloads the exact files previewed.

Deploy takes only crates from Git or crates.io, since an image build cannot see a local directory. Start the server with `--gdl-git` and `--gdl-rev` so that the runtime is fetchable too. Deploy writes a bundle without cells only, and refuses a spec with classes or several layouts.

The tab reports what the host offers on each visit: Docker Compose when `docker compose` and the daemon both answer, and Kubernetes when `kubectl` has a current context (reported only). With `--docker`, **Build and apply** generates the bundle again on the server from the previewed draft. It writes it to `<target-dir>/deployments/gdl-<graph>/` and runs `docker compose -p gdl-<graph> up --build --detach` there, showing the output as it runs. Applying again updates the same project. One apply runs at a time. [Deploy with Docker Compose](compose-deploy.md) covers the bundle itself.

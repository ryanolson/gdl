# CUT HERE film stage

This local HTML stage presents the 8:25 GDL film: 505 seconds and 39 spoken turns across 10 scenes. It reads `agent-docs/video/script.md`. The script remains the source for dialogue and ElevenLabs delivery tags.

With the empty media manifest, the stage is a silent animatic. Missing demo clips appear as placeholders. Accepted voice takes, recorded demonstrations, and a final audio mix remain production inputs.

Each first cast introduction has a first-name label and a face hold of at least one second. Ishan uses light slang. Ryan asks for a translation exactly once, in the opening. Kyle interrupts with friendly impatience. Stock cast and narrator voices are planned. Ryan can supply a custom ElevenLabs voice for himself.

## Install local dependencies

Run commands from the repository root unless a step states another directory.

```bash
npm --prefix tools/video ci
npm --prefix tools/video test
```

The project pins `playwright-core` and `ffmpeg-static`. Dependencies stay inside `tools/video/node_modules`. The renderer uses an existing Google Chrome installation. It does not download a browser or require a global package installation.

Run the separate Chrome regression suite:

```bash
timeout 60 node --test tools/video/tests/browser.mjs
```

This suite starts its own loopback server on an OS-assigned port. It covers capture playback, the script duration, the initial demo mode, and exact paused seeks. Ordinary unit tests remain browser-independent.

## Open the stage and composer

Start the stage in one terminal:

```bash
npm --prefix tools/video run dev
```

Open [the film stage](http://127.0.0.1:8088/). The server listens on `127.0.0.1` only.

Prepare the real API example catalog in another terminal:

```bash
cargo run -p gdl-gen -- extract examples/api --catalog tools/video/work/catalog
```

Start the composer:

```bash
cargo run -p gdl-web -- --catalog tools/video/work/catalog --listen 127.0.0.1:8089 --local-root "$PWD" --gdl-path crates/gdl-runtime
```

Open [the real GDL composer](http://127.0.0.1:8089/). The stage route `/composer` redirects to this address. The API example supplies a real catalog for rehearsal. It does not supply the proposed ACME, NIM-shaped, or Stargate demonstrations.

| Stage route | Source |
| --- | --- |
| `/script.md` | `agent-docs/video/script.md` |
| `/assets/` | `agent-docs/video/assets/` |
| `/media/` | `tools/video/work/media/` |
| `/composer` | Redirect to the composer on port 8089 |

## Record the API composer rehearsal

Keep the composer open with the API example catalog. Check the prerequisites:

```bash
node tools/video/record.mjs --dry-run
```

Record the real UI:

```bash
node tools/video/record.mjs --out tools/video/work/composer.mp4
```

The recorder uses installed Chrome in a fresh browser context. It enables Chat Completions and disables Responses and Messages. It adds ExtractChat, FormatChat, and Echo, then waits for the four-node flow to pass validation. In Segments, it moves Echo into an `engine` segment. In Deploy, it records the actual host state and the disabled Build and apply button. It never invokes deployment.

UI selectors establish readiness before each action. Short editorial holds give viewers time to read. The local catalog can reject deployment previews because deployment bundles require Git or crates.io dependencies.

The output is a silent 1920×1080 MP4. The recorder preserves the original WebM under `tools/video/work/recordings/`. It replaces the selected MP4 only after conversion completes. `--url` accepts a local composer address. `--help` lists the command options.

This recording is API rehearsal footage. The ACME, NIM, and Stargate scenes still require their own proof recordings.

The separate [film example workspace](../../examples/film/README.md) contains `gdl-demo-nodes` and `acme-gdl-nodes`. Those packages supply the mock engine, typed messages, response observer, configuration, and metrics for the film demonstration. The API recorder continues to use the API example catalog.

## Record the film composer rehearsal

The [film workspace](../../examples/film/README.md) supplies the reference engine, HTTP frontend, ACME tap, configuration schemas, and metrics. Extract both packages:

```bash
cargo run -p gdl-gen -- extract examples/film/gdl-demo-nodes --catalog tools/video/work/film-catalog
cargo run -p gdl-gen -- extract examples/film/acme-gdl-nodes --catalog tools/video/work/film-catalog
```

Start its composer separately:

```bash
cargo run -p gdl-web -- --catalog tools/video/work/film-catalog --listen 127.0.0.1:8090 --local-root "$PWD" --gdl-path crates/gdl-runtime
```

Record the three-node graph, configuration controls, process placement, and actual Deploy status:

```bash
node tools/video/record.mjs --fixture film --url http://127.0.0.1:8090/ --out tools/video/work/media/demo-composer.mp4
```

Set `composerUrl` to `http://127.0.0.1:8090/` in the selected media manifest. The live iframe and **Open gdl-web** link use that address. Recorded footage still needs its scene mapping in the manifest.

The composer currently omits marked message re-exports from its message list. Show their shared identity through source extraction and generated artifacts. The reference package defines its canonical messages at the crate root.

## Attach production media

The default manifest is `tools/video/dist/media.json`. Its `voices` and `footage` maps are empty. The `composerUrl` field identifies the live composer.

Keep local takes under `tools/video/work/media/`. This directory also holds an optional manifest:

```json
{
  "composerUrl": "http://127.0.0.1:8089/",
  "voices": {
    "opening-1": "/media/opening-1.wav",
    "interruption-1": "/media/kyle-interruption.wav"
  },
  "footage": {
    "catalog": "/media/catalog.mp4"
  }
}
```

Save this file as `tools/video/work/media/manifest.json`. Open [the stage with this manifest](http://127.0.0.1:8088/?manifest=/media/manifest.json).

Voice keys identify turns as `<scene-id>-<number>`, with numbers that start at one. `interruption-1` is Kyle's first interruption. The stage removes delivery tags from visible captions. The script retains those tags for TTS production.

Footage keys identify scenes. These scenes require recorded proof clips for a final export:

| Scene ID | Film interval |
| --- | --- |
| `catalog` | 80–130s |
| `types` | 130–170s |
| `acme` | 170–225s |
| `deployment` | 225–315s |
| `nim` | 315–360s |
| `stargate` | 360–430s |
| `receipts` | 430–475s |

The opening crawl occupies 0–24s. The cast interruption occupies 24–80s. Together they form the 80-second introduction. The final reveal occupies 475–505s. Cue timings remain provisional until the accepted voice takes and demonstration clips fit the edit.

## Export an MP4

Keep the stage server open. Run the prerequisite check for the opening:

```bash
node tools/video/render.mjs --from 0 --to 80 --preflight
```

Export the opening:

```bash
node tools/video/render.mjs --from 0 --to 80 --fps 30 --out tools/video/work/opening.mp4
```

This command creates a silent MP4. The opening does not require recorded demo clips.

Export a full silent animatic with explicit placeholders:

```bash
node tools/video/render.mjs --animatic --out tools/video/work/animatic.mp4
```

Without `--animatic`, the renderer refuses a range with missing recorded demo clips. Live composer iframes never enter deterministic exports.

After the production media and full audio mix exist, export the final film:

```bash
node tools/video/render.mjs \
  --url 'http://127.0.0.1:8088/?manifest=/media/manifest.json' \
  --audio tools/video/work/media/mix.wav \
  --out tools/video/work/cut-here.mp4
```

The renderer captures pixels only. Per-turn voice files support stage playback. They do not automatically become the MP4 soundtrack. The `--audio` file contains the complete mixed soundtrack, aligned to film time zero. For a clip export, the renderer applies the same `--from` offset to that track. Short audio tracks receive silence padding.

One headless Chrome instance captures `#film-stage` at 1920×1080. The renderer seeks each frame to `from + frame / fps`. PNG frames stream directly to ffmpeg with backpressure. No frame directory is necessary. The MP4 uses H.264, `yuv420p`, AAC for supplied audio, and faststart metadata.

The renderer replaces the output file only after a complete encode. It deletes partial output after an error.

The default output always resolves to `tools/video/work/film.mp4` inside this checkout, independent of the current directory. Explicit `--out` paths remain relative to the current directory. The default also works with `npm --prefix tools/video run render`.

## Prerequisites and overrides

Show all renderer options:

```bash
node tools/video/render.mjs --help
```

The renderer uses local `ffmpeg-static`, then `ffmpeg` on `PATH`. An explicit `--ffmpeg /absolute/path` or `FFMPEG_PATH` overrides that selection.

The renderer accepts local `playwright` or `playwright-core`. An explicit `--playwright /absolute/path/to/playwright/index.js` or `PLAYWRIGHT_MODULE_PATH` selects another installed module. The browser still uses the installed Chrome channel.

Preflight reports missing dependencies, an unavailable stage, invalid capture dimensions, or uncovered demo intervals. It creates no movie. A successful animatic export does not establish that the production demonstrations or soundtrack exist.

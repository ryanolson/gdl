# Catalogue design

This chapter records why the catalogue and the source reader work as they do, what was tried first, and what each rejected option cost. [The catalogue and the source reader](../concepts/catalogue.md) describes how they work. Read that chapter first.

## Why the reader reads source instead of building

A node crate can need a toolkit to build. A CUDA engine needs the CUDA toolkit, and checking a graph that names it needs none of it. A build also runs the crate's build scripts and proc macros, which is arbitrary code. The composer's server reads crates that users name, so it must run none of their code. Both facts rule out any design that compiles a node crate to learn what it offers.

So `cargo metadata` fetches sources, which runs no build script and no proc macro, and a reader of ours parses them with `syn`. This holds for the generator too. A graph spec compiles no contract: its manifest is built from the graph file and the resolution (`Graph::manifest`). A server that cannot build a CUDA crate cannot build any contract that names its messages either. `a_graph_files_manifest_is_built_as_its_contract_prints_it` still compiles the contracts of the chat example and two fixtures, and checks that the two manifests, hash included, are equal.

Two checks moved from the compiler to the reader when the contract stopped compiling:

- **Public paths.** Before, rustc's E0603 reported a message in a private module when the contract compiled. Now the reader records whether each definition path and export path is public, and resolution refuses a message with none, before anything is generated.
- **Wire capability.** The manifest's wire bits are now the reader's, not the compile-time probe's. A wrong "wire" fails the segment's build, as before. A wire type whose serde impls a macro writes is missed, and the generator refuses a cut that would work. That regression was accepted so that nothing is built. `--verify` shows it, and the remedy is to write the impls where the reader can see them.

**Why not rust-analyzer.** Its library crates resolve names as the compiler does. They are large, slow to build, and change their API often. The reader's output format does not depend on how names are resolved, so rust-analyzer can replace the resolver later, behind the same output, if real crates outgrow it.

## What was tried first

Each of these designs was built or ruled, then dropped. They are the negative results that the current design rests on.

### A catalogue program, linked at resolution

The first catalogue was a link-time list. `derive(Message)` put an entry in a `linkme` distributed slice: package, name, definition path, and whether the type is a wire type, from the probe at the concrete type. The generator built one program that linked every listed crate and printed the slice as TOML. A prototype showed that a crate's entries are kept when the program refers to it only with `use krate as _`, in debug and release, under `deny(unsafe_code)`.

Rejected at the time: a source scan (it misses re-exports and macro-made types, and cannot tell wire capability), rustdoc JSON (it needs nightly, and the toolchain stays on the stable pin), and a probe of `crate::Name` paths (it finds only crate-root items, and its errors are rustc diagnostics).

Building it found three things that still hold:

- **One program per set of crates.** One shared program relinked at every spec change, and the composition tests took 5.3 s warm. One program per set of listed crates, named by a hash of its dependencies, took 0.17 s. The `--verify` program is named by a hash of its manifest, its `main.rs`, and the lock it starts from.
- **A second gdl collides.** linkme names a slice's linker section by the static's name alone. Two gdl versions in one program share one section and panic on first use. The slice is named `GDL_MESSAGE_CATALOGUE`, so an unrelated `MESSAGES` registry cannot collide with it. So the `--verify` program takes a Git `gdl-message` under the branch, tag, or revision the crate's lock names, not under the commit it resolved to: Cargo counts each reference as its own source, and a pinned commit links a second copy, which a test in `crates/gdl-gen/tests/compose.rs` shows.
- **The linker decides, not a prediction.** The generator first predicted which packages the program would link from `cargo metadata`. Review found eight cases where that was wrong: a dependency for another platform, a build dependency, a proc macro's dependencies, features enabled only for a build script, `RUSTFLAGS`, and raw identifiers in a `cfg`, among them. Each fix copied another part of Cargo's logic. The generator then stopped predicting. It ran the program, read the duplicate-slice panic, and told packages apart by the directory each was built from (`CARGO_MANIFEST_DIR`). `--verify` keeps that rule today. A test in `crates/gdl-gen/tests/compose.rs` holds it: the crate it verifies has two dependencies that register messages, one of them another version of the crate itself. A filter by package name, as `cargo metadata` gives it, fails that test, whichever version `cargo metadata` lists first. One prediction is left: the program must name one `gdl-message` before it builds, and it takes the one nearest to the crate in `cargo metadata`'s dependencies. A dependency for another platform cannot change that choice unless it is as near or nearer ([Known limitations](../operations/limitations.md)).

The program was dropped because it compiled every listed crate, which a CUDA crate prevents.

### A sandboxed build service

The next plan was a server that fetched `name@version`, built it without network access in a sandbox with limits on time, memory, and disk, and kept the catalogue forever. It was dropped the same day, before it was built. A node crate cannot be expected to build in a service, and a sandbox still runs the crate's code.

### A hand-written nodes file

Next, a node crate would ship its declarations in graph syntax, such as `sink Engine: Words -> stream Token;`, named by `[package.metadata.gdl] nodes`. `#[gdl::node]` on the type read the file at compile time and failed the build on a mismatch. A separate interface crate per node crate was rejected, since it means two published crates per node crate. This was built, then dropped. A file written by hand must be edited at every change to the code, and the attribute only reports when the two disagree.

### A generated `catalog.toml` shipped in the crate

Next, `cargo gdl extract` would build the crate on the author's machine, where CUDA is, and write `gdl/catalog.toml` into the published package, kept current by `cargo gdl extract --check` in CI. It was dropped before it shipped. The composer must read any crate version a user picks, so crates ship nothing for gdl, and the code is the only source.

### `#[gdl::node]` on the role impl

The marker briefly went on the role trait impl, where the role and port types are visible. It moved to `#[derive(gdl::Node)]` on the type, so that messages and nodes are marked the same way, on the type that a graph and the generated segments name. A derive cannot see impls, so it probes which role trait the type implements, with the same autoref selection as the wire probe. The reader finds the impl in source.

## Two crates for every marker

`derive(Message)` lives in `gdl-message`, which depends on `linkme`, `serde`, `toml`, and the macros. A protocols crate depends on it alone, without the runtime. A proc macro has no `$crate`, so an expansion must name one crate's paths. Each marker therefore has two entry points in `gdl-macros`: one expands to `::gdl_message`, the other to `::gdl::__private`. Reading the caller's manifest at expansion (`proc-macro-crate`) would let one entry point follow either crate, at the cost of a TOML parser in every node crate's build and an expansion that depends on the manifest.

## Origin identity

A message is identified by its origin: the defining package's name, version, and source, and its definition path.

- **By definition, not by path.** A type that one crate re-exports from another is one message. Listing both crates is never ambiguous.
- **By source, not by build directory.** The catalogue program told packages apart by `CARGO_MANIFEST_DIR`. A reader has no build directory. The source (a registry, a Git commit, or `path+file://<dir>`) tells packages of one name and version apart in the same way.
- **Version included.** Ports connect on equal origin, as binding compares. Cargo would unify semver-compatible versions of one package into one crate, so this is stricter than the build. Whether to connect across compatible versions is still open.
- **Search order never decides.** A name that two listed crates offer as two messages is an error until a `pin` chooses. A type added later to an earlier crate would otherwise rebind the graph without a word.
- **The contract hash covers the path.** The generated contract writes each message as a rooted path through a listed crate. A name that resolves elsewhere changes the hash. The package version is not in the hash. A version that changes a wire form needs a `schema` bump, as any message change does.

An instantiation of a generic message is identified by its type, as `core::any::type_name` writes it, plus the package of each argument. The rejected option was the alias's own path. Then a node crate's port would match only an alias that the reader had found in some crate it read. Two aliases of one type would also be two messages. By type, each crate is read on its own, and two aliases are one message under two names. `type_name` is best effort. A change in its form shows as a `--verify` difference, never a wrong graph, since generation uses the reader's identity. A test in `gdl-extract` (`tests/spelling.rs`) compiles one crate and reads it, and holds the two spellings equal for a table of instantiations: nested, with `()`, and with each standard type the reader names. A compiler that writes one of them another way fails that test first.

## The wire check fails closed, with a cap

For an instantiation of a generic message, the reader must judge what serde's derive would bound, or what a hand-written impl bounds. Each round of review found more rules of serde or rustc that the reader did not model: a hand impl's bound on `Copy` or `Sync`, a `Default` impl's own bounds, a container `#[serde(default)]`, a field of a parameter's associated type. Each made the reader say "wire" where the build's probe says "local".

The two errors cost different amounts. A wrong "local" refuses a crossing that would work, and `--verify` reports it. A wrong "wire" lets a crossing through that fails to build. So the reader judges an allowlist and fails closed. It accepts serde's traits, `Default`, `Sized`, `?Sized`, and lifetimes as bounds, a bound on a parameter alone, the serde attributes it reads, and serde's and `Default`'s traits only where it can follow their paths to serde or the standard library. Anything else makes the instantiation local. Serde's directions are judged one at a time, and an argument is a wire type if either direction needs one.

The reader tells each item of a crate it does not read by one table, in `gdl-extract`'s `external` module. A row holds the item's name, the crates a path to it may start from, and whether the prelude brings it. The items are serde's traits, `Default`, `Sized`, the standard types the reader names, and gdl's export marker, role traits, `Streamed`, and `PoolPolicy`. Each check reads a path against that table, with its own rule. A bound or an impl's trait must lead to the item's crate, or be the prelude's bare name. A role trait needs only its name, through a rename. The export marker must be `<crate>::export`, or a `use` of exactly that.

After seven rounds, most new findings were deliberate name tricks: a local `mod serde`, a local `trait Default`, a trait named like serde's, a dependency renamed `serde`. The bar is capped there. Crashes, and a wrong "wire" for code that real crates write, are always fixed. A name trick is documented in [Known limitations](../operations/limitations.md), not chased. The checks for a local `mod serde`, a local `trait Default`, and a trait of serde's name were already built, and they stay. Sizedness is judged by name where the reader cannot name a struct's last field: only `str`, `Path`, `OsStr`, and `CStr` count as unsized.

The cap is safe because of a backstop in every build. Each generated segment asserts at compile time that every message it sends or receives across a cut is a wire type: `gdl::__private::crosses`, one assertion per message type per segment, named by the crossing. A trick that fools the reader fails that segment's build with one error that names the message and the crossing, never a running graph.

## The probe records the build's reading

`cargo gdl extract --probe` exists for the wrong "local" that the cap accepts. It builds the program that `--verify` builds, and records the build's reading of each instantiation in the catalogue entry.

- **Opt-in.** A probe runs the crate's build scripts and proc macros, as `--verify` does. The composer's server must run none of a crate's code, so it never probes, and no add probes by default.
- **On the export.** The build's reading is a field of the instantiation's export entry, beside the reader's. The composer lends its checks the offers that its entries hold, without a copy, so data kept beside the offer would not reach resolution. Resolution takes the reading of the entry whose path the contract names, the build's where it holds one. Taking any listed crate's probe of the same type was tried and dropped. A facade's probe of a directory crate's type then outranked that crate's newer reading after an edit. Two facades that enable different features of one published crate also read its type differently, and the first's probe outranked the second's. A build's reading holds only for the build that made it.
- **A directory crate is built at each probe.** A hash of the crate's files was the rejected alternative. A path dependency, the lock, or a feature can change an instantiation's wire capability without a change to the crate's own files. Cargo already knows what changed, and a probe with nothing to rebuild costs a fraction of a second. An add without `--probe` drops the data, since it may describe code that is gone.
- **A published version is probed once.** It never changes, so a held entry with the build's reading of each instantiation needs no build. The probe fills in a held entry that has none, by a rename, and keeps the rest of it as first written. It fills the entry as the file holds it, not as this catalogue loaded it, so a fill that another probe made since stays.

## The reader is linear in its source

The reader once grew with the product of two lengths in a file, not their sum. Each name a `use` bound copied the item's attributes and the path before its braces. Each module copied its parent's path. Each name's location re-tokenized its whole item, because syn's `span` builds the item's tokens. The wire check of each message, and the role check of each node, walked and resolved every impl in the crate.

Now a `use` path and a module directory are each a chain of links that share what comes before them. A location is computed once, and shares its file's path. The crate's serde and role impls are resolved once and indexed by the type they are for. A module indexes its types and bound names. The export check runs once per `use` item. `crates/gdl-extract/tests/cost.rs` holds each shape at two sizes. [Source reader cost](../operations/reader-cost.md) has the numbers.

Two costs remain. A resolution repeats, per query, the chain it follows, since the resolver keeps no answers between queries: N resolutions through a K-segment path cost N times K. Output whose size is itself a product, such as a full path for each message of a deep chain, stays a product. One extraction reads at most 16 MiB of source. At about 120 times a source byte, that bounds what the reader stores near 2 GB. The composer's limits on its import child bound the rest.

## The composer's server

The composer applies the generator's rules, and only the server decides.

- **One copy of each rule.** The rules live in `gdl-syntax` and `gdl-model`, which build for wasm, and a test keeps that build working. The page does not run them. `wasm-bindgen-cli` was not available offline, and the server can answer from the same rules `cargo gdl` uses.
- **The page decides nothing.** It offers only nodes whose port carries the arriving message, which is a lookup. It sends a draft as JSON. The server writes the graph file and the spec, so one writer decides what is generated, and the user sees the exported text.
- **Only an import fetches.** Check and export take the loaded catalogue's offers (`gdl_gen::Offers::Given`) and read nothing. A crate read with problems adds no entry, and its problems are the answer.
- **No index without a caller.** The catalogue once built an origin-to-port index and query methods that nothing called. They were removed. A query is added when a caller needs one.
- **Entries.** A published version is written once, by a hard link that never replaces a file, so the first writer's entry stands. A directory is one package that changes as it is edited, so its entry is replaced by a rename. The checksum is the one Cargo locked, and nothing is invented. The caller passes the time, so tests stay deterministic.

The server also faces input it did not write:

- **Host and Origin.** A DNS name that an attacker points at the user's machine would make the attacker's page same-origin with the server. So the server answers only to `localhost`, IP addresses, and `--host` names. A browser sends `Origin` with every cross-origin `POST`, including simple ones without a preflight, and `Sec-Fetch-Site` with every request. A state-changing request from another origin is refused.
- **An import runs in a child.** A crate made to exhaust the parser must end the child, not the server. The child has 4 GiB of address space (`ulimit -v`, not enforced on macOS), 600 s of CPU, a 360 s deadline, 300 s for cargo, and 2 MiB of output. It runs in its own process group, which is ended with it.
- **Cargo and Git are confined.** Git runs over HTTP(S) only, without the user's configuration or credential helpers, and never prompts. A Git import reads `Cargo.toml` from the fetched objects, not a checkout, so a symlink is refused by its mode and a size limit of 1 MiB applies before reading. Local imports stay under `--local-root`, with symlinks resolved.
- **Applying is opt-in.** Applying builds images from the graph's crates and publishes host ports. A server listening beyond loopback would offer that to anyone who reaches it, so it needs `--docker`. The server regenerates the bundle from the draft and never takes files back from the page. It writes the bundle beside the target and moves it in, so a failed generation keeps the last one. It never removes a directory without its `.gdl-deployment` marker. It does not pass `--wait`, because the segments' health check allows a start period of 30 minutes.

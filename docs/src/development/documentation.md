# Documentation

This book is the documentation of dynamo-gdl. The source is in `docs/src/`.

## Where text goes

| Text | Place |
|---|---|
| How a subsystem works, and why | A chapter in this book |
| Measured numbers, with their setup | A chapter under Operations |
| Rejected designs and negative results | A chapter under Development |
| A limit that a user can run into, its cause, and what lifts it | [Known limitations](../operations/limitations.md) |
| The reason for a line of code | A short code comment |
| A hard lesson ("X breaks when Y") | A test. Its doc comment can be long. |
| Plans, remaining work, and follow-ups | The work tracker, not this repository |
| Notes while work is in progress | `agent-docs/`, which git ignores |

## Rules

- **`agent-docs/` is local scratch.** Git ignores it, and nothing in the build or the tests reads it. Before a change is done, move the lasting parts into this book and put the remaining work in the work tracker.
- **Keep only what lasts.** Write how it works, why, the numbers with their setup, and the results that did not work. Do not copy plans, milestone labels, ruling numbers, PR numbers, or handoff notes. History is in git. The test `crates/gdl-gen/tests/references.rs` fails on a reference to `agent-docs/`, a ruling, a milestone, a review, or a PR number in the code or the book.
- **Write in simple English.** Use the `simple-english` skill for the book, README files, and comments.
- **Keep comments short.** A comment says why, or which fault the code avoids. It does not repeat the code, and it does not point to a plan.
- **Update the book in the same change.** Each change to behavior updates the chapters that describe it, the top-level `README.md`, and the known limitations.

## Build the book locally

Install the tools at the versions that `scripts/build-book.sh` pins:

```bash
cargo install --locked mdbook --version 0.4.52
cargo install --locked mdbook-mermaid --version 0.16.2
cargo install --locked mdbook-linkcheck --version 0.7.7
```

Or let the script put them in `.book-tools/`. It downloads release binaries on x86_64 Linux and builds the tools elsewhere:

```bash
INSTALL_TOOLS=1 bash scripts/build-book.sh
```

Build the book and check its links:

```bash
bash scripts/build-book.sh
```

A link to a missing page fails the build, and so does a link to a missing heading (`page.md#heading`). `mdbook-linkcheck` 0.7.7 does not check the part after `#`, so the script then runs `scripts/book/anchors.py`, which reads the built HTML and checks each heading link against the ids of the page it resolves to. It skips the links of the print page, which joins every chapter, and the heading links that mdBook writes on the 404 page. It needs `python3`. Its tests run with `python3 -m unittest discover -s scripts/book/tests`. CI runs the same script, the heading check's tests, and the reference test, on each pull request and on each push to `main` (`.github/workflows/ci.yml`). Each push to `main` also publishes the book to the repository's private GitHub Pages site (`.github/workflows/pages.yml`). To preview while you edit, serve the book:

```bash
mdbook serve docs --open
```

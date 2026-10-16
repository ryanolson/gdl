# Whole-repository mega review, pass 7

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `d99e711`. Raw text: `mega-r7-raw.txt`. ACTIONABLE: 1.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P2 | A binding whose library is named like a keyword (`async`) is refused when its type is written `r#async::Engine`, and the unescaped `async::Engine` passes validation and generates invalid Rust | valid | Cargo builds a dependency named `async` that a user crate reaches as `r#async::Engine` (checked in a scratch workspace). `a_keyword_library_takes_a_raw_type_path` (gdl-gen unit test). Fix: the binding's type is parsed as a Rust path (`syn`), and its first segment, without `r#`, must be the library's name. A type that is not a path, such as `async::D`, is refused at generation. The type is emitted as written, so the raw prefix stays |

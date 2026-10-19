# Whole-repository mega review, pass 5

Reviewer: fresh Codex (`gpt-6-astra`) session, read-only, `thermo-nuclear-code-quality-review` over the whole repository at `6ec06a7`. Raw text: `mega-r5-raw.txt`. ACTIONABLE: 2.

| # | Severity | Finding | Ruling | Evidence and fix |
|---|---|---|---|---|
| 1 | P2 | A caller's trait with a `from(&str)` function, implemented for `String`, makes the expansion's `String::from` ambiguous | valid | `tests/pass/graph-caller-traits.rs` (E0034). Fix: `<::std::string::String as ::core::convert::From<&::core::primitive::str>>::from`. The first form of the fix wrote `&str`, which `graph-shadowed-prelude.rs` caught (its caller defines `str`) |
| 2 | P3 | The stop-forwarding layer in `inbound_items` is unreachable: both callers pass `None` | valid | Both calls pass `None`, and stop already reaches the producer through the watcher (outer end) and the pump's upstream canceller (backward runs). Fix: `inbound_items` is a plain frame-to-item adapter; `StopSource` and its two impls are gone. Behavior is unchanged; the full suite passes |

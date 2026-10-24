# Config files refuse what they do not know and say where: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-23). Target: `976aedd..6c5dcff`, config hardening for the owner's scope of 2026-10-23:
- **Where:** every problem of the spec, the fleet file, and a cell's settings prints as `<file>[:<line>]: [<table>] <key>: <message>`, or `<VARIABLE>: <message>` (`gdl_model::Problem`, `46fae45`, `dff1f34`, `6c5dcff`).
- **Unknown keys:** refused where they were not, namely `[segment.<name>]`, the Git entries of `[messages] crates` and `[nodes] crates`, and `[routing]`'s selector table, which refused a second key without naming it.
- **`seeds`:** removed and refused, since nothing read it (`702051a`).
- **`home` is the parent cell:** required for a cell of a class that is sent to, it must name a cell of a class that sends to the cell's class, and it is refused on a cell of a top class (`371fef8`).
- **The check command:** `cargo gdl check --spec [--fleet]`, and `compose --fleet`, report every problem through one function (`68d2eee`).
- **Defaults:** every default is a named constant in one README table, with round-trip tests (`55e6add`).

Verdict: REJECT, four major and four minor findings. Scores: correctness 6, tests 6, fit 6, code quality 7.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | A `[routing]` or `[cells.<class>]` entry that spells only defaults was dropped before its name was checked, so an unknown node or class passed `check` | valid | `a_spelled_default_for_a_missing_name_is_refused`; `check` exited 0 | a placement keeps the names it read with only defaults; they take no part in equality, the hash, or what is written, but are checked (`caa9522`) |
| 2 | major | `check` accepted an unknown `[deployment.segment.<name>]`, checked only when Compose generated | valid | `check` exited 0 | the check moved into the spec's validation, which `check` and `compose` both run (`1e80624`) |
| 3 | major | A bad `[gdl] path` returned before binding and validation, hiding the spec's other problems | valid | only the path's problem was reported | the `[gdl]` problem is kept and validation still runs (`ed43c31`) |
| 4 | major | A fleet checked without a layout skipped checks that need none, such as counts of 0 | valid | 3 problems reported of 5 | counts of 0 are refused without a layout (`599d769`) |
| 5 | minor | A cell's diagnostics named a variable that was not set, or no origin at all | valid | `GDL_CELL__READY='{d="many"}'` was blamed on `GDL_CELL__READY__D` | the variable actually set is named; a missing key is named by its source where all others share it (`d1bacd2`) |
| 6 | minor | Syntax errors had no table or key, and a bad `[gdl] path` did not name itself | valid | `line 2: unexpected key or value` | the table and key are taken from the text around the error (`ca71e8d`) |
| 7 | minor | The spec could not be written back, so it had no round trip | valid | the round-trip test did not compile | the spec's file form is written, and reads back equal, with the same placement and hash (`12bb465`) |
| 8 | minor | Two defaults had no named constant | valid | the defaults test required a constant per row | `RequestRouter::DEFAULT` and `DirectoryBackend::DEFAULT` (`bcee7f6`) |

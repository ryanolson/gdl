# A fleet deploys by cell: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `2947273..2bfe4e9`. `cargo gdl compose --spec --fleet` writes one Docker Compose bundle that deploys a whole fleet on one host:

- **Hubs:** one per cell, named for the cell, with a fixed `container_name` so that Compose refuses to scale it. A cell's record has one writer. Its settings come from `GDL_CELL__*` as `Fleet::cell` fills them in, with `DIRECTORY=dir`, a `DIR` on one `cells` volume shared by every hub, and `HUB=<cell>:1337`. Its health check is its own liveness.
- **Segments:** each cell's segments of its class in its layout, configured as that cell, each depending only on its own cell's hub.
- **Images:** built once per layout and segment, and shared by the cells of that layout and class.
- **Specs without classes:** `compose` keeps today's bundle byte for byte (pinned), and refuses a classed or multi-layout spec without `--fleet` (`29260dd`).
- **Support** (`df0736b`, `8916f9d`, `2bfe4e9`): `Fleet::cells()`, and `gdl_hub::cell_from_vars`, so that a test reads a hub's variables exactly as the hub does.

Verdict: REJECT, two major and one minor finding. Scores: correctness 7, tests 7, fit 8, code quality 8.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | A quorum key containing `__` became `GDL_CELL__READY__ENGINE__WORK`, which the readers split as another level, so the hub could not start | valid | `a_quorum_of_a_segment_whose_name_holds_a_separator_reads_back`: `found map, expected u32` | a table with such a key is written as one variable holding the whole table (`3c6a7a1`) |
| 2 | major | A string that the settings parser reads as another type, such as a cell named `true`, was written bare and refused by the hub | valid | `a_name_that_reads_as_another_type_reads_back_as_written`: `found bool true, expected a string` | a string is written bare only if the parser reads it back as itself, otherwise quoted, for `GDL_CELL__*` and `GDL_HUB__HOST` (`3c6a7a1`) |
| 3 | minor | Any port list was refused for a segment that runs in several cells, though ports without a fixed host port do not conflict | valid | `a_segment_that_publishes_only_dynamic_host_ports_runs_in_several_cells` was refused | only an entry that names a fixed host port is refused; a form the reader does not recognise still counts as fixed (`8bfffbe`) |

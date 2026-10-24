# One graph, several layouts: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `f77bc26..c4804af`, the owner's scope of 2026-10-23 (one graph may be split differently in different regions, each split a layout with its own placement hash):

- **Reading** (`df6a821`): a spec holds its layouts as `[layout.<name>]` tables, each holding exactly what a placement holds today (`.segment.*`, `.routing`, `.class.*`, `.cells.*`). So each is read by the placement's reader and hashes as today's placement of those tables. A spec without layout tables has one layout, named `default`, in today's form and with today's hash.
- **Validation and generation** (`da7d334`):
  - Each layout is validated as a complete graph and located as every other problem is, naming its layout.
  - `generate` writes each layout's segments to `<out>/<layout>/<segment>` when there are several, with packages named `<graph>-<layout>-<segment>`; with one layout the output is unchanged.
  - `compose`, `export`, and `diagram` refuse a spec with several layouts.
- **Cells and fleets** (`c4804af`): a cell's settings and the fleet file name the cell's layout; the fleet check refuses an unknown layout, a class not in the cell's layout, and a `home` in another layout.

A chain of three classes needs two roots: the boundary rule forbids a class crossing nested under another. So the README's three-class example uses a two-root graph.

Verdict: REJECT, one major and one minor finding. Scores: correctness 7, tests 7, fit 8, code quality 8. The reviewer compiled generated crates for three segments in one layout and two in another.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | The composer's draft ignored an unknown `layout` field, so `/api/export` answered 200 with only the top-level segments: the layouts were dropped silently, and the refusal checked a spec that could never hold them | valid | `a_draft_with_layouts_is_refused_not_dropped`, through `/api/check`, `/api/export`, and `/api/deploy`: `/api/check` answered 200 | `Draft` refuses unknown fields (422, naming the field), after checking that every key the page sends is one of its fields; the unreachable guard is removed (`ca855e3`) |
| 2 | minor | `diagram` reported a layout's placement errors without the layout or table | valid | `diagram_locates_a_layouts_problems`: no table was named | `diagram` reports through the layout's located problems, as `check` does (`8deb9f7`) |

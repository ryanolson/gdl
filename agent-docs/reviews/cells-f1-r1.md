# Every parent cell has a child of each class it sends to: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `ac36f63..1b6a090`, the owner's scope of 2026-10-23 (every parent cell has a complete set of children in its layout):

- **The fleet check** (`4140d13`): for every cell whose class sends to another class in its layout, at least one cell of that class must have it as `home`, in the same layout. Each missing class is one problem, located at `[cell.<parent>]` and naming the layout, from `cargo gdl check --spec --fleet` and `compose --fleet`. With three classes over two roots, the rule holds at every level.
- **A parent's health** (`1b6a090`):
  - `requires` defaults to one ready child of each class the cell's class sends to (`CellSettings::DEFAULT_REQUIRES`), counted among the cell's own children.
  - An explicit `requires` replaces the default whole; a count of 0 and a class not sent to stay refused.
  - The health body names the classes not met in an optional `missing` field, and `healthy()` still judges by the counts, for a body from an older hub.

Decisions the coordinator accepted before review:
- **Where the note is printed:** when a fleet's layouts are unknown, the note that its children are not judged is printed by `check` and `compose`, not raised as a `FleetError`, so that a sound fleet checked without a layout returns none.
- **An explicit `requires` equal to the default** compares equal through `requirements(&layout)`, since settings are read without a layout. A cell therefore cannot require nothing; a parent cell must have its children.

Verdict: APPROVE, no findings. The full workspace suite passed independently at `4140d13` and at `1b6a090`, which closes the implementation's note that `4140d13` alone had not had a full run. Fmt, clippy, the composer's 74 editor tests, and `examples/http`, `examples/api`, and `examples/film` passed. Disabling fleet completeness, and disabling the default `requires`, each fails a test. The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 10, tests 9, fit 10, code quality 9.

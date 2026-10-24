# One graph, several layouts: review round 4, the merge

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `889e4e8..0ebfa4a`, the merge of `main` at `cbe75b5`, which brings a chooser offering to its own cell's children by round robin and a placement hash that covers each class's effective cell policy. Verdict: APPROVE, no findings.

- **Conflicts:** `README.md` and `LIMITATIONS.md` keep the layouts' text, describe following by parent and child, and drop the stale claim that `home` is unused.
- **Hashes:** a layout's hash follows the new rule. A layout that leaves the policy unsaid and one that spells `round_robin` hash alike, and one naming `home_first` differs. A named layout and the same tables at the top level hash alike, and so do identical layouts. The pinned hashes of placements without classes are unchanged.
- **Keys:** `spill_rtt_ms` and `max_near` are refused in a layout's `[cells.<class>]`, located at that table and key.

Fmt, clippy, the workspace suite (1,141; one known `retire` flake passed on its own), the composer's 74 editor tests, `examples/http`, `examples/api`, `examples/film`, and `generate --check` for chat and asr passed. The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 9, tests 9, fit 9, code quality 9.

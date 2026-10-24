# A fleet deploys by cell: review round 4, the merge

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `62e4987..4418679`, the merge of `main` at `9b1c3e7`, which brings cross-cell retirement acknowledgements. Verdict: APPROVE, no findings.

- **The conflict:** `LIMITATIONS.md` keeps both sides' lines of one entry.
- **Hub addresses:** a parent hub sends the release and the new acknowledgement to the child record's `hub`, which in the bundle is `<cell>:1337` on the bundle's network.
- **Stop grace:** an entry instance's longer retirement waits inside the drain's deadline, so the generated stop grace (the drain bound plus 30 s) still covers it.

Fmt, clippy, the workspace suite (1,207, 7 ignored), the composer's 74 editor tests, `examples/http`, `examples/api`, and `examples/film`, and `generate --check` for chat and asr passed. No bundle has been run under Docker; the suite checks the generated files. The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 9, tests 9, fit 9, code quality 9.

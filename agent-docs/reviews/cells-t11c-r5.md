# An entry instance's retirement waits for its parent cell: review round 5, the merge

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `57b9b38..6bc5513`, the merge of `main` at `2947273`, which brings several layouts and fleet completeness, with a parent's `requires` defaulting to a ready child of each class it sends to. Verdict: APPROVE, no findings.

- **Conflicts:** the two paragraphs in `README.md` and `LIMITATIONS.md` keep both sides' edits exactly.
- **The fixture:** it now leaves `requires` unsaid, since an explicit requirement of 0 is refused.
- **A parent whose health is 503:** with its own quorum met and no ready child, it still followed, routed 12 requests through two choosers, and acknowledged a retirement in 3.7 ms. The retiree left after 1.12 s, inside its 3 s bound, and no later offer reached it.

Fmt, clippy, the workspace suite (1,186, 7 ignored), the composer's 74 editor tests, `examples/http`, `examples/api`, and `examples/film`, and `generate --check` for chat and asr passed. The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 9, tests 9, fit 9, code quality 9.

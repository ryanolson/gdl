# `home_first` by load, spill on room, and damping: review round 3

Reviewer: the same reviewer, read-only (2026-10-23). Target: `254b515..7083fe8`. Verdict: APPROVE, no findings.

Checked and fine: round 2's finding is fixed: one immutable damping view per choice serves both the cap and the draw, and the compare-and-swap update keeps competing writes; both stress regressions and the deterministic one passed 5 of 5 runs; two more probes passed (concurrent updates across 512 cells keep state and earlier views; simultaneous declines and acceptances write once). The workspace suite passed (940, 7 ignored). On cost, a boundary pick measured 73 to 74 ns in release (24 ns of it the clock read) and a pick with its acceptance 89 ns, in runs not interleaved with round 1's; the 1%-in-instructions gate is not yet verified on this machine (`kernel.perf_event_paranoid = 4`, no valgrind).

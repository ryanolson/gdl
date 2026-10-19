# Milestones 10 to 18, mega review pass 5

Reviewer: a fresh Fable subagent, read-only, running the thermo-nuclear code-quality review over `main..HEAD` (73 commits, 233 files). It checked the fixes of `mega-r4.md` first, by probe and by mutation, then read the whole diff fresh. In a scratch copy it ran:
- the workspace suite (46 targets, all green), clippy, and fmt;
- `generate --check` of both examples;
- a Node probe of the page's script.

Verdict: REJECT, 2 actionable, both minor.

Pass 4's four fixes held, and the two with tests failed when mutated. The reviewer also traced the reader's in-progress memoisation and found that a query cannot wrongly come back empty.

| # | Finding | Ruling | Test | Fix |
|---|---|---|---|---|
| R5-1 | the page kept an inner end that no slot would offer after a forward node was removed, and a whole hop after the source's request changed (the server then refused the draft) | valid | none in the suite (ruling 85); a Node probe of `prune` with a stubbed page passes after the fix | `prune` drops, before each render, any chosen node whose port no longer carries the arriving message, and everything after it |
| R5-2 | `Names::new` listed crates by two rules in two passes | valid (structure) | the draft tests, unchanged | one pass per message finds the crate that offers it at a public path (its own definition or export, else another crate's export), lists that crate, and records the path |

The workspace suite, clippy, and fmt pass after the fixes.

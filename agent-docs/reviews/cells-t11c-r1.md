# An entry instance's retirement waits for its parent cell: review round 1

Reviewer: an independent reviewer, read-only, with the `thermo-nuclear-code-quality-review` and `rust-code-review` skills, in `/tmp` copies with their own target directories (2026-10-24). Target: `cbe75b5..5370078`, cells 5's "Instance retirement" across a class boundary, under the owner's scope, where a model cell's only follower is its parent:

- **The model hub** (`23cb9a7`): `retired` gains an optional `acknowledged` field, sent again once every cell that followed the model cell at the retirement has acknowledged or released it. With no follower, the first `retired` says so. A watch lost without a release never counts, a watch replaced under the same follow keeps its key, and a follow that starts after the retirement is not waited for.
- **The parent hub** (`284233d`):
  - A chooser acknowledges on `POST instances/{id}/avoids`, authenticated as the instance.
  - The relay waits for the declared chooser instances whose hops enter the retiree's segment, as they were when the retirement was applied. A chooser that leaves or registers again counts as acknowledged.
  - It then tells the model hub on `POST /v1/ext/gdl-cell/avoided`, which names the follow as the release does.
- **The chooser** (`c92d59f`): it acknowledges once its published tables hold the instance retired, from a change, a relay snapshot, or a cell read from its own watch. A refused acknowledgement is tried again (`319d9ba`). A chooser that declares after the retirement counts as acknowledged at once.
- **The retiree** (`b87d339`, `5370078`):
  - **Before:** an entry instance in a cell did not wait at all across the boundary: its senders and sources are of another class, with no member on its hub.
  - **Wait (A)** now also needs its parent's acknowledgement, bounded by the setup timeout plus twice `summary_ms` from the retirement, and is then followed by the setup timeout.
  - **Wait (B)** never holds while a source of another class reaches it.

Departures the coordinator accepted before review:
- **The acknowledgement's listener:** the parent hub's acknowledgement goes to the model hub's public listener, beside the release. A cell's record carries only that address, and the route is unauthenticated, as the release is. A forged acknowledgement could end a retiree's wait early. The harm is bounded: a chooser that still picks the retiree is refused, and offers the request to another child. `LIMITATIONS.md` says so.
- **One test changed:** `cells::a_model_cell_serves_with_no_global_instance` gives its instances a short setup timeout, since an entry instance now waits at least that long when it retires.

Verdict: REJECT, three major findings. Scores: correctness 7, tests 7, fit 7, code quality 7.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | Learning of a cell drain counted as the parent's acknowledgement, and the drain's rules ran only before the retirement wait, so a drain that arrived during the wait ended it early | valid | `a_drain_while_the_retiree_waits_keeps_it_to_the_drains_bound`, once it waited for the retiree to withdraw `loaded`: intake stopped 1.20 s into a 3 s drain bound | the wait ends only on an acknowledgement, on the hub saying no cell follows the draining cell, or once the bound has passed since both the retirement and the drain (`b8d34d1`) |
| 2 | major | A retiree that left kept its waiting state in the parent's relay for as long as the follow lived | valid | `a_retiree_that_leaves_is_waited_for_no_more`: `retirements()` still held it | the relay forgets a retirement when it sends that instance's `left`, under the same lock (`6bf2bb2`) |
| 3 | major | One retirement acknowledgement ran the not-offered bookkeeping over every follow, twice | valid | `a_retirement_is_acknowledged_without_a_pass_over_the_follows`: 10 visits with 4 other cells | retirement acknowledgements have their own timer and chore, which read only their queue: 0 visits at 4 and at 64 cells (`6b8fd39`) |

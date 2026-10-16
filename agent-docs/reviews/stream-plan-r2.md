# Streaming requests plan review, round 2

Reviewer: a fresh Codex (`gpt-6-astra`, high) worker, read-only, one bb workflow (`wfr_e4afaeeb`). Subject: the round 1 fix (an in-band `WireItem::Stop` on input anchors, ruling 26 addendum) and the whole streaming design at `43e330f`. Verdict: REJECT, two findings. Round 1's finding is only partly resolved. Raw text: `stream-plan-r2-raw.txt`.

| # | Severity | Finding | Ruling | Disposition |
|---|---|---|---|---|
| 1 | major | An in-band `Stop` is data. It waits behind exhausted credit, and a relay whose stream waits for stop before it reads its input never sees it. The local run finishes; the remote run waits until the deadline | valid | The in-band frame is removed. Stop reaches the inner end on the response side; each receiving run whose context is stopping calls velo `request_stop` on its input anchor, a control message (`_stream_stop`, or a mux lifecycle stop on the ticket path; `velo-0.19.0/src/streaming/control/registry.rs:177`) that data credit does not hold back. The sender's pump already turns its consumer's stop into a stop of its own context and of the anchor it reads. Ruling 26 addendum (round 2) |
| 2 | major | After the input ends, and for a relay that turns a value into a stream, no input pump is left to send a `Stop` | valid | Same fix: the stop route runs from each receiver back to its sender's output pump, which exists while the relay produces output, whatever its input |

The reviewer also confirmed that a duplicate stop is idempotent, that velo keeps a stop requested before attach, and that nested child scopes pass a received stop on. It ran the existing stop fault tests (4 pass).

What remains unreached, as for a cancel (7.4): a forward node that waits for stop inside `forward`, before the hand-off. The deadline bounds it.

Tests added to milestone 8: stop reaches a relay whose stream waits for stop before reading under exhausted credit (TCP and same-worker), a relay whose input has ended and that adds a last item on stop, and a relay that turned a value into a stream.

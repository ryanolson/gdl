# Streaming requests plan review, round 3 (confirmation)

Reviewer: a fresh Codex (`gpt-6-astra`, high) worker, read-only, one bb workflow (`wfr_ce555b1a`). Subject: the round 2 fix (stop up the request path through velo `request_stop`, ruling 26 addendum) and the whole streaming design at `2b1a650`. Verdict: APPROVE. Actionable: 0. Raw text: `stream-plan-r3-raw.txt`.

- **Rounds 1 and 2 are resolved.** velo 0.19 keeps a stop requested before attach. It delivers the stop to the sender's `stop_token` on a same-worker attach, a remote attach, and a ticket open. The control messages do not depend on data credit (`streaming/control/registry.rs:177`; `streaming/anchor.rs:454,1525,1714,2537`; `messenger_mux/peer_batcher/mod.rs:663`). A stop leaves sending enabled.
- **The relay route holds.** A relay's output pump observes stop while it waits for input or for a blocked send. It then stops its own context and the anchor it reads (`dist/hop.rs:754,800,845`). The route still works when the relay's stream waits for stop before it reads, when the relay's input has ended, and when a forward node turned a value into a stream. A forward node that waits for stop inside `forward` is the accepted limitation.
- **Implementation note, carried into milestone 8.** When the receiver drops its input, the input pump's cancel must not cancel the response (ruling 24). The response pump's cancel cannot be reused unchanged.
- **Validation.** Four gdl stop fault tests and three velo stop and lifecycle tests pass.

The plan review is complete after three rounds: three findings (1, 2), all valid, all fixed in the plan.

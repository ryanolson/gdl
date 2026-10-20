# Milestones 20 and 21, a segment's lifecycle: plan review round 1

Reviewer: a fresh Opus subagent, read-only. It reviewed the first draft of the lifecycle addendum (one milestone, rulings 104 to 108). Partway through, the owner's ruling on joining arrived and the reviewer took it in. It read the process module, the routing table, the hub session, the roster directory, the admission ledger, the JSON-lines entry, the generated binary and Compose file, the multi-process chat harness, and velo and velo-hub at `1bbeb88`. Verdict: APPROVE WITH CHANGES, 6 must change, 7 should change, 8 nits. Every finding is taken into the rewritten addendum, which splits the work into milestones 20 and 21.

| # | Finding | Taken as |
|---|---|---|
| M1 | the generated body owns the segment and drops it when it returns, so a drain after the body has nothing serving; a source that is also routed to must keep serving other sources' requests | ruling 107: `Launch::serve` keeps the segment until the drain ends, and exit needs the body returned and the conditions held |
| M2 | an acknowledgement is not binding: once the others retire too, a peer falls back to an instance that acknowledged and left | ruling 112: an acknowledgement is a promise, and the fallback takes only instances not acknowledged |
| M3 | under a hub rosters differ, and a peer or joiner that took a roster after X loaded is not in X's roster; X would exit under it | rulings 107 and 112: the conditions read the hub's current membership; a retiring instance withdraws `loaded` and `ready` first |
| M4 | velo registers peers only through `register_peer`; a joiner, or an instance a request is never routed to but whose anchor an inner end attaches to, fails with "peer not registered" | ruling 110: every member registered, velo built with the hub's discovery, the gap recorded in `velo-issues.md` |
| M5 | a joiner's barrier fails when an instance in its roster retires and leaves; retired instances counted toward `[min_replicas]` | ruling 111: the barrier follows live membership, retired instances not counted; 97a amended |
| M6 | an instance retired alone exits on an empty ledger while velo still holds its finished output; the consumer sees the sender dropped | ruling 113: open for the owner (a velo signal, or a consumer's acknowledgement in gdl); milestone 20 does not depend on it, since its condition is that the sources have drained |
| S1 | polling the hub every second is O(N²) at the hub and slow | ruling 110: velo-hub's `watch_instances` |
| S2 | a runtime default port collides where instances share a host, and port 0 cannot be checked | ruling 104: the default lives in the generated Compose config; the bound address is printed; `--healthcheck` dials loopback and refuses port 0 |
| S3 | signals registered only in `serve`; no `left` on a stop during loading or on an error exit | ruling 106 |
| S4 | the early-request and never-starting tests across processes could not prove what they claimed under a roster directory | ruling 109: they assert what happens (stdin buffered by the rendezvous; the rendezvous error); the held roster is proved in process and by milestone 21's joiner |
| S5 | missing tests: two retirements in turn, a joiner during a retirement, a joining source, a returning placement, a slow consumer, a crashed peer, a lease lost while draining; timing by sleeps | rulings 109 and 115; a gate instead of `delay_ms`, the first item as the signal |
| S6 | unregistering a peer from velo on `left` breaks one that lost its lease while it drains | ruling 110: dropped from routing at once, unregistered after the bound |
| S7 | a request with its own deadline longer than the default outlives the bound | ruling 107: the bound is the drain's, stated |
| N1 | the setup-timeout window is not needed after the sources drained | ruling 112: only after the acknowledgements |
| N2 | the health body `serving` would differ from the hub phase `serving` | ruling 104: the body says `ready` |
| N3 | the hot path: `load`, not `load_full`; an eligible list per snapshot; one table for both pickers; poll the ledger | rulings 107 and 114 |
| N4 | where `drain_ms` lives; Compose's bound fixed at generation | ruling 107 |
| N5 | acknowledgement services never dropped; failed announcements | rulings 105 and 112 |
| N6 | stale documents (`deployment.md`, the Compose README, 97a) | to update with each milestone |
| N7 | velo's timeouts are client-side, so a forward can arrive late | ruling 107: `begin_drain` after the conditions |
| N8 | retiring the last instance of a segment while sources serve | ruling 112: stated |

Checked and sound by the reviewer: the `setup_timeout` window (the call starts right after `pick`, `released` comes before it); no deadlock from announcing before waiting, including between segments that send to each other; provisioning within the instance's own segment is local, so leaving it out of `sends_to` loses nothing; the admission ledger as the signal for work at an instance; the hub service names (at most 128 bytes); Compose stops the hub last and `init: true` passes SIGTERM on; parsing two paths by hand; ruling 108; a lost lease skipping the wait.

# Issues to raise with velo

Drafts found while building the gdl runtime on velo 0.19.0. Not filed: each needs the owner's approval first.

## 1. A signal for when a finished stream's output is gone

**Observed.** `StreamSender::finalize` consumes the sender and returns at once; when the slot's inlet is full, the terminal frame waits in a background task (`streaming/sender.rs`). The producer's node keeps the slot's withheld queue (up to `slot_byte_budget`, 1 MiB by default) and inlet until the consumer returns credit or cancels (`docs/src/operations/saturation.md`). Nothing tells the producing application when that memory is released.

**Why it matters.** A runtime that admits work by how much egress it holds cannot count a finished stream correctly: releasing at `finalize` undercounts while output is still queued; holding for a fixed grace caps throughput at limit / grace and is still wrong for a consumer that stalls longer than the grace. gdl measured the second: 1,024 permits with a 5 s grace refused benchmark traffic within seconds.

**Ask.** A way to learn that a stream's slot has retired: for example, `StreamSender::finalize_and_wait(self) -> impl Future<Output = Result<(), SendError>>`, resolving once the terminal has been delivered or the slot is torn down, or a retirement callback.

**Addendum 2026-10-20.** Designed and prototyped against `1bbeb88` (ruling 120; `agent-docs/velo/retired/`): `StreamSender::retired()`, resolving `Sent` or `Discarded`, carried on the per-stream allocation the sender and its slot already share, with no wire change, no new message, and no measurable cost when unused. It needs a second change: a graceful shutdown that writes frames already admitted to a TCP or UDS writer instead of failing them. dynamo's PR 15231 has the same gap: its worker releases its in-flight count at `finish` and then shuts velo down.

## 2. Single-stream throughput in one process

**Observed.** With default settings, two velo 0.19 nodes in one process over loopback TCP move one stream of small items (about 12 bytes each, `Vec<u8>` items, the server sending in a loop and finalizing) at about 12 K items/s, about 83 us per item. A one-item request (unary request carrying an anchor handle, then attach and stream) takes about 330 us. Setup: `crates/gdl/benches/remote.rs` in dynamo-gdl, `velo-direct` cases; 4 tokio worker threads.

**Ask.** Is this the expected single-stream rate with the default mux settings (initial credit 32, auto flush on admission, no linger)? If not, which setting is the intended lever?

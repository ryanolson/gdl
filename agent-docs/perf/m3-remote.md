# Milestone 3 measurements: crossing segments

Measured 2026-10-15 with `cargo bench -p gdl --bench remote` (release profile; one process; 4 tokio worker threads; velo 0.19.0 nodes on loopback TCP with default settings; aarch64 workstation). The reference graph's mock sink emits items with no delay, so the numbers are the runtime's and velo's own cost.

| Path | Latency, 1 item per request | Throughput, 1,000 items per request | Allocations per request (1 item) | Live tasks before and after 2,000 requests |
|---|---|---|---|---|
| Local (all nodes in one segment) | 25 us | 586 K items/s | 67 | 49, 49 |
| gdl, 2 segments: `{A,B,C,F,E,G} {D}` | 256 us | 10.5 K items/s | 208 | 57, 59 |
| gdl, 3 segments: `{A,B,C,G} {D} {F,E}` | 558 us | 10.4 K items/s | 347 | 85, 85 |
| velo direct: unary request carrying an anchor handle, server attaches and streams | 330 us | 12.0 K items/s | 130 | 93, 93 |

Reading:

- **Per request, gdl over two segments is faster than the direct velo baseline** (256 vs 330 us): the forward envelope carries a prebound ticket, so the producer opens its stream with no attach round trip; the direct baseline attaches.
- **The third segment costs one more round trip and one more crossing** (558 us): the outer end opens the intermediate run before dispatch.
- **Per item, gdl costs about 13 % over direct velo** (10.5 K vs 12.0 K items/s): the edge codec, the `WireItem` wrapper, one pump task per crossing, and the boundary wrappers. Both are bounded by velo's single-stream rate in this setup (about 83 us per item); that rate is velo's characteristic, not gdl's, and is worth raising with velo separately.
- **Allocations:** a crossing costs about 70 allocations per request on top of velo's own (anchor, ticket, envelope, pump, scopes). Not yet optimized.
- **Tasks return to baseline**; the two extra tasks after 2,000 two-segment requests did not grow with the request count.

Local per-item and per-request costs for composition alone are in `agent-docs/reviews/m1-r*.md` (`benches/hop.rs`).

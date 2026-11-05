# Target feature costs

What `[deployment.codegen.arm64] target_features = ["+lse"]` saves, and what the thin LTO costs that a build with a table uses ([Target features design](../development/codegen-design.md)). Counts only: callgrind instructions (Ir) and allocations, no wall time. Ir counts each instruction as 1, so a saving of 10% in Ir is not a saving of 10% in time.

- Thin LTO with `+lse`, the build a table gives, uses about 10.6% fewer Ir per request through local stages than the default build, and about 8.7% fewer per two-segment request with metrics.
- Thin LTO alone costs about 0.8% on local stages.
- Allocations do not change with the build.
- Fat LTO with `+lse` is not offered: on a core without LSE it faults before `main`, where the start check cannot refuse it.

## Instructions per request

**Setup.** One shared 20-core aarch64 host (Cortex-X925 and Cortex-A725 cores), rustc 1.93.1, valgrind 3.22 callgrind, gdl at `5853294d`. The load average was 29 to 40. Load does not change an instruction count. Every build has `--cfg rustix_use_libc`, which callgrind needs, and one codegen unit. `+lse` adds `-Ctarget-feature=+lse`, as the build of an image with the table does.

```bash
# <lto> is fat or thin; <flags> adds " -Ctarget-feature=+lse" for the +lse builds.
export RUSTFLAGS="--cfg rustix_use_libc<flags>" \
  CARGO_PROFILE_RELEASE_LTO=<lto> CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
  CARGO_PROFILE_BENCH_LTO=<lto> CARGO_PROFILE_BENCH_CODEGEN_UNITS=1
cargo build --locked --release -p gdl --example stages
cargo bench --locked -p gdl-runtime --bench requests --no-run
cargo bench --locked -p gdl-runtime --bench calls --no-run
valgrind --tool=callgrind --callgrind-out-file=/dev/null <binary> <args> <n>
valgrind --tool=callgrind --callgrind-out-file=/dev/null <binary> <args> <2n>
```

Each figure is (Ir at 2n − Ir at n) / n, paired by round, over 7 rounds, six runs at a time. The table gives the median, with the lowest and highest round in brackets.

| Row: args, n and 2n | fat LTO (the default build) | thin LTO | thin LTO, `+lse` (the build with a table) | fat LTO, `+lse` (not offered) |
|---|---|---|---|---|
| local stages: `stages operator u64 5 1`, 200 and 400, per request | 7,008.9 (7,008.6 to 7,009.6) | 7,065.3 (7,064.9 to 7,065.9) | **6,263.2** (6,262.9 to 6,263.5) | 6,215.9 (6,215.6 to 6,216.2) |
| one-segment request with metrics: `requests one-metrics`, 200 and 400 | 42,343 (38,822 to 43,754) | 41,369 (37,722 to 43,832) | 40,897 (37,870 to 43,923) | 40,693 (38,173 to 41,446) |
| two-segment request with metrics: `requests two-metrics`, 200 and 400 | 244,357 (239,716 to 250,094) | 248,750 (228,195 to 251,931) | **223,144** (218,012 to 233,987) | 219,317 (209,038 to 224,463) |
| remote call: `calls remote`, 1,000 and 2,000, per call | 39,913 (31,988 to 40,904) | 41,155 (40,412 to 41,677) | 37,697 (28,162 to 45,617) | 36,057 (35,135 to 36,862) |

The `requests` and `calls` rows spread over thousands of Ir between rounds: thread scheduling moves them more than the build does. Only the local stages row and the two-segment row show the change beyond their spread. On the others the median falls, inside the spread.

## Allocations

The benches print their own counts. `stages` makes 17.00 allocations per request in every round of every build. `calls remote` makes 26.5 to 27.1 per call at 2,000 calls in every build, over 5 runs each, without valgrind. `requests` counts only when it is built with `--cfg gdl_count_allocations`, since the count's own atomic add would move the Ir beside it. Over 5 runs of 400 requests each, without valgrind:

| Row | fat LTO | thin LTO | thin LTO, `+lse` | fat LTO, `+lse` |
|---|---|---|---|---|
| `requests one-metrics` | 60.00 to 60.64 | 60.64 | 60.64 to 60.65 | 60.64 |
| `requests two-metrics` | 187.67 to 189.93 | 186.35 to 190.88 | 185.00 to 190.93 | 187.86 to 192.26 |

## Start on a core without LSE

A segment built with `+lse`, run under QEMU on an emulated Cortex-A57 (Armv8.0, no LSE) and Cortex-A76 (Armv8.2, with LSE). QEMU gives the binary the emulated CPU's `/proc/cpuinfo`.

```bash
bash scripts/qemu-startcheck.sh
```

Built for `aarch64-unknown-linux-gnu` with `-Ctarget-feature=+lse`, the chat example's engine segment with mimalloc, as every segment image links it, run with no settings file, on rustc 1.93.1 and QEMU 8.2.2:

| LTO | Cortex-A57 (no LSE) | Cortex-A76 (LSE) |
|---|---|---|
| fat | exit 132: SIGILL, with no message | exit 1: `engine: usage: chat-engine [--healthcheck] <config.toml> [override.toml ...]` |
| thin | exit 1: ``segment `engine`: this binary was built for CPU feature `lse`, which this aarch64 CPU does not have; ...`` | exit 1: the same usage line |
| none | exit 1: the same refusal | exit 1: the same usage line |
| thin, beside a Cargo configuration with `build.rustflags = ["-Clto=fat"]`, and `CARGO_BUILD_RUSTFLAGS=-Clto=fat` | exit 1: the same refusal | exit 1: the same usage line |

On the Cortex-A76 the binary passes the check, starts its runtime, and stops at its settings, as it does on any host without a settings file.

With fat LTO, std's start-up is built with `+lse` and runs an inline `cas` before `main`, so the Cortex-A57 faults with SIGILL and no message. With thin LTO or none, the binary's start check runs first and refuses the Cortex-A57 with its one line. Run the script again on each toolchain update.

The script's last row shows that thin LTO is not enough for every feature. A binary that only runs the start check, built for amd64 with thin LTO by `rustc` directly, since the generator refuses `+avx2`, run under `qemu-x86_64-static` 8.2.2 on an emulated Nehalem, which has no AVX:

| Build | Nehalem |
|---|---|
| `-Ctarget-feature=+avx2` | exit 132: SIGILL, with no message. `std::rt::lang_start_internal` runs `vmovaps` before `main` |
| no target feature | exit 139: QEMU's own internal fault, `QEMU internal SIGSEGV {code=MAPERR, addr=0x20}`, which QEMU 8.2's x86 user mode on an aarch64 host gives every program after its start, a hello world too |

So the SIGILL is the feature's, and a codegen table cannot name `+avx2` ([A table names only proven features](../development/codegen-design.md#a-table-names-only-proven-features)).

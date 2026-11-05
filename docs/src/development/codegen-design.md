# Target features design

A `[deployment.codegen.<arch>]` table builds the segment images of one architecture for CPU features ([Target features](../guides/kubernetes.md#target-features)). This chapter says why the build and the start check are as they are, and what was rejected. [Target feature costs](../operations/target-feature-costs.md) has the numbers.

## The setting is in the image build

The gain comes only from how a binary is built: an inline LSE atomic is one instruction, and the default build calls a helper that first checks the CPU. So the setting goes where the generator builds the segment images, per architecture.

- **Not in `.cargo/config.toml`.** A repository's Cargo configuration never reaches a segment image, whose context holds only the generated crates. Any `RUSTFLAGS` in the environment replaces it with no word. `--local` builds refuse any Cargo configuration in the sources but one network key.
- **Not a target CPU.** A CPU name turns on many features: `neoverse-n1` turns on `crc`, `rdm`, `dotprod`, `rcpc`, `ssbs`, and more. Each of them needs its own proof that the start check runs before it, so the table names features, not a CPU.
- **Not a run-time check in the meter.** A check of the CPU at each atomic add costs more than the helper call it replaces: 1,145 instructions per visit against 1,110. One check per visit step saves 69 of 1,110, or 0.16% of a request. It needs `unsafe` code and a second copy of the visit path.

## The build of an architecture with a table uses thin LTO

The start check must run before any instruction of the features. With the profile's fat LTO, std's own start-up is built with the image's flags too, since fat LTO compiles std's bitcode into the one module. It then runs an inline `cas` before `main`, in the `ThreadId` of the main thread. So on a core without LSE the binary faults before the check can run. With thin LTO, or with none, std's start-up kept the helpers for `+lse` on arm64, and the check ran first. QEMU shows this ([Target feature costs](../operations/target-feature-costs.md#start-on-a-core-without-lse)). So the build of an architecture with a table sets `CARGO_PROFILE_RELEASE_LTO=thin`, over the build image's own, which cargo also reads over a `[profile.release]` in a Cargo configuration.

A flag in the build image's `RUSTFLAGS` could still set fat LTO: rustc reads it after the profile's thin LTO. So the build refuses any `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS` the build image sets, whatever it holds, and sets `RUSTFLAGS` to exactly the table's flags. Cargo then reads no `rustflags` from the build image's configuration or environment either. `RUSTC`, `RUSTC_WRAPPER`, `RUSTC_WORKSPACE_WRAPPER`, and the image's Cargo configuration are trusted, as the build image itself is.

- **Rejected: a list of the flags that set LTO.** The build first refused `-C lto` and `-C embed-bitcode=no` in an inherited `RUSTFLAGS`, and kept the rest. Each review found a form that passed the list and gave fat LTO: `--codegen lto=fat` and `--codegen=lto=fat`, then `-C @file` with `lto=fat` in the file, then `-C embed_bitcode=no`, since rustc takes `_` for `-` in a key. A list over rustc's option grammar cannot be finished, so the build reads no inherited flag. A build with fat LTO and `+lse` is not offered: it faults before `main` on an Armv8.0 core.

## A table names only proven features

Thin LTO does not keep std's start-up clean for every feature. Built for amd64 with `+avx2` and thin LTO, `std::rt::lang_start_internal` runs AVX (`vmovaps`) before it calls `main`. So a core without AVX faults before the start check, as with fat LTO and `+lse`. QEMU shows it on a Nehalem core.

So a table can name only a feature that QEMU shows the start check refusing first. The list is `PROVEN` in `crates/gdl-gen/src/deployment.rs`, and holds `+lse` for arm64. `check`, `images`, and `k8s` refuse any other feature, with one line that names it. A new entry needs its own row in `scripts/qemu-startcheck.sh`: a build with the feature, on an emulated core without it, that ends with the start check's line.

- **Rejected: run the check before std's start-up.** A function in `.init_array` runs before `main`, but it needs `#[unsafe(link_section)]` in the generated code, and generated code has no `unsafe`.
- **Rejected: fat LTO, with no start check for `lse`.** It saves about 50 more instructions per request on local stages than thin LTO does, but a core without LSE then faults with no message.

## The start check reads `/proc/cpuinfo`

The generated `main` calls `gdl_runtime::check_cpu_features` first. It reads the features the kernel lists for each CPU in `/proc/cpuinfo`: `Features` on arm64, which the kernel prints from the CPU's HWCAP bits, and `flags` on x86_64. A feature counts only when every CPU lists it. A table maps each rustc feature name to the kernel's names: `lse` is `atomics`, and `sha2` needs `sha1` and `sha2`. The check covers each feature of the table that the binary was built with (`cfg!(target_feature = ...)`), not only the ones the spec names. A file it cannot read, or one that lists no CPU's features, stops the binary too. The table leaves out `ssbs`: a kernel can hide SSBS on cores that have it, as on a Cortex-X925 and QEMU's Neoverse N1, so the check would refuse those cores. The compiler emits no SSBS instruction of its own.

- **Rejected: `std::arch::is_aarch64_feature_detected!` and `is_x86_feature_detected!`.** Each expands to `cfg!(target_feature = "<f>") || <run-time check>`. In a binary built with `+lse`, the check of `lse` is the constant `true`, and the compiler removes it. A binary whose `main` calls only that check, built with `+lse`, compiles `main` to `mov w0, wzr; ret`.
- **Rejected: the aux vector through a crate such as rustix.** That crate is built with the image's flags too, and each of its paths would have to be shown free of atomics before the check.

The check holds no atomic operation of its own. Built with `+lse` and thin LTO, it runs on a Cortex-A57, which has no LSE, and prints its line.

The tests of the check (`crates/gdl-runtime/src/cpu.rs`) read a file of their own in place of `/proc/cpuinfo`, under `cfg(test)` only. They run the real parser, the real table, and the real exit path, in a process of their own.

#!/usr/bin/env bash
# Run a generated segment binary built as `cargo gdl images` builds an arm64 image with
# `[deployment.codegen.arm64] target_features = ["+lse"]`, and mimalloc, on two emulated CPUs, outside the default suite: an
# Armv8.0 core without LSE (QEMU's cortex-a57) and one with it (cortex-a76). It builds the chat example's segments
# for aarch64-unknown-linux-gnu with `-Ctarget-feature=+lse`, at fat, thin, and no LTO, and runs the engine's binary
# under qemu-aarch64-static, which also gives the binary the emulated CPU's /proc/cpuinfo. It checks that:
#   - built with thin LTO, as the image build builds an architecture with a codegen table, the binary refuses the
#     cortex-a57 with its one line that names `lse`, and exits 1, before it runs any instruction of the feature;
#   - on the cortex-a76 it passes the check and starts: with no settings file it stops at its settings, as on any
#     host;
#   - with no LTO it does the same;
#   - with thin LTO and a Cargo configuration whose `build.rustflags`, and a CARGO_BUILD_RUSTFLAGS, ask for fat LTO
#     (row `cfg`), it does the same: the build step sets RUSTFLAGS, which cargo reads in place of both.
# It prints what fat LTO does too, without a check: fat LTO compiles std's start-up with the flags, so on the
# cortex-a57 the binary faults with SIGILL before `main`. That is why an architecture with a table builds with thin
# LTO (docs/src/development/codegen-design.md). Rerun it on each toolchain bump: what runs before `main` is the
# toolchain's.
#
# Each feature a codegen table can name (`PROVEN` in crates/gdl-gen/src/deployment.rs) has a row here: arm64 `+lse`
# today. A last row shows that the script sees a failure: a binary that only runs the start check, built for amd64
# with `+avx2` and thin LTO, outside the generator, which refuses `+avx2`. On QEMU's Nehalem, which has no AVX, it
# must fault with SIGILL, since std's start-up runs AVX before `main`. The same binary built without `+avx2` must not:
# that shows the fault is the feature's. (QEMU 8.2's x86 user mode on an aarch64 host then stops every program with an
# internal fault of its own, exit 139, so no x86 binary is shown to start here.) The row needs qemu-x86_64-static (or
# qemu-x86_64), the x86_64-unknown-linux-gnu standard library, x86_64-linux-gnu-gcc, and the x86_64 C library under
# X86_SYSROOT (default /usr/x86_64-linux-gnu, Debian's libc6-amd64-cross); without one, it says so in one line, and the
# arm64 rows still run.
#
#   INSTALL_TOOLS=1 bash scripts/qemu-startcheck.sh   # first extracts Debian's qemu-user-static into TOOLS_DIR
#   bash scripts/qemu-startcheck.sh
#
# It needs an aarch64 host, which builds the binaries natively; on another host, or without qemu-aarch64-static on
# the PATH or in TOOLS_DIR, it says so in one line and exits 0. INSTALL_TOOLS=1 uses `apt-get download`, which needs
# no root. TOOLS_DIR holds the tool (default: .qemu-tools at the repository's root, which git ignores). TIMEOUT
# bounds each build in seconds (default 1800: each builds three release binaries). On exit it removes its scratch
# directory, which holds the spec, the generated crates, and their target directories.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
tools="${TOOLS_DIR:-${root}/.qemu-tools}"
TIMEOUT="${TIMEOUT:-1800}"

skip() {
  echo "qemu-startcheck.sh: skipped: $*"
  exit 0
}
fail() {
  echo "qemu-startcheck.sh: $*" >&2
  exit 1
}

[[ "$(uname -m)" == "aarch64" ]] || skip "it builds aarch64 binaries natively, and this host is $(uname -m)"
if [[ "${INSTALL_TOOLS:-0}" == "1" ]] && ! command -v qemu-aarch64-static >/dev/null \
  && [[ ! -x "${tools}/usr/bin/qemu-aarch64-static" ]]; then
  download="$(mktemp -d)"
  trap 'rm -rf "${download}"' EXIT
  (cd "${download}" && apt-get download qemu-user-static >/dev/null)
  mkdir -p "${tools}"
  dpkg -x "${download}"/qemu-user-static_*.deb "${tools}"
  rm -rf "${download}"
  trap - EXIT
fi
if command -v qemu-aarch64-static >/dev/null; then
  qemu="$(command -v qemu-aarch64-static)"
elif [[ -x "${tools}/usr/bin/qemu-aarch64-static" ]]; then
  qemu="${tools}/usr/bin/qemu-aarch64-static"
else
  skip "qemu-aarch64-static is not on the PATH or in ${tools}; run with INSTALL_TOOLS=1"
fi

scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

# The chat example's spec, every path made absolute, with the table.
chat="${root}/examples/chat"
sed -e "s|^path = \"chat.gdl\"|path = \"${chat}/chat.gdl\"|" \
  -e "s|^crates = \[\"protocols\"\]|crates = [\"${chat}/protocols\"]|" \
  -e "s|^crates = \[\"front-nodes\", \"engine-mock\", \"post-nodes\"\]|crates = [\"${chat}/front-nodes\", \"${chat}/engine-mock\", \"${chat}/post-nodes\"]|" \
  -e "s|^path = \"../../crates/gdl-runtime\"|path = \"${root}/crates/gdl-runtime\"|" \
  "${chat}/gdl.toml" >"${scratch}/gdl.toml"
for absolute in "${chat}/chat.gdl" "${chat}/protocols" "${chat}/engine-mock" "${root}/crates/gdl-runtime"; do
  grep -qF "\"${absolute}\"" "${scratch}/gdl.toml" || fail "the chat spec no longer reads as this script expects"
done
# mimalloc, as every segment image links it: its C code runs before `main`, when std's start-up allocates.
printf '\n[binary]\nallocator = "mimalloc"\n\n[deployment.codegen.arm64]\ntarget_features = ["+lse"]\n' >>"${scratch}/gdl.toml"

# The same spec beside a Cargo configuration that asks for fat LTO through rustflags, for the row `cfg`: cargo reads
# configuration from the directory it runs in, which is the spec's, and the row also sets CARGO_BUILD_RUSTFLAGS.
mkdir -p "${scratch}/configured/.cargo"
cp "${scratch}/gdl.toml" "${scratch}/configured/gdl.toml"
printf '[build]\nrustflags = ["-Clto=fat"]\n' >"${scratch}/configured/.cargo/config.toml"

(cd "${root}" && cargo build -q -p gdl-gen --bin cargo-gdl)
gdl="${CARGO_TARGET_DIR:-${root}/target}/debug/cargo-gdl"

# Each LTO, and what the engine's binary did on each CPU: its exit status, whether it printed the refusal, and the
# first line it printed on stderr.
declare -A status refused said
for lto in fat thin off cfg; do
  # As the image build: exactly the table's flags in RUSTFLAGS, which reach only the target's code, not build
  # scripts and proc macros. `cfg` is thin LTO with the configured spec, and CARGO_BUILD_RUSTFLAGS too: RUSTFLAGS
  # outranks both, so neither may bring fat LTO back.
  spec="${scratch}/gdl.toml" profile="${lto}" extra=()
  if [[ "${lto}" == "cfg" ]]; then
    spec="${scratch}/configured/gdl.toml" profile=thin extra=(CARGO_BUILD_RUSTFLAGS=-Clto=fat)
  fi
  env "${extra[@]}" CARGO_BUILD_TARGET=aarch64-unknown-linux-gnu RUSTFLAGS="-Ctarget-feature=+lse" \
    CARGO_PROFILE_RELEASE_LTO="${profile}" \
    timeout "${TIMEOUT}" "${gdl}" gdl build --spec "${spec}" --out "${scratch}/out-${lto}" \
    --release --target-dir "${scratch}/target-${lto}" </dev/null >/dev/null \
    || fail "the build at ${lto} LTO failed"
  # `--out` holds the binaries; the generated workspace is in the target directory.
  binary="${scratch}/out-${lto}/chat-engine"
  [[ -x "${binary}" ]] || fail "no ${binary}"
  main="$(find "${scratch}/target-${lto}" -path '*/workspace/engine/src/main.rs' -print -quit)"
  [[ -n "${main}" ]] && grep -q "check_cpu_features" "${main}" || fail "the engine's main does not check its CPU"
  for cpu in cortex-a57 cortex-a76; do
    set +e
    timeout 60 "${qemu}" -cpu "${cpu}" "${binary}" </dev/null >"${scratch}/stdout" 2>"${scratch}/stderr"
    status["${lto} ${cpu}"]=$?
    set -e
    said["${lto} ${cpu}"]="$(head -c 100 "${scratch}/stderr" | head -n 1)"
    if grep -q "this binary was built for CPU feature \`lse\`, which this aarch64 CPU does not have" "${scratch}/stderr"; then
      refused["${lto} ${cpu}"]=yes
    else
      refused["${lto} ${cpu}"]=no
    fi
  done
done

printf '%-5s %-11s %-5s %-8s %s\n' LTO CPU exit refused "first line on stderr"
for lto in fat thin off cfg; do
  for cpu in cortex-a57 cortex-a76; do
    printf '%-5s %-11s %-5s %-8s %s\n' "${lto}" "${cpu}" "${status["${lto} ${cpu}"]}" "${refused["${lto} ${cpu}"]}" \
      "${said["${lto} ${cpu}"]}"
  done
done

for lto in thin off cfg; do
  [[ "${status["${lto} cortex-a57"]}" == "1" && "${refused["${lto} cortex-a57"]}" == "yes" ]] \
    || fail "at ${lto} LTO, the cortex-a57 was not refused with the one line and exit 1"
  # A start past the check: its own settings stop it, with no signal and no refusal.
  [[ "${status["${lto} cortex-a76"]}" -lt 128 && "${refused["${lto} cortex-a76"]}" == "no" ]] \
    || fail "at ${lto} LTO, the cortex-a76 did not pass the check"
done

# The amd64 row, which must fail: it shows that the script sees a fault before the check.
x86_skip=""
qemu_x86="$(command -v qemu-x86_64-static || command -v qemu-x86_64 || true)"
[[ -n "${qemu_x86}" ]] || x86_skip="qemu-x86_64-static is not on the PATH"
command -v x86_64-linux-gnu-gcc >/dev/null || x86_skip="${x86_skip:-x86_64-linux-gnu-gcc is not on the PATH}"
x86_sysroot="${X86_SYSROOT:-/usr/x86_64-linux-gnu}"
[[ -e "${x86_sysroot}/lib64/ld-linux-x86-64.so.2" ]] \
  || x86_skip="${x86_skip:-no x86_64 C library under ${x86_sysroot} (X86_SYSROOT)}"
[[ -d "$(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu" ]] \
  || x86_skip="${x86_skip:-the x86_64-unknown-linux-gnu standard library is not installed (rustup target add x86_64-unknown-linux-gnu)}"
if [[ -n "${x86_skip}" ]]; then
  echo "qemu-startcheck.sh: the amd64 +avx2 row skipped: ${x86_skip}"
else
  printf '#[path = "%s"]\nmod cpu;\nfn main() -> std::process::ExitCode {\n    match cpu::check_cpu_features("engine") {\n        Ok(()) => std::process::ExitCode::SUCCESS,\n        Err(code) => code,\n    }\n}\n' \
    "${root}/crates/gdl-runtime/src/cpu.rs" >"${scratch}/check.rs"
  printf '%-9s %-11s %-5s %-8s %s\n' build CPU exit refused "first line on stderr"
  for build in avx2 baseline; do
    flags=()
    [[ "${build}" == "avx2" ]] && flags=(-Ctarget-feature=+avx2)
    timeout "${TIMEOUT}" rustc --edition=2024 --target=x86_64-unknown-linux-gnu -Clinker=x86_64-linux-gnu-gcc \
      -Copt-level=3 -Ccodegen-units=1 -Clto=thin "${flags[@]}" "${scratch}/check.rs" -o "${scratch}/check-${build}" \
      </dev/null || fail "the amd64 ${build} build failed"
    set +e
    timeout 60 "${qemu_x86}" -L "${x86_sysroot}" -cpu Nehalem "${scratch}/check-${build}" </dev/null \
      >"${scratch}/stdout" 2>"${scratch}/stderr"
    status["x86 ${build}"]=$?
    set -e
    refused["x86 ${build}"]=no
    grep -q "this binary was built for CPU feature" "${scratch}/stderr" && refused["x86 ${build}"]=yes
    printf '%-9s %-11s %-5s %-8s %s\n' "${build}" Nehalem "${status["x86 ${build}"]}" "${refused["x86 ${build}"]}" \
      "$(grep -v "TCG doesn't support" "${scratch}/stderr" | head -c 100 | head -n 1)"
  done
  [[ "${status["x86 avx2"]}" == "132" ]] \
    || fail "amd64 +avx2 on Nehalem did not fault with SIGILL: the script would not see a fault before the check"
  [[ "${status["x86 baseline"]}" != "132" ]] || fail "amd64 without +avx2 on Nehalem faulted with SIGILL too"
fi
echo "qemu-startcheck.sh: ok"

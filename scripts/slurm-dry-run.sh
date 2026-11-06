#!/usr/bin/env bash
# Dry-run each srt-slurm recipe that `cargo gdl slurm` writes with `srtctl dry-run`, at the srt-slurm commit the
# recipes are written for, fetched through uv. A recipe srt-slurm would refuse fails the run. Read-only: srtctl
# renders each job's sbatch script and submits nothing. Outside the default suite: the recipes' contents are pinned by
# `crates/gdl-gen/tests/deployment/slurm.rs`, and this checks that srt-slurm accepts each one.
#
#   bash scripts/slurm-dry-run.sh [dir ...]
#
# Without a directory it dry-runs every recipe under crates/gdl-gen/tests/fixtures/slurm/out. Each run reads a
# placeholder `srtslurm.yaml` written to a temporary directory: an account, a partition, a time limit, and a network
# interface that name nothing. TIMEOUT bounds each dry run, in seconds (default 300: the first run fetches srt-slurm
# and its dependencies).
set -euo pipefail

# The commit of github.com/NVIDIA/srt-slurm the recipes are written for.
SRT_SLURM_COMMIT="acfb4dcc3bd48fd6a1e7b615e849e7954c0c3852"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v uv >/dev/null || { echo "slurm-dry-run.sh: uv is not on the PATH; install it from https://docs.astral.sh/uv/getting-started/installation/" >&2; exit 1; }

dirs=("$@")
if [[ ${#dirs[@]} -eq 0 ]]; then
  dirs=("${root}/crates/gdl-gen/tests/fixtures/slurm/out")
fi

scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
cat >"${scratch}/srtslurm.yaml" <<'EOF'
default_account: "ACCOUNT"
default_partition: "PARTITION"
default_time_limit: "04:00:00"
network_interface: "eth0"
gpus_per_node: 4
EOF

recipes=()
for dir in "${dirs[@]}"; do
  # Absolute: each dry run runs from the scratch directory.
  dir="$(cd "${dir}" && pwd)" || { echo "slurm-dry-run.sh: no directory ${dir}" >&2; exit 1; }
  while IFS= read -r -d '' recipe; do
    recipes+=("${recipe}")
  done < <(find "${dir}" -name '*.yaml' -type f -print0 | sort -z)
done
if [[ ${#recipes[@]} -eq 0 ]]; then
  echo "slurm-dry-run.sh: no recipe under ${dirs[*]}" >&2
  exit 1
fi

failed=0
for recipe in "${recipes[@]}"; do
  log="${scratch}/dry-run.log"
  if (cd "${scratch}" && SRTSLURM_CONFIG="${scratch}/srtslurm.yaml" timeout "${TIMEOUT:-300}" \
    uvx -q --from "git+https://github.com/NVIDIA/srt-slurm@${SRT_SLURM_COMMIT}" srtctl dry-run -f "${recipe}") \
    </dev/null >"${log}" 2>&1; then
    echo "ok      ${recipe#"${root}/"}"
  else
    echo "refused ${recipe#"${root}/"}" >&2
    cat "${log}" >&2
    failed=1
  fi
done
exit "${failed}"

#!/usr/bin/env bash
# Build the mdBook in docs/ and check its internal links.
#
# With INSTALL_TOOLS=1, download the pinned tool versions first (x86_64 Linux
# release binaries, as on GitHub-hosted runners). The versions match
# ../velo and ../rhino so the books render the same way.
set -euo pipefail

MDBOOK_VERSION="0.4.52"
MDBOOK_MERMAID_VERSION="0.16.2"
MDBOOK_LINKCHECK_VERSION="0.7.7"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${INSTALL_TOOLS:-0}" == "1" && "$(uname -m)" != "x86_64" ]]; then
  # mdbook-linkcheck publishes no release binary for other architectures, so build all three.
  for tool in "mdbook@${MDBOOK_VERSION}" "mdbook-mermaid@${MDBOOK_MERMAID_VERSION}" "mdbook-linkcheck@${MDBOOK_LINKCHECK_VERSION}"; do
    cargo install --locked --quiet --root "${root}/.book-tools" "${tool}"
  done
  export PATH="${root}/.book-tools/bin:${PATH}"
elif [[ "${INSTALL_TOOLS:-0}" == "1" ]]; then
  bin="${root}/.book-tools/bin"
  mkdir -p "${bin}"
  curl -sSfL "https://github.com/rust-lang/mdBook/releases/download/v${MDBOOK_VERSION}/mdbook-v${MDBOOK_VERSION}-x86_64-unknown-linux-gnu.tar.gz" \
    | tar -xz -C "${bin}"
  curl -sSfL "https://github.com/badboy/mdbook-mermaid/releases/download/v${MDBOOK_MERMAID_VERSION}/mdbook-mermaid-v${MDBOOK_MERMAID_VERSION}-x86_64-unknown-linux-gnu.tar.gz" \
    | tar -xz -C "${bin}"
  tmp="$(mktemp -d)"
  curl -sSfL -o "${tmp}/linkcheck.zip" \
    "https://github.com/Michael-F-Bryan/mdbook-linkcheck/releases/download/v${MDBOOK_LINKCHECK_VERSION}/mdbook-linkcheck.x86_64-unknown-linux-gnu.zip"
  unzip -q -o "${tmp}/linkcheck.zip" -d "${bin}"
  chmod +x "${bin}"/mdbook*
  export PATH="${bin}:${PATH}"
fi

mdbook --version
mdbook-mermaid --version
mdbook-linkcheck --version

# The mermaid assets are generated, not committed (see docs/.gitignore).
mdbook-mermaid install "${root}/docs"
mdbook build "${root}/docs"

# With more than one renderer, mdBook writes the HTML to book/html/.
test -f "${root}/docs/book/html/index.html"

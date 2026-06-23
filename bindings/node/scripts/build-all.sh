#!/usr/bin/env bash
# Build the two macOS napi targets on the local machine (Apple Silicon Mac mini).
# Linux and Windows targets are handled by scripts/remote-build.sh via SSH.
#
# Requirements:
#   rustup target add aarch64-apple-darwin x86_64-apple-darwin
#
# Output:
#   artifacts/flowdb-node.darwin-{arm64,x64}.node

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p artifacts

# (alias, rust target triple, napi platform suffix)
TARGETS=(
  "arm64-mac|aarch64-apple-darwin|darwin-arm64"
  "x64-mac|x86_64-apple-darwin|darwin-x64"
)

FILTER="${1:-all}"

for entry in "${TARGETS[@]}"; do
  IFS='|' read -r alias triple suffix <<< "$entry"
  if [[ "$FILTER" != "all" && "$alias" != "$FILTER" ]]; then
    continue
  fi
  echo "==> Building $alias ($triple)"
  napi build --platform --release --target "$triple"
  mv -f "flowdb-node.${suffix}.node" "artifacts/flowdb-node.${suffix}.node"
  echo "    -> artifacts/flowdb-node.${suffix}.node"
done

echo
echo "==> macOS builds done."

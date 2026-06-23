#!/usr/bin/env bash
# Cross-machine build orchestrator (single Linux remote).
#
# Verified strategy on Ubuntu 22.04 x86_64:
#   x86_64-unknown-linux-gnu   → native cargo
#   aarch64-unknown-linux-gnu  → gcc-aarch64-linux-gnu (apt) cross
#   x86_64-pc-windows-msvc     → cargo-xwin (Microsoft SDK repackage)
#
# Mac mini handles the two darwin targets locally via build-all.sh.
# After both finish, run on Mac:
#   node scripts/publish-platforms.js
#   npm publish && (for p in npm/*/; do (cd $p && npm publish); done)
#
# Prerequisites on remote:
#   - passwordless sudo (for apt install)
#   - outbound HTTPS (to rsproxy.cn, crates.io, Microsoft CDN)
#
# Usage:
#   LINUX_SSH=user@host ./scripts/remote-build.sh
#   LINUX_SSH="user@host -p 2222" SKIP_BOOT=1 ./scripts/remote-build.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

: "${LINUX_SSH:?LINUX_SSH is required}"
REMOTE_PATH="${REMOTE_PATH:-flowdb}"   # relative to $HOME on remote
SKIP_BOOT="${SKIP_BOOT:-0}"

REMOTE="$LINUX_SSH:~/$REMOTE_PATH"
LOCAL_ROOT="$(cd "$ROOT/../.." && pwd)"   # workspace root containing Cargo.toml

mkdir -p "$ROOT/artifacts"

# ── 1. sync repo (rsync workspace root so path="../../" resolves) ──
echo "==> rsync $LOCAL_ROOT → $REMOTE"
rsync -az --delete --rsh="ssh -o BatchMode=yes" \
  --exclude target --exclude node_modules --exclude .git \
  --exclude artifacts --exclude 'flowdb-node.*.node' \
  --exclude '*.log' --exclude dist --exclude npm \
  "$LOCAL_ROOT/" "$REMOTE/"

# ── 2. bootstrap toolchain (idempotent) ───────────────────────────
if [[ "$SKIP_BOOT" != "1" ]]; then
  echo
  echo "==> bootstrap toolchain (idempotent)"
  ssh -o BatchMode=yes "$LINUX_SSH" "RP='$REMOTE_PATH'" 'bash -s' <<'BOOT'
set -euo pipefail

# rustup
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
fi
. "$HOME/.cargo/env"

# RsProxy mirror + aarch64 linker
if ! grep -q rsproxy "$HOME/.cargo/config.toml" 2>/dev/null; then
  mkdir -p "$HOME/.cargo"
  cat > "$HOME/.cargo/config.toml" <<EOF
[source.crates-io]
replace-with = "rsproxy-sparse"
[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"
[registries.rsproxy]
index = "https://rsproxy.cn/crates.io-index"
[net]
git-fetch-with-cli = true

[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"
EOF
fi

rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-pc-windows-msvc >/dev/null

# node
command -v node >/dev/null 2>&1 || {
  curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
  sudo apt-get install -y nodejs
}

# cross toolchains
dpkg -l | grep -q gcc-aarch64-linux-gnu || sudo apt-get install -y gcc-aarch64-linux-gnu
command -v ld.lld >/dev/null || sudo apt-get install -y lld

# cargo subcommands (fast via RsProxy)
cargo install cargo-xwin --locked --quiet

# npm deps
cd "$HOME/$RP/bindings/node"
[ -d node_modules ] || npm install --silent
BOOT
fi

# ── 3. build 3 targets on remote ──────────────────────────────────
build_one() {
  local triple="$1" suffix="$2" extra="${3:-}"
  echo
  echo "==> building $triple"
  ssh -o BatchMode=yes "$LINUX_SSH" "RP='$REMOTE_PATH'" "bash -s" <<BUILD
set -euo pipefail
. "\$HOME/.cargo/env"
cd "\$HOME/\$RP/bindings/node"
npx napi build --platform --release --target $triple $extra
ls -la flowdb-node.$suffix.node
BUILD
  scp -o BatchMode=yes "$LINUX_SSH:~/$REMOTE_PATH/bindings/node/flowdb-node.$suffix.node" \
    "$ROOT/artifacts/"
}

build_one x86_64-unknown-linux-gnu  linux-x64-gnu
build_one aarch64-unknown-linux-gnu linux-arm64-gnu
build_one x86_64-pc-windows-msvc    win32-x64-msvc  --cross-compile

echo
echo "==> Remote artifacts pulled back:"
ls -la "$ROOT/artifacts/"

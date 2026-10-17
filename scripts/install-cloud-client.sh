#!/usr/bin/env bash
# Build the reviewed tools checkout and atomically replace the installed client.
# Pulling Git source alone never updates the executable used by Cloud agents.
set -euo pipefail

grove_source_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
grove_install_dir="${HOME}/.local/bin"
grove_install_stage=""
trap 'if [[ -n "$grove_install_stage" ]]; then rm -f -- "$grove_install_stage"; fi' EXIT

if [[ -n "$(git -C "$grove_source_root" status --porcelain --untracked-files=normal)" ]]; then
    printf 'Use a reviewed clean Grove tools checkout; preserve uncommitted work before installing.\n' >&2
    exit 1
fi
cargo build --manifest-path "$grove_source_root/Cargo.toml" -p grove --locked
grove_target_dir="$(cargo metadata --manifest-path "$grove_source_root/Cargo.toml" --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
mkdir -p -- "$grove_install_dir"
grove_install_stage="$(mktemp "$grove_install_dir/.grove.XXXXXXXX")"
install -m 755 "$grove_target_dir/debug/grove" "$grove_install_stage"
mv -f -- "$grove_install_stage" "$grove_install_dir/grove"
grove_install_stage=""
"$grove_install_dir/grove" --version
git -C "$grove_source_root" rev-parse HEAD
printf 'Installed %s\n' "$grove_install_dir/grove"

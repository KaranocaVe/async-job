#!/bin/sh
set -eu

repo_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
codex_root=${CODEX_HOME:-"$HOME/.codex"}
destination="$codex_root/skills/async-job"
mkdir -p "$codex_root/skills" "$destination"
cp -R "$repo_dir/async-job/." "$destination/"
chmod +x "$destination/scripts/async_watch.py"
printf 'Installed async-job to %s\n' "$destination"

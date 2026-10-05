#!/usr/bin/env bash
# Runs a command while holding the shared bench lock (for CPU-heavy steps such
# as image builds, so they do not land in the middle of someone's timing run).
LOCK=/tmp/BENCH_LOCK
until (set -o noclobber; echo "loadcontrol $$ $(date +%FT%T) $(echo "$*" | head -1 | cut -c1-80)" > "$LOCK") 2>/dev/null; do
  sleep 15
done
trap '[ "$(head -1 "$LOCK" 2>/dev/null | cut -d" " -f1,2)" = "loadcontrol $$" ] && rm -f "$LOCK"' EXIT
LC_LOCK_HELD=1 "$@"

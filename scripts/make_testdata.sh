#!/usr/bin/env bash
# Creates a realistic inbox for the file pipeline demo.
#
#   ./scripts/make_testdata.sh 100
#
# Trailing spaces and CRLF line endings are deliberate: the convert stage must
# normalise them, which makes the output easy to verify.
set -euo pipefail

COUNT="${1:-100}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INBOX="${INBOX:-$ROOT/data/inbox}"

mkdir -p "$INBOX"

for i in $(seq 1 "$COUNT"); do
  name=$(printf 'sample_%04d.txt' "$i")
  lines=$((5 + i % 7))
  : > "$INBOX/$name"
  for l in $(seq 0 $((lines - 1))); do
    printf 'record %d line %d: the quick brown fox jumps over the lazy dog   \r\n' "$i" "$l" >> "$INBOX/$name"
  done
done

# Decoys that the source must ignore.
printf 'incomplete' > "$INBOX/still-writing.tmp"
printf 'incomplete' > "$INBOX/half-download.part"
printf 'invisible' > "$INBOX/.hidden"

echo "created $COUNT text files plus 3 decoys in $INBOX"

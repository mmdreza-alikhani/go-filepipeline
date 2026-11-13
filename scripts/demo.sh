#!/usr/bin/env bash
# End to end demo: build, generate files, run in batch mode, verify, and prove
# that a rerun is idempotent.
#
#   ./scripts/demo.sh 100
set -euo pipefail

COUNT="${1:-100}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -d ./data ]; then rm -rf ./data; fi
mkdir -p ./bin

echo "==> building"
go build -o ./bin/pipeline ./cmd/pipeline

echo "==> generating $COUNT files"
./scripts/make_testdata.sh "$COUNT"
printf '\xff\xfe\x00AB' > ./data/inbox/broken_0001.bin

# How many files the source will actually consider input. Recomputed before the
# rerun because the corrupt file moves to data/failed.
count_inputs() {
    find ./data/inbox -maxdepth 1 -type f ! -name '.*' ! -name '*.tmp' ! -name '*.part' | wc -l | tr -d ' '
}

echo "==> running the pipeline (batch mode: -limit)"
time ./bin/pipeline -config ./configs/config.example.json -limit "$(count_inputs)"

echo "==> verifying"
problems=0
gz_count=0
for f in $(find ./data/out -name '*.gz' -type f); do
    gz_count=$((gz_count + 1))
    if ! gzip -t "$f" 2>/dev/null; then
        echo "PROBLEM: $f is not a valid gzip member"
        problems=$((problems + 1))
        continue
    fi
    if zcat "$f" | grep -q $'\r'; then
        echo "PROBLEM: $f still contains CR"
        problems=$((problems + 1))
    fi
    if zcat "$f" | grep -qE '[ \t]+$'; then
        echo "PROBLEM: $f has trailing whitespace"
        problems=$((problems + 1))
    fi
done

if find ./data/out -type f \( -name '*.part' -o -name '*.tmp' \) | grep -q .; then
    echo "PROBLEM: partial file in out/"
    problems=$((problems + 1))
fi
if [ -n "$(find ./data/out/.tmp -type f 2>/dev/null)" ]; then
    echo "PROBLEM: staging directory is not empty"
    problems=$((problems + 1))
fi

manifests=$(find ./data/out -name '*.json' -type f | wc -l | tr -d ' ')
failed=$(find ./data/failed -name '*.json' -type f 2>/dev/null | wc -l | tr -d ' ')
done_count=$(grep -l '"state": "done"' ./data/work/jobs/*.json 2>/dev/null | wc -l | tr -d ' ')

echo "artefacts          $gz_count"
echo "manifests          $manifests"
echo "journal done       $done_count"
echo "dead letters       $failed"

if [ "$problems" -ne 0 ]; then
    echo "verification failed: $problems problem(s)" >&2
    exit 1
fi
echo "verification: all artefacts are complete, normalised gzip"

echo "==> rerunning the same inbox (idempotency)"
./bin/pipeline -config ./configs/config.example.json -limit "$(count_inputs)" >/dev/null
after=$(find ./data/out -name '*.gz' -type f | wc -l | tr -d ' ')
echo "artefacts before=$gz_count after=$after"
if [ "$after" -ne "$gz_count" ]; then
    echo "the rerun created duplicate artefacts" >&2
    exit 1
fi
echo "idempotency: no duplicate output"

echo "==> dead letter record"
head -c 400 "$(find ./data/failed -name '*.json' -type f | head -n1)"
echo

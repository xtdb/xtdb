#!/usr/bin/env bash

# Downloads SEC EDGAR 'Financial Statement Data Sets' — the quarterly TSV bulk
# dumps — for the xtdb.datasets.edgar.tsv loader. One ZIP per quarter, every
# filer's facts for that quarter; we keep sub.txt + num.txt (gzipped), the only
# two files the loader reads (tag.txt/pre.txt are the tag dictionary and
# presentation, unused).
#
# Unlike companyfacts (one file per company, full history), the quarterly dumps
# are cut by time — this is the breadth/streaming source for the Postgres → XT
# demo. Default window is the last ~4 published quarters.
#
# Requires `curl` and `unzip`. SEC requires a descriptive User-Agent (they 403 a
# blank one); set EDGAR_UA to override. Already-present quarters are skipped.

set -euo pipefail

cd "$(dirname "$0")/.."

OUT="src/dev/resources/data/edgar/tsv"   # gitignored (see src/dev/resources/.gitignore)
UA="${EDGAR_UA:-XTDB datasets hello@xtdb.com}"
BASE="https://www.sec.gov/files/dera/data/financial-statement-data-sets"

# Default: 2025Q2–2026Q1 (the last ~year as of mid-2026). Override with --quarter.
QUARTERS=(2025q2 2025q3 2025q4 2026q1)

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --out) OUT="$2"; shift 2;;
        --quarter) QUARTERS=("$2"); shift 2;;
        --help)
            echo "Usage: download-edgar-tsv.sh [--out DIR] [--quarter YYYYqN]"
            echo "  Requires curl, unzip. Downloads quarterly sub.txt + num.txt, gzipped."
            echo "  Set EDGAR_UA to override the User-Agent SEC requires."
            exit 0;;
        *) echo "Unknown parameter: $1"; exit 1;;
    esac
done

mkdir -p "$OUT"

# Download one quarter's ZIP, extract sub.txt + num.txt, re-gzip to OUT/<q>/.
# Skips if both already present; a failed fetch warns and continues.
fetch() {
    local q="$1" dir="$OUT/$q"
    if [[ -f "$dir/sub.txt.gz" && -f "$dir/num.txt.gz" ]]; then
        echo "skip (present): $q"; return 0
    fi
    mkdir -p "$dir"
    local tmp; tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN
    echo "downloading: $q"
    if ! curl -fL -# -A "$UA" "$BASE/$q.zip" -o "$tmp/q.zip" \
         || ! unzip -oq "$tmp/q.zip" sub.txt num.txt -d "$tmp"; then
        echo "warn: fetch/extract failed, skipping: $q" >&2
        return 0
    fi
    # temp + mv so an interrupted run never leaves a corrupt dest a later run skips.
    gzip -c "$tmp/sub.txt" > "$dir/sub.txt.gz.tmp" && mv "$dir/sub.txt.gz.tmp" "$dir/sub.txt.gz"
    gzip -c "$tmp/num.txt" > "$dir/num.txt.gz.tmp" && mv "$dir/num.txt.gz.tmp" "$dir/num.txt.gz"
    echo "wrote: $dir/{sub,num}.txt.gz"
}

for q in "${QUARTERS[@]}"; do
    fetch "$q"
    sleep 0.5   # be polite to SEC between large downloads
done

echo "done -> $OUT"

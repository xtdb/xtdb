#!/usr/bin/env bash

# Downloads SEC EDGAR companyfacts JSON for the xtdb.datasets.edgar loader: one
# file per CIK in the demo allow-set, gzipped. Each file is an issuer's full
# XBRL fact history, already bitemporal (period = valid-time, filed =
# system-time), so there's nothing date-windowed to walk — unlike GLEIF, EDGAR's
# companyfacts is a single complete document per issuer.
#
# Requires `curl`. SEC requires a descriptive User-Agent (they 403 a blank one)
# and rate-limits to ~10 req/s, so we identify ourselves and pace between CIKs.
#
# Already-present files are skipped, so re-runs are cheap.

set -euo pipefail

cd "$(dirname "$0")/.."

OUT="src/dev/resources/data/edgar"   # gitignored (see src/dev/resources/.gitignore)
UA="${EDGAR_UA:-XTDB datasets demo (contact: dev@xtdb.com)}"

# The demo allow-set — keep in sync with xtdb.datasets.edgar.parse/demo-cik-allow-set.
CIKS=(
    0000320193   # Apple Inc.
    0000040545   # General Electric Co
    0001657853   # Hertz Global Holdings, Inc
    0001336917   # Under Armour, Inc.
    0000072971   # Wells Fargo & Company
)

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --out) OUT="$2"; shift 2;;
        --cik) CIKS=("$2"); shift 2;;
        --help)
            echo "Usage: download-edgar.sh [--out DIR] [--cik CIK]"
            echo "  Requires curl. Downloads companyfacts JSON per allow-set CIK, gzipped."
            echo "  Set EDGAR_UA to override the User-Agent SEC requires."
            exit 0;;
        *) echo "Unknown parameter: $1"; exit 1;;
    esac
done

mkdir -p "$OUT"

# Download one CIK's companyfacts JSON, gzip to dest. Skips if dest exists; a
# failed fetch warns and continues (one bad CIK shouldn't tank the run).
fetch() {
    local cik="$1" dest="$2"
    if [[ -f "$dest" ]]; then echo "skip (present): $dest"; return 0; fi
    local url="https://data.sec.gov/api/xbrl/companyfacts/CIK${cik}.json"
    echo "downloading: CIK${cik}"
    local tmp; tmp="$(mktemp)"
    if ! curl -fsSL -A "$UA" "$url" -o "$tmp"; then
        echo "warn: fetch failed, skipping: CIK${cik}" >&2
        rm -f "$tmp"
        return 0
    fi
    # temp + mv so an interrupted run never leaves a corrupt dest a later run skips.
    gzip -c "$tmp" > "$dest.tmp"
    mv "$dest.tmp" "$dest"
    rm -f "$tmp"
    echo "wrote: $dest"
}

for cik in "${CIKS[@]}"; do
    fetch "$cik" "$OUT/CIK${cik}.json.gz"
    sleep 0.2   # stay well under SEC's ~10 req/s
done

echo "done -> $OUT"

#!/usr/bin/env bash

usage() {
  echo "Usage: $0 <crash-log-dir>" >&2
  exit 1
}

process_file() {
  local task=$1 in=$2 out=$3
  [[ -f $in ]] || return
  echo "Processing $(basename "$in") -> $(basename "$out")"
  "$PROJECT_ROOT/gradlew" -q "$task" -Pfile="$in" > "$out"
}

copy_edn_files() {
  [[ -f "$1/crash.edn" ]] && {
    echo "Copying crash.edn"
    cp "$1/crash.edn" "$2/"
  }
}

process_arrow_files() {
  process_file readArrowFile "$1/query-rel.arrow" "$2/query-rel.edn"
  process_file readArrowFile "$1/tx-ops.arrow" "$2/tx-ops.edn"
  process_file readArrowFile "$1/live-table-tx.arrow" "$2/live-table-tx.edn"
}

process_trie_files() {
  process_file readHashTrieFile "$1/live-trie-tx.binpb" "$2/live-trie-tx.edn"
  process_file readHashTrieFile "$1/live-trie.binpb" "$2/live-trie.edn"
}

main() {
  [[ $# -eq 1 ]] || usage
  [[ -d $1 ]] || { echo "Error: crash log directory '$1' not found" >&2; exit 1; }

  PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

  local crash_log_dir
  crash_log_dir=$(cd "$1" && pwd)

  local out_dir="$crash_log_dir/edn"
  mkdir -p "$out_dir"

  echo "Reading crash log files from $crash_log_dir ..."

  copy_edn_files "$crash_log_dir" "$out_dir"
  process_arrow_files "$crash_log_dir" "$out_dir"
  process_trie_files "$crash_log_dir" "$out_dir"

  echo "Done. EDN files in $out_dir"
}

main "$@"

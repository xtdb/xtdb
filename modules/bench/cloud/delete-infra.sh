#!/usr/bin/env bash

CLOUD=$1
if [ -z "$CLOUD" ]; then
  echo "Usage: $0 <aws|azure|google-cloud>"
  exit 1
fi 

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

set -e
(
  "${SCRIPT_DIR}/clear-bench.sh" "$CLOUD" || true

  cd "$SCRIPT_DIR/$CLOUD"
  echo "Destroying infrastructure for $CLOUD"
  terraform destroy || true
  echo "Infrastructure destroy complete for $CLOUD"
)

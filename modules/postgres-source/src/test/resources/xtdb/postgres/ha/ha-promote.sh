#!/usr/bin/env bash
# Promote this standby and don't return until it's actually a primary.
set -euo pipefail

if [ "$(id -u)" = '0' ]; then
    exec gosu postgres "$0" "$@"
fi

pg_ctl -D "$PGDATA" promote

# pg_ctl returns as soon as the request is *accepted*, which is too early for a caller to start
# asserting against the new primary. pg_controldata rather than pg_is_in_recovery() so the wait
# doesn't depend on how this container's pg_hba happens to authenticate a local connection.
for _ in $(seq 1 60); do
    if pg_controldata "$PGDATA" | grep -q 'Database cluster state: *in production'; then
        echo "ha-promote: promoted"
        exit 0
    fi
    sleep 1
done

echo "ha-promote: still in recovery after 60s" >&2
pg_controldata "$PGDATA" | grep 'Database cluster state' >&2
exit 1

#!/usr/bin/env bash
# Runs once on the primary, after initdb, via the official entrypoint's initdb.d hook.
set -euo pipefail

# The shipped `replication` entries only cover 127.0.0.1 and ::1, and a `host all all all` line
# does *not* match replication connections — the database column has to say `replication`
# explicitly. Without this, pg_basebackup from the standby container is rejected outright.
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"

# Lets ha-entrypoint.sh supply settings from the environment. pg_basebackup copies both conf
# files verbatim, so the standby — and the primary it becomes once promoted — inherit this
# without repeating the setup.
echo "include_if_exists = '/etc/xtdb/ha.conf'" >> "$PGDATA/postgresql.conf"

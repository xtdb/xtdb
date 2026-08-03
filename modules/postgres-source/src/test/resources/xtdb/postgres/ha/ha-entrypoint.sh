#!/usr/bin/env bash
set -euo pipefail

HA_ROLE="${HA_ROLE:-primary}"
HA_CONF=/etc/xtdb/ha.conf

log() { echo "ha[$HA_ROLE]: $*"; }

write_ha_conf() {
    # Pulled in by the include line primary-initdb.sh adds to postgresql.conf. A caller can
    # override any of it by appending `-c name=value` to the container command — command-line
    # args beat postgresql.conf.
    cat > "$HA_CONF" <<'EOF'
wal_level = logical
max_replication_slots = 20
max_wal_senders = 20
hot_standby = on
hot_standby_feedback = on
EOF

    # PG17+ slot synchronisation, off unless a test asks for it. On its own this preserves
    # nothing: only slots created with `failover = true` are ever candidates for syncing, and
    # XTDB can't create those yet (#5828).
    if [ "$HA_ROLE" = 'standby' ] && [ "${HA_SYNC_SLOTS:-off}" = 'on' ]; then
        echo "sync_replication_slots = on" >> "$HA_CONF"
    fi
}

# Build this standby's data directory from the primary.
standby_setup() {
    local host="${HA_PRIMARY_HOST:?HA_ROLE=standby requires HA_PRIMARY_HOST}"
    local port="${HA_PRIMARY_PORT:-5432}"
    local user="${HA_REPLICATION_USER:-${POSTGRES_USER:-postgres}}"

    # a populated PGDATA means we've already done this, so a restart is a no-op
    if [ -f "$PGDATA/PG_VERSION" ]; then
        log "$PGDATA already initialised, skipping pg_basebackup"
        return
    fi

    log "waiting for primary at $host:$port"
    for _ in $(seq 1 120); do
        pg_isready -h "$host" -p "$port" -U "$user" -q && break
        sleep 1
    done

    # A physical slot makes the primary hold WAL on our behalf rather than recycling it out
    # from under us, and PG17 slot synchronisation requires the standby to be using one.
    # -R writes primary_conninfo and standby.signal; given -S it writes primary_slot_name too.
    local slot_args=()
    if [ -n "${HA_PRIMARY_SLOT:-}" ]; then
        slot_args=(--create-slot --slot "$HA_PRIMARY_SLOT")
    fi

    # The primary accepts connections a little before it accepts replication connections, so
    # pg_isready returning isn't sufficient on its own.
    local attempt
    for attempt in $(seq 1 30); do
        log "pg_basebackup from $host:$port (attempt $attempt)"
        if pg_basebackup -h "$host" -p "$port" -U "$user" -D "$PGDATA" \
                         -R -X stream -c fast "${slot_args[@]}"; then
            break
        fi
        if [ "$attempt" -eq 30 ]; then
            log "pg_basebackup did not succeed" >&2
            exit 1
        fi
        sleep 2
    done

    # Postgres refuses to start on a data directory that's group- or world-accessible, and the
    # directory pg_basebackup wrote into doesn't come out 0700.
    chmod 0700 "$PGDATA"

    # The slot sync worker opens an *ordinary* connection to the primary, so unlike pg_basebackup
    # above it isn't covered by the `host replication ... trust` line and needs both a database
    # and a password. `pg_basebackup -R` writes neither. Ours goes in last, and the last setting
    # in postgresql.auto.conf wins.
    if [ "${HA_SYNC_SLOTS:-off}" = 'on' ]; then
        local db="${HA_PRIMARY_DB:-${POSTGRES_DB:-postgres}}"
        local pw="${HA_PRIMARY_PASSWORD:-${POSTGRES_PASSWORD:-}}"
        log "rewriting primary_conninfo for slot synchronisation (dbname=$db)"
        echo "primary_conninfo = 'host=$host port=$port user=$user dbname=$db${pw:+ password=$pw}'" \
            >> "$PGDATA/postgresql.auto.conf"
    fi

    log "standby ready to start in recovery"
}

case "$HA_ROLE" in
    primary)
        write_ha_conf
        # The official entrypoint handles initdb, POSTGRES_* and its own drop to the postgres
        # user; 00-ha-primary.sh runs partway through it.
        exec docker-entrypoint.sh "$@"
        ;;

    standby)
        # No initdb here — pg_basebackup produces the data directory — so the official
        # entrypoint has nothing to do, and we drop privileges ourselves.
        if [ "$(id -u)" = '0' ]; then
            write_ha_conf
            mkdir -p "$PGDATA"
            chown postgres:postgres "$PGDATA"
            exec gosu postgres "$0" "$@"
        fi

        standby_setup
        exec "$@"
        ;;

    *)
        echo "ha: HA_ROLE must be 'primary' or 'standby', got '$HA_ROLE'" >&2
        exit 1
        ;;
esac

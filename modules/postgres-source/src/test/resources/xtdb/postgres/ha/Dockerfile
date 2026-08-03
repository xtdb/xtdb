# Either half of a streaming-replication pair, selected by HA_ROLE — see README.adoc.
#
# TEST USE ONLY. The primary permits unauthenticated replication connections from any host; see
# primary-initdb.sh for why that's needed and why no real deployment should do it.

# Point at another major to check version-specific behaviour — PG16 for a server predating
# failover slots, PG18 for idle_replication_slot_timeout.
ARG PG_IMAGE=postgres:17-alpine
FROM ${PG_IMAGE}

COPY ha-entrypoint.sh ha-promote.sh /usr/local/bin/

# The official entrypoint runs initdb.d scripts on first start, so this one only ever fires on
# the primary — a standby's data directory arrives via pg_basebackup, never initdb.
COPY primary-initdb.sh /docker-entrypoint-initdb.d/00-ha-primary.sh

# /etc/xtdb holds the config ha-entrypoint.sh generates. Owned by postgres so it's writable
# whether the container runs as root (the norm) or with --user postgres.
RUN chmod +x /usr/local/bin/ha-entrypoint.sh \
             /usr/local/bin/ha-promote.sh \
             /docker-entrypoint-initdb.d/00-ha-primary.sh \
 && mkdir -p /etc/xtdb \
 && chown postgres:postgres /etc/xtdb

ENTRYPOINT ["ha-entrypoint.sh"]
CMD ["postgres"]

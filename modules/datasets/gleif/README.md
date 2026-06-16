# GLEIF federation demo

This demo loads the public GLEIF LEI register (legal-entity reference data) into plain Postgres — a current-state system-of-record — then has XTDB *follow Postgres over CDC*, deriving a bitemporal history the source never held.
GLEIF is the vehicle because it's redistribution-clean (CC0) and genuinely bitemporal: its events carry both an effective and a recorded date, so the temporal story is real data, not synthesised.

The federation *is* the XTDB-population story — there's no native XTDB loader.
XTDB attaches Postgres as an external source, and a custom `GleifPgIndexer` shapes each CDC row into a bitemporal document.

## What runs where

XTDB itself runs from a **REPL on the host**, not as a container — it's the only way to put `GleifPgIndexer` on the node's classpath.
`docker-compose.yml` brings up only the supporting cast:

| Service | Host port | Role |
| --- | --- | --- |
| Kafka | `127.0.0.1:9092` | XTDB log |
| Postgres | `127.0.0.1:5432` | GLEIF sec-master (db `gleif`, schema from `init/postgres/01-gleif.sql`) |
| Metabase | `127.0.0.1:3001` | queries XTDB pgwire at `172.30.0.1:5432` |
| Prometheus | `127.0.0.1:9090` | scrapes XTDB `/metrics` at `172.30.0.1:8081` |
| Grafana | `127.0.0.1:3000` | dashboards (`admin`/`admin`) |

Every published port binds `127.0.0.1`, so nothing is reachable from an untrusted LAN — only from the host.
The host XTDB node needs the same care: bind it to `172.30.0.1` — the gateway of this stack's pinned network (`docker-compose.yml`), i.e. the host as the containers see it, which they can reach but the LAN can't — **not** `*`.

## Running it

The REPL snippets live in a `(comment …)` block at the bottom of the relevant namespace.
This README is the map; those comments are the copy-paste.

1. **Get the dataset** — it's not checked in.
   Sync it from S3 (needs AWS creds) with `modules/datasets/scripts/download-dataset.sh --gleif` — it lands under `src/test/resources/data/gleif/`.
   Or produce it locally with `xtdb.datasets.gleif.mirror/mirror!`, which fetches the GLEIF API.
   The anchor is ~1GB, so producing it is slow.

2. **Bring up the supporting cast.**
   ```bash
   cd modules/datasets/gleif && docker-compose up -d
   ```
   Postgres creates the GLEIF tables, enum types and CDC publication on first boot from `init/postgres/`.

3. **Start XTDB and federate**, then **load GLEIF into Postgres and watch it stream in**, then **query the bitemporal history** — all from the root REPL (`./gradlew :clojureRepl`).
   The end-to-end run-through is the `(comment …)` at the bottom of `xtdb.datasets.gleif` (`src/main/clojure/xtdb/datasets/gleif.clj`).
   It starts a node with the `pg` remote and `:kafka` log-cluster, `ATTACH`es `gleif` while Postgres is still empty, then loads Postgres — which streams into XTDB over CDC — and runs the demo queries.
   The Postgres-load step has its own rich comment, with `psql` verification queries and how to watch per-batch progress, at the bottom of `xtdb.datasets.gleif.pg`.

After the load, `SELECT count(*) FROM lei;` should return **227,922** — that alone confirms the dataset and loader.
Once the `gleif` database is attached, the `q-entity-as-of-system-time` / `q-entity-as-of-valid-time` / `q-legal-name-history` queries (defined in `xtdb.datasets.gleif`) show truth-as-of-then versus corrected-truth-now — the point the federation makes.

## Exploring the data

Metabase and Grafana both query the live XTDB node over pgwire — the same endpoint the run-through binds to the compose network gateway — so the node has to be up with `gleif` attached and loaded (the steps above).
They connect with the same details:

- Host `172.30.0.1` (this stack's network gateway — the host node, reachable from the containers), port `5432`.
- Database `gleif`, user `xtdb`, no password, SSL disabled.

### Metabase — http://localhost:3001

1. Open Metabase and complete the first-run admin account setup.
2. Add a database of type **PostgreSQL** with the connection details above.
3. Browse the `lei` and `lei_*_changes` tables, or use the SQL editor for the bitemporal queries (`FOR VALID_TIME` / `FOR SYSTEM_TIME`).

You should see the ~228k `lei` rows once the CDC stream catches up.

### Grafana — http://localhost:3000 (`admin`/`admin`)

Grafana comes up pre-wired for both metrics and data — no manual setup.

**Metrics.**
The XTDB dashboards (`XTDB: Monitoring Dashboard`, `XTDB: Node Debugging Dashboard`) are provisioned from the repo's canonical set (`monitoring/dev-dashboards`, which reference the Prometheus datasource by uid so file-provisioning resolves them) and backed by Prometheus, which scrapes the node's `/metrics` at `172.30.0.1:8081`.
Log in and open **XTDB: Monitoring Dashboard**.
You should see ingestion, query and storage metrics within a few seconds of the node coming up — as long as the node binds `healthz` to `172.30.0.1:8081` (the run-through does).

**Data.**
XTDB is also provisioned as a `postgres` data source (`grafana/datasource.yml`), for querying the GLEIF tables directly.
Add a visualization on a new dashboard, pick the **XTDB** data source, and write SQL — e.g. an entity's legal-name history along valid-time:
```sql
SELECT legal_name, _valid_from, _valid_to
FROM lei_legal_name_changes FOR ALL VALID_TIME
WHERE _id = '<lei>'
ORDER BY _valid_from
```

To point Grafana at a different node, add a PostgreSQL data source by hand with the details above.

## Status

The Postgres load is verified end to end (227,922 `lei` rows, events fanned out by type, enums/jsonb/timestamptz correct, idempotent re-runs).
The federation half — node config, `ATTACH`, CDC snapshot, bitemporal queries — has run against a live node.
But the run-through in the `gleif` rich comment is reconstructed from the tests, since the original driver namespace wasn't committed, so sanity-check the node and `ATTACH` config before relying on it verbatim.
Entity relationships (the GLEIF `rr` file) are deferred — the temporal hierarchy deserves its own modelling pass.

## Troubleshooting

**Prometheus target down / Grafana "no data" / Metabase can't connect.**
The containers reach the node at `172.30.0.1` (the pinned gateway, i.e. the host), but a host firewall can drop container→host traffic — Docker manages only the `FORWARD` chain, not `INPUT`.
On a `ufw` host (default `INPUT DROP`), allow the pinned subnet to the node's ports:

```bash
sudo ufw allow from 172.30.0.0/16 to any port 8081 proto tcp   # metrics  → Prometheus
sudo ufw allow from 172.30.0.0/16 to any port 5432 proto tcp   # pgwire   → Metabase / Grafana
```

No restart needed — Prometheus retries within its scrape interval, and the datasource reconnects.

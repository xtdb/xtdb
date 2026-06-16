# EDGAR fundamentals federation demo

This demo loads a curated slice of SEC EDGAR company fundamentals into plain Postgres — a current-state system-of-record — then has XTDB *follow Postgres over CDC*, deriving a bitemporal history the source never held.
EDGAR is the vehicle because its bitemporality is real, not synthesised.
Companies restate: they re-file corrected figures for a period they already reported, so the system-time axis carries a genuine "what we believed, and when".
And instant balances, which a later as-of supersedes, give the valid-time axis a real timeline.

The federation *is* the XTDB-population story.
There's a native XTDB loader too — `xtdb.datasets.edgar/submit-edgar!` replays filings in filing order to reconstruct the same vintage history — but the demo's point is that XTDB accrues that history by *following* a non-bitemporal Postgres.
XTDB attaches Postgres as an external source, and a custom `EdgarPgIndexer` shapes each CDC row into a bitemporal document.
Its `_id`s match the native loader exactly, so a bulk load and a CDC stream are indistinguishable.

## What runs where

XTDB itself runs from a **REPL on the host**, not as a container — it's the only way to put `EdgarPgIndexer` on the node's classpath.
`docker-compose.yml` brings up only the supporting cast:

| Service | Host port | Role |
| --- | --- | --- |
| Kafka | `127.0.0.1:9092` | XTDB log |
| Postgres | `127.0.0.1:5432` | EDGAR sec-master (db `edgar`, schema from `init/postgres/01-edgar.sql`) |
| Metabase | `127.0.0.1:3001` | queries XTDB pgwire at `172.30.0.1:5432` |
| Prometheus | `127.0.0.1:9090` | scrapes XTDB `/metrics` at `172.30.0.1:8081` |
| Grafana | `127.0.0.1:3000` | dashboards (`admin`/`admin`) |

These are the same host ports as the GLEIF demo, so run one demo at a time.
Every published port binds `127.0.0.1`, so nothing is reachable from an untrusted LAN — only from the host.
The host XTDB node needs the same care: bind it to `172.30.0.1` — the gateway of this stack's pinned network (`docker-compose.yml`), i.e. the host as the containers see it, which they can reach but the LAN can't — **not** `*`.

## Running it

The REPL snippets live in a `(comment …)` block at the bottom of the relevant namespace — this README is the map, those comments are the copy-paste.

1. **Get the dataset** — it's not checked in.
   Sync from S3 (needs AWS creds): `modules/datasets/scripts/download-dataset.sh --edgar` — lands under `src/test/resources/data/edgar/`.
   Or produce it locally via `xtdb.datasets.edgar.mirror/mirror!`, which fetches the SEC quarterly financial-statement TSVs and curates to the registry CIKs.

2. **Bring up the supporting cast.**
   ```bash
   cd modules/datasets/edgar && docker-compose up -d
   ```
   Postgres creates the EDGAR tables and CDC publication on first boot from `init/postgres/`.

3. **Load EDGAR into Postgres**, then **start XTDB and federate**, then **query the bitemporal history** — all from the root REPL (`./gradlew :clojureRepl`).
   The end-to-end run-through is the `(comment …)` at the bottom of `xtdb.datasets.edgar` (`src/main/clojure/xtdb/datasets/edgar.clj`): docker-compose reference, Postgres load, node start with the `pg` remote + `:kafka` log-cluster, `ATTACH DATABASE edgar`, and the demo queries.
   The Postgres-load step has its own rich comment — with `psql` verification queries and how to watch per-quarter progress — at the bottom of `xtdb.datasets.edgar.pg`.

After the load, `SELECT count(*) FROM income_statement;` confirms the dataset and loader landed.
Once the `edgar` database is attached, `q-income-restatement-history` (defined in `xtdb.datasets.edgar`) walks every vintage of a statement for one period — where a value changes between consecutive `filed` dates, a restatement occurred.
`q-income-as-of-system-time` and `q-balance-as-of-valid-time` show truth-as-of-then versus corrected-truth-now — the point the federation makes.

## Replica identity — the EDGAR-specific gotcha

`EdgarPgIndexer` derives `_id` on **every** op, including `DELETE`.
A delete's row carries only the table's **REPLICA IDENTITY** columns, and each EDGAR table's `deriveId` reads exactly its primary key:

- `issuer` → `cik` (the PK)
- `income_statement` → `cik`, `period_start`, `period_end` (the compound PK)
- `balance_sheet` → `cik`, `period_end` (the compound PK)

Postgres' **default** replica identity is the primary key, and every EDGAR table has one — so the default is already sufficient for deletes, even with the compound PKs.
You do **not** need `REPLICA IDENTITY FULL`.

There's one case where you would.
If the source ever issues an `UPDATE` that leaves a TOAST-able column (a large `numeric`/`text`) unchanged, pgoutput sends it as "unchanged" and the source throws (`Received unchanged TOASTed column …`).
The EDGAR sink only ever UPSERTs whole rows, so this shouldn't arise; if you hand-edit rows and hit it, set `ALTER TABLE <t> REPLICA IDENTITY FULL`.

## Notes on identity correctness

`EdgarPgIndexer` derives `_id` from the **PG column values** (`cik`, `period_start`, `period_end`), which the sink stores in the same form the native loader uses:

- `cik` is the 10-digit zero-padded string (`parse/cik->padded`), stored verbatim in the `cik text` column.
- Dates render via `LocalDate.toString()` (canonical ISO-8601) on both sides, so `2023-09-30` is identical whether produced by the native loader or derived from the PG `date` column.

A native bulk load and a CDC stream therefore produce **identical `_id`s** for the same fact — the federation demo's whole point.
The CDC docs carry the **PG column names** (snake_case: `period_start`, `net_income_loss`) rather than the native loader's kebab keywords (`:period-start`, `:net-income-loss`).
XTDB normalises separators in SQL, so the demo queries (snake_case) resolve against either form.

## Exploring the data

Metabase and Grafana both query the live XTDB node over pgwire — the same endpoint the run-through binds to the compose network gateway — so the node has to be up with `edgar` attached and loaded (the steps above).
They connect with the same details:

- Host `172.30.0.1` (this stack's network gateway — the host node, reachable from the containers), port `5432`.
- Database `edgar`, user `xtdb`, no password, SSL disabled.

### Metabase — http://localhost:3001

1. Open Metabase and complete the first-run admin account setup.
2. Add a database of type **PostgreSQL** with the connection details above.
3. Browse the `issuer`, `income_statement` and `balance_sheet` tables, or use the SQL editor for the bitemporal queries (`FOR VALID_TIME` / `FOR SYSTEM_TIME`).

You should see the `income_statement` and `balance_sheet` rows once the CDC stream catches up.

### Grafana — http://localhost:3000 (`admin`/`admin`)

Grafana comes up pre-wired for both metrics and data — no manual setup.

**Metrics.**
The XTDB dashboards (`XTDB: Monitoring Dashboard`, `XTDB: Node Debugging Dashboard`) are provisioned from the repo's canonical set (`monitoring/dev-dashboards`, which reference the Prometheus datasource by uid so file-provisioning resolves them) and backed by Prometheus, which scrapes the node's `/metrics` at `172.30.0.1:8081`.
Log in and open **XTDB: Monitoring Dashboard**.
You should see ingestion, query and storage metrics within a few seconds of the node coming up — as long as the node binds `healthz` to `172.30.0.1:8081` (the run-through does).

**Data.**
XTDB is also provisioned as a `postgres` data source (`grafana/datasource.yml`), for querying the EDGAR tables directly.
Add a visualization on a new dashboard, pick the **XTDB** data source, and write SQL — e.g. the restatement trail of an income-statement figure along system-time:
```sql
SELECT net_income_loss, form, filed, accession
FROM income_statement FOR ALL SYSTEM_TIME
WHERE cik = '<cik>' AND period_end = '<period-end>'
ORDER BY filed
```

To point Grafana at a different node, add a PostgreSQL data source by hand with the details above.

## Status

The Postgres load is verified end to end: issuer / income_statement / balance_sheet UPSERTed by their period keys, latest-`filed`-wins, idempotent re-runs.
The parse/pivot is covered by unit tests (`edgar_test`, `edgar_tsv_test`, `edgar_snapshot_test`, `edgar_restatement_test`).
The federation half — node config, `ATTACH`, CDC snapshot, bitemporal queries — mirrors the GLEIF demo, so sanity-check the node/`ATTACH` config in the `edgar` rich comment against a live node before relying on it verbatim.

## Troubleshooting

**Prometheus target down / Grafana "no data" / Metabase can't connect.**
The containers reach the node at `172.30.0.1` (the pinned gateway, i.e. the host), but a host firewall can drop container→host traffic — Docker manages only the `FORWARD` chain, not `INPUT`.
On a `ufw` host (default `INPUT DROP`), allow the pinned subnet to the node's ports:

```bash
sudo ufw allow from 172.30.0.0/16 to any port 8081 proto tcp   # metrics  → Prometheus
sudo ufw allow from 172.30.0.0/16 to any port 5432 proto tcp   # pgwire   → Metabase / Grafana
```

No restart needed — Prometheus retries within its scrape interval, and the datasource reconnects.
